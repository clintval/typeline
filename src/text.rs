//! Parsing and writing the text of a field as typeline and msgspec do.

/// Parses an integer written as JSON writes one, e.g. `-12`, but not `012` or `1e3`.
pub fn plain_int(text: &str) -> Option<i64> {
    let digits = text.strip_prefix('-').unwrap_or(text).as_bytes();
    let plain = !digits.is_empty()
        && digits.len() <= 18
        && (digits[0] != b'0' || digits.len() == 1)
        && digits.iter().all(u8::is_ascii_digit);
    if plain { text.parse().ok() } else { None }
}

/// Skips the ASCII digits of a text from a position, returning where they end.
fn skip_digits(bytes: &[u8], from: usize) -> usize {
    from + bytes[from..]
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count()
}

/// Parses a finite float written as JSON writes a number, e.g. `-1.5e3`, but not `.5` or `nan`.
pub fn plain_float(text: &str) -> Option<f64> {
    let bytes = text.as_bytes();
    let mut at = usize::from(bytes.first() == Some(&b'-'));
    match bytes.get(at) {
        Some(b'0') => at += 1,
        Some(b'1'..=b'9') => at = skip_digits(bytes, at),
        _ => return None,
    }
    if bytes.get(at) == Some(&b'.') {
        let end = skip_digits(bytes, at + 1);
        if end == at + 1 {
            return None;
        }
        at = end;
    }
    if matches!(bytes.get(at), Some(b'e' | b'E')) {
        at += 1;
        if matches!(bytes.get(at), Some(b'+' | b'-')) {
            at += 1;
        }
        let end = skip_digits(bytes, at);
        if end == at {
            return None;
        }
        at = end;
    }
    if at != bytes.len() {
        return None;
    }
    text.parse().ok().filter(|value: &f64| value.is_finite())
}

/// Returns whether Python's `str.isspace` holds for a character.
pub fn is_python_space(character: char) -> bool {
    matches!(
        character,
        '\u{9}'..='\u{d}'
            | '\u{1c}'..='\u{20}'
            | '\u{85}'
            | '\u{a0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200a}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202f}'
            | '\u{205f}'
            | '\u{3000}'
    )
}

/// Returns whether text looks like JSON: a literal like `null`, an array, or an object.
pub fn looks_like_json(text: &str) -> bool {
    let stripped = text.trim_matches(is_python_space);
    matches!(stripped, "null" | "true" | "false") || stripped.starts_with(['{', '['])
}

/// Writes text as a JSON string, e.g. `say "hi"` as `"say \"hi\""`.
pub fn json_string(text: &str) -> Vec<u8> {
    let mut json = Vec::with_capacity(text.len() + 2);
    json.push(b'"');
    for &byte in text.as_bytes() {
        match byte {
            b'"' => json.extend_from_slice(b"\\\""),
            b'\\' => json.extend_from_slice(b"\\\\"),
            0..0x20 => json.extend_from_slice(format!("\\u{byte:04x}").as_bytes()),
            _ => json.push(byte),
        }
    }
    json.push(b'"');
    json
}

/// Names the line a record starts on in an error, and its file when it has a name.
pub fn on_line(line_number: Option<u64>, file_name: Option<&str>) -> String {
    match (line_number, file_name) {
        (None, _) => String::new(),
        (Some(line), None) => format!(" on line {line}"),
        (Some(line), Some(file)) => format!(" on line {line} of '{file}'"),
    }
}

/// Writes a finite float as msgspec writes one in JSON, e.g. `0.00001`, `1000.0`, `1e-6` or
/// `1.5e16`: the shortest digits that read back as the float, as Ryu finds them, in decimal
/// notation from `1e-5` up to `1e16` and in scientific notation beyond.
pub fn json_float(value: f64) -> String {
    let mut buffer = ryu::Buffer::new();
    let shortest = buffer.format_finite(value);
    let (sign, shortest) = shortest
        .strip_prefix('-')
        .map_or(("", shortest), |rest| ("-", rest));
    let (mantissa, exponent) = shortest.split_once('e').unwrap_or((shortest, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let all = format!("{whole}{fraction}");
    let digits = all.trim_start_matches('0');
    let leading = (all.len() - digits.len()) as i32;
    let digits = match digits.trim_end_matches('0') {
        "" => "0",
        digits => digits,
    };
    let exponent = if digits == "0" {
        0
    } else {
        exponent + whole.len() as i32 - 1 - leading
    };
    if (-5..16).contains(&exponent) {
        let point = exponent + 1;
        if point <= 0 {
            format!(
                "{sign}0.{}{digits}",
                "0".repeat(point.unsigned_abs() as usize)
            )
        } else if point as usize >= digits.len() {
            format!(
                "{sign}{digits}{}.0",
                "0".repeat(point as usize - digits.len())
            )
        } else {
            let (before, after) = digits.split_at(point as usize);
            format!("{sign}{before}.{after}")
        }
    } else {
        match digits.split_at(1) {
            (first, "") => format!("{sign}{first}e{exponent}"),
            (first, rest) => format!("{sign}{first}.{rest}e{exponent}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn plain_int_reads_only_json_integers() {
        assert_eq!(plain_int("0"), Some(0));
        assert_eq!(plain_int("-0"), Some(0));
        assert_eq!(plain_int("-12"), Some(-12));
        assert_eq!(
            plain_int("999999999999999999"),
            Some(999_999_999_999_999_999)
        );
        for text in [
            "",
            "-",
            "+1",
            "01",
            "1e3",
            "1.0",
            " 1",
            "1 ",
            "1_000",
            "9999999999999999999",
        ] {
            assert_eq!(plain_int(text), None, "{text:?}");
        }
    }

    #[test]
    fn plain_float_reads_only_finite_json_numbers() {
        assert_eq!(plain_float("0"), Some(0.0));
        assert_eq!(plain_float("-1.5e3"), Some(-1500.0));
        assert_eq!(plain_float("1E-2"), Some(0.01));
        for text in [
            "", ".5", "1.", "01", "+1", "nan", "inf", "1e", "1e+", "1e400", "1_0", " 1",
        ] {
            assert_eq!(plain_float(text), None, "{text:?}");
        }
    }

    #[test]
    fn looks_like_json_finds_literals_arrays_and_objects() {
        for text in ["null", "true", "false", " true\n", "[1", "{", "\u{a0}[]"] {
            assert!(looks_like_json(text), "{text:?}");
        }
        for text in ["", "None", "1", "\"a\"", "nulls", "x[1]"] {
            assert!(!looks_like_json(text), "{text:?}");
        }
    }

    #[test]
    fn json_float_writes_floats_as_msgspec_does() {
        let cases = [
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (1.0, "1.0"),
            (0.1, "0.1"),
            (1e-5, "0.00001"),
            (1e-6, "1e-6"),
            (1.5e-7, "1.5e-7"),
            (1e15, "1000000000000000.0"),
            (1e16, "1e16"),
            (1.234_567_890_123_456_7e16, "1.2345678901234568e16"),
            (5e-324, "5e-324"),
            (123_456_789.125, "123456789.125"),
        ];
        for (value, text) in cases {
            assert_eq!(json_float(value), text, "{value:?}");
        }
    }

    #[test]
    fn on_line_names_the_line_and_file() {
        assert_eq!(on_line(None, Some("a.tsv")), "");
        assert_eq!(on_line(Some(2), None), " on line 2");
        assert_eq!(on_line(Some(2), Some("a.tsv")), " on line 2 of 'a.tsv'");
    }

    proptest! {
        #[test]
        fn plain_int_reads_what_it_writes(value in -999_999_999_999_999_999_i64..=999_999_999_999_999_999) {
            prop_assert_eq!(plain_int(&value.to_string()), Some(value));
        }

        #[test]
        fn json_float_reads_back_as_the_same_float(bits in any::<u64>()) {
            let value = f64::from_bits(bits);
            prop_assume!(value.is_finite());
            let text = json_float(value);
            prop_assert_eq!(text.parse::<f64>().unwrap().to_bits(), value.to_bits());
            prop_assert_eq!(plain_float(&text).map(f64::to_bits), Some(value.to_bits()));
        }

        #[test]
        fn json_string_reads_back_as_the_same_text(text in any::<String>()) {
            let decoded: String = serde_json::from_slice(&json_string(&text)).unwrap();
            prop_assert_eq!(decoded, text);
        }
    }
}
