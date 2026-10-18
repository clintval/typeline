//! Reading dates, times, and datetimes from ISO 8601 text, to the microsecond, as Python holds them.

use jiff::civil::{Date, DateTime, DateTimeRound, Time, TimeRound};
use jiff::fmt::temporal::{Pieces, PiecesOffset};
use jiff::{RoundMode, Unit};

/// The time of day of a date or datetime, to the microsecond.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Clock {
    /// The hour, from 0 to 23.
    pub hour: u8,
    /// The minute, from 0 to 59.
    pub minute: u8,
    /// The second, from 0 to 59.
    pub second: u8,
    /// The microsecond, from 0 to 999,999.
    pub microsecond: u32,
}

/// The time zone of a time or datetime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Zone {
    /// No time zone: a naive time or datetime.
    Naive,
    /// Coordinated universal time, written `Z`, `+00:00`, or `-00:00`.
    Utc,
    /// A fixed offset from UTC, in seconds east of it.
    Fixed(i32),
}

/// A calendar date.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Day {
    /// The year, which Python allows from 1 to 9999.
    pub year: i32,
    /// The month, from 1 to 12.
    pub month: u8,
    /// The day of the month, from 1 to 31.
    pub day: u8,
}

/// Reads a date, e.g. `2021-03-04`.
pub fn date(text: &str) -> Result<Day, String> {
    let pieces = Pieces::parse(text).map_err(|error| error.to_string())?;
    if pieces.time().is_some() || pieces.offset().is_some() {
        return Err("A date holds no time!".to_string());
    }
    refuse_annotation(&pieces)?;
    Ok(day(pieces.date()))
}

/// Reads a datetime, e.g. `2021-03-04T05:06:07.5+05:30`, rounding it to the microsecond.
pub fn datetime(text: &str) -> Result<(Day, Clock, Zone), String> {
    let pieces = Pieces::parse(text).map_err(|error| error.to_string())?;
    let Some(time) = pieces.time() else {
        return Err("A datetime needs a time!".to_string());
    };
    refuse_annotation(&pieces)?;
    let rounded = microseconds(pieces.date().to_datetime(time))?;
    Ok((
        day(rounded.date()),
        clock(rounded.time()),
        zone(pieces.offset()),
    ))
}

/// Reads a time, e.g. `05:06:07.5Z`, rounding it to the microsecond, past midnight if need be.
pub fn time(text: &str) -> Result<(Clock, Zone), String> {
    let datetime = format!("2000-01-01T{text}");
    let pieces = Pieces::parse(&datetime).map_err(|_| {
        format!("Expected a time like 05:06:07, with an offset or not, but found '{text}'")
    })?;
    let Some(time) = pieces.time() else {
        return Err("Expected a time!".to_string());
    };
    refuse_annotation(&pieces)?;
    let rounded = time
        .round(
            TimeRound::new()
                .smallest(Unit::Microsecond)
                .mode(RoundMode::HalfExpand),
        )
        .map_err(|error| error.to_string())?;
    Ok((clock(rounded), zone(pieces.offset())))
}

/// Refuses a time zone annotation, e.g. `[America/New_York]`, which a fixed offset cannot hold.
fn refuse_annotation(pieces: &Pieces<'_>) -> Result<(), String> {
    match pieces.time_zone_annotation() {
        Some(_) => Err("A time zone annotation cannot be read, only an offset!".to_string()),
        None => Ok(()),
    }
}

/// Rounds a datetime to the microsecond, half away from zero.
fn microseconds(datetime: DateTime) -> Result<DateTime, String> {
    datetime
        .round(
            DateTimeRound::new()
                .smallest(Unit::Microsecond)
                .mode(RoundMode::HalfExpand),
        )
        .map_err(|error| error.to_string())
}

fn day(date: Date) -> Day {
    Day {
        year: i32::from(date.year()),
        month: date.month() as u8,
        day: date.day() as u8,
    }
}

fn clock(time: Time) -> Clock {
    Clock {
        hour: time.hour() as u8,
        minute: time.minute() as u8,
        second: time.second() as u8,
        microsecond: (time.subsec_nanosecond() / 1000) as u32,
    }
}

fn zone(offset: Option<PiecesOffset>) -> Zone {
    match offset.map(|offset| offset.to_numeric_offset().seconds()) {
        None => Zone::Naive,
        Some(0) => Zone::Utc,
        Some(seconds) => Zone::Fixed(seconds),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(hour: u8, minute: u8, second: u8, microsecond: u32) -> Clock {
        Clock {
            hour,
            minute,
            second,
            microsecond,
        }
    }

    #[test]
    fn date_reads_iso_8601_dates() {
        assert_eq!(
            date("2021-03-04"),
            Ok(Day {
                year: 2021,
                month: 3,
                day: 4
            })
        );
        assert_eq!(
            date("20210304"),
            Ok(Day {
                year: 2021,
                month: 3,
                day: 4
            })
        );
        for text in ["2021-02-30", "2021-3-4", "2021-03-04T05:06:07", "x", ""] {
            assert!(date(text).is_err(), "{text:?}");
        }
    }

    #[test]
    fn datetime_reads_rfc_3339_datetimes() {
        let day = Day {
            year: 2021,
            month: 3,
            day: 4,
        };
        assert_eq!(
            datetime("2021-03-04T05:06:07"),
            Ok((day, at(5, 6, 7, 0), Zone::Naive))
        );
        assert_eq!(
            datetime("2021-03-04 05:06:07Z"),
            Ok((day, at(5, 6, 7, 0), Zone::Utc))
        );
        assert_eq!(
            datetime("2021-03-04t05:06:07z"),
            Ok((day, at(5, 6, 7, 0), Zone::Utc))
        );
        assert_eq!(
            datetime("2021-03-04T05:06:07-00:00"),
            Ok((day, at(5, 6, 7, 0), Zone::Utc))
        );
        assert_eq!(
            datetime("2021-03-04T05:06:07.5+05:30"),
            Ok((day, at(5, 6, 7, 500_000), Zone::Fixed(19_800)))
        );
        assert_eq!(
            datetime("2021-03-04T05:06:07-0530"),
            Ok((day, at(5, 6, 7, 0), Zone::Fixed(-19_800)))
        );
        for text in [
            "2021-03-04",
            "2021-03-04T05:06:07[America/New_York]",
            "2021-03-04T25:00",
        ] {
            assert!(datetime(text).is_err(), "{text:?}");
        }
    }

    #[test]
    fn datetime_rounds_to_the_microsecond_carrying_into_the_next_day() {
        let next = Day {
            year: 2021,
            month: 3,
            day: 5,
        };
        assert_eq!(
            datetime("2021-03-04T05:06:07.1234567").map(|(_, clock, _)| clock),
            Ok(at(5, 6, 7, 123_457))
        );
        assert_eq!(
            datetime("2021-03-04T23:59:59.9999995"),
            Ok((next, at(0, 0, 0, 0), Zone::Naive))
        );
    }

    #[test]
    fn time_reads_times_with_and_without_offsets() {
        assert_eq!(time("05:06:07"), Ok((at(5, 6, 7, 0), Zone::Naive)));
        assert_eq!(time("05:06:07.25Z"), Ok((at(5, 6, 7, 250_000), Zone::Utc)));
        assert_eq!(
            time("05:06:07+01:00"),
            Ok((at(5, 6, 7, 0), Zone::Fixed(3600)))
        );
        for text in ["", "5", "25:00:00", "2021-03-04T05:06:07", "T05:06:07"] {
            assert!(time(text).is_err(), "{text:?}");
        }
    }
}
