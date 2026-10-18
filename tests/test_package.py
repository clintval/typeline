import typeline


def test_package_lists_only_its_public_names() -> None:
    """Test that the package lists its public names and public module, not its private modules."""
    assert dir(typeline) == sorted([*typeline.__all__, "codecs"])
