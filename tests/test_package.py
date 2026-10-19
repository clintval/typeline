import typeline


def test_package_lists_only_its_public_names() -> None:
    """Test that the package lists its public names and public module, not its private modules."""
    assert dir(typeline) == sorted([*typeline.__all__, "codecs"])


def test_codecs_lists_only_its_public_names() -> None:
    """Test that the codecs module lists its public names, not the names it imports."""
    assert dir(typeline.codecs) == sorted(typeline.codecs.__all__)
