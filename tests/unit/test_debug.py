from kafkac.debug import parse_debug_options


def test_no_matches_returns_empty_string() -> None:
    data = "no,matches,valid"
    assert parse_debug_options(data) == ""


def test_casing_does_not_matter() -> None:
    data = "CGRP,foo"
    output = parse_debug_options(data)
    assert "cgrp" in output
    assert "foo" not in output


def test_only_includes_valid_options_trimmed() -> None:
    data = "CgRp, ConSumEr, Topic,     fetch"
    output = parse_debug_options(data)
    assert "consumer" in output
    assert "cgrp" in output
    assert "topic" in output
    assert "fetch" in output


def test_returns_empty_string_for_none() -> None:
    assert parse_debug_options(None) == ""
