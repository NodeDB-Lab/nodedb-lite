#!/usr/bin/env python3
"""Enforce time-bounded, individually reviewed RustSec advisory ignores."""

from __future__ import annotations

import argparse
import re
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = ROOT / "deny.toml"
SECTION_RE = re.compile(r"^\s*\[advisories\]\s*(?:#.*)?$")
ANY_SECTION_RE = re.compile(r"^\s*\[[^]]+\]\s*(?:#.*)?$")
IGNORE_ASSIGN_RE = re.compile(r"^\s*ignore\s*=")
IGNORE_START_RE = re.compile(r"^\s*ignore\s*=\s*\[\s*(?:#.*)?$")
IGNORE_END_RE = re.compile(r"^\s*\]\s*(?:#.*)?$")
ENTRY_RE = re.compile(
    r'^\s*"(?P<identifier>[^"]*)"\s*,?\s*(?:#(?P<comment>.*))?$'
)
RUSTSEC_RE = re.compile(r"RUSTSEC-\d{4}-\d{4}\Z")
REVIEW_RE = re.compile(r"\breview-by\s*:\s*(\S+)")
DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
WILDCARD_RE = re.compile(r"[\*?\[]")


def advisory_lines(text: str) -> tuple[list[tuple[int, str]], list[str]]:
    """Return the ignore-list source lines from the advisories TOML table."""
    lines = text.splitlines()
    errors: list[str] = []
    start = next((index for index, line in enumerate(lines) if SECTION_RE.match(line)), None)
    if start is None:
        return [], ["missing [advisories] table"]

    end = next(
        (index for index in range(start + 1, len(lines)) if ANY_SECTION_RE.match(lines[index])),
        len(lines),
    )
    ignore_assignments = [
        index for index in range(start + 1, end) if IGNORE_ASSIGN_RE.match(lines[index])
    ]
    if not ignore_assignments:
        return [], errors
    if len(ignore_assignments) != 1:
        return [], ["[advisories].ignore is declared more than once"]
    ignore_start = ignore_assignments[0]
    if IGNORE_START_RE.match(lines[ignore_start]) is None:
        return [], [
            "[advisories].ignore must place one advisory ID on each line "
            "inside a multiline array"
        ]

    entries: list[tuple[int, str]] = []
    for index in range(ignore_start + 1, end):
        line = lines[index]
        if IGNORE_END_RE.match(line):
            return entries, errors
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        entries.append((index + 1, line))
    errors.append("[advisories].ignore is missing its closing bracket")
    return entries, errors


def validate_text(text: str, today: date) -> list[str]:
    """Validate a deny.toml source string and return stable, line-specific errors."""
    entries, errors = advisory_lines(text)
    seen: set[str] = set()

    for line_number, line in entries:
        match = ENTRY_RE.match(line)
        if match is None:
            errors.append(f"line {line_number}: malformed advisory ignore entry")
            continue

        identifier = match.group("identifier")
        comment = match.group("comment") or ""
        if WILDCARD_RE.search(identifier):
            errors.append(f"line {line_number}: blanket/wildcard advisory ignore is forbidden")
            continue
        if RUSTSEC_RE.fullmatch(identifier) is None:
            errors.append(
                f"line {line_number}: malformed advisory ID `{identifier}`; "
                "expected RUSTSEC-YYYY-NNNN"
            )
            continue
        if identifier in seen:
            errors.append(f"line {line_number}: duplicate advisory ID `{identifier}`")
        seen.add(identifier)

        annotations = REVIEW_RE.findall(comment)
        if len(annotations) != 1:
            errors.append(
                f"line {line_number}: `{identifier}` must have exactly one "
                "review-by: YYYY-MM-DD annotation"
            )
            continue
        review_by = annotations[0]
        if DATE_RE.fullmatch(review_by) is None:
            errors.append(
                f"line {line_number}: `{identifier}` has invalid review-by date `{review_by}`"
            )
            continue
        try:
            review_date = date.fromisoformat(review_by)
        except ValueError:
            errors.append(
                f"line {line_number}: `{identifier}` has invalid review-by date `{review_by}`"
            )
            continue
        if review_date < today:
            errors.append(
                f"line {line_number}: `{identifier}` review-by date `{review_by}` has expired"
            )
    return errors


def fixture(*entries: str) -> str:
    return "[advisories]\nignore = [\n" + "\n".join(entries) + "\n]\n"


def self_test() -> None:
    today = date(2026, 1, 1)
    fixtures = (
        (
            "positive",
            fixture('    "RUSTSEC-2025-0134", # review-by: 2027-01-31'),
            None,
        ),
        (
            "missing annotation",
            fixture('    "RUSTSEC-2025-0134",'),
            "must have exactly one",
        ),
        (
            "invalid annotation",
            fixture('    "RUSTSEC-2025-0134", # review-by: 2027/01/31'),
            "invalid review-by date",
        ),
        (
            "expired date",
            fixture('    "RUSTSEC-2025-0134", # review-by: 2025-12-31'),
            "has expired",
        ),
        (
            "duplicate ID",
            fixture(
                '    "RUSTSEC-2025-0134", # review-by: 2027-01-31',
                '    "RUSTSEC-2025-0134", # review-by: 2027-01-31',
            ),
            "duplicate advisory ID",
        ),
        (
            "malformed ID",
            fixture('    "RUSTSEC-2025-134", # review-by: 2027-01-31'),
            "malformed advisory ID",
        ),
        (
            "wildcard ignore",
            fixture('    "RUSTSEC-*", # review-by: 2027-01-31'),
            "blanket/wildcard",
        ),
    )
    for name, source, expected_error in fixtures:
        errors = validate_text(source, today)
        if expected_error is None:
            assert not errors, f"{name} fixture unexpectedly failed: {errors}"
        else:
            assert any(expected_error in error for error in errors), (
                f"{name} fixture did not fail with {expected_error!r}: {errors}"
            )
    print("OK: advisory-ignore policy gate self-tests passed.")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0

    errors = validate_text(DEFAULT_CONFIG.read_text(encoding="utf-8"), date.today())
    if errors:
        print("ERROR: advisory ignores must be individually time-bounded:", file=sys.stderr)
        for error in errors:
            print(f"  {error}", file=sys.stderr)
        return 1
    print("OK: advisory ignores are individually time-bounded.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
