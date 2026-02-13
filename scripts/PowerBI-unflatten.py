'''
1. print all json properties (generate a schema) for the native snaffler json output stored at ./snaffledShares.json
2. Save the schema to ./snafflerSchema.tmp.json
3. read the schema and understand it
4. create a function to mutate the schema so it's nested properly (i.e. no more entries.eventProperties.Green, but rather, enteries.event.severity, etc)
5. use the function to mutate ./snaffledShares.json into ./snaffledShares.powerbi.json
6. read the mutated json file and confirm it's valid json and has similar data.

NOTE: You can remove 'entries.[].rawEventProperties' in PowerBI when you import, if it causes issues.
Add that removal as a transform step.
'''

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set


# NOTE USER PROVIDED ARGUMENTS: Change directories here.
BASE_DIR = Path(__file__).parent
SOURCE_PATH = BASE_DIR / "snaffledShares.json"
SCHEMA_PATH = BASE_DIR / "snafflerSchema.tmp.json"
OUTPUT_PATH = BASE_DIR / "snaffledShares.powerbi.json"
SHOULD_EXCLUDE_RAW_EVENT_PROPERTIES = True


def load_source() -> Dict[str, Any]:
    """Load the raw snaffler output."""
    with SOURCE_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _walk_schema(node: Any, path: str, found: Dict[str, Set[str]]) -> None:
    """Collect a lightweight path->types schema."""
    found.setdefault(path, set()).add(_type_name(node))

    if isinstance(node, dict):
        for key, value in node.items():
            next_path = f"{path}.{key}" if path else key
            _walk_schema(value, next_path, found)
    elif isinstance(node, list):
        next_path = f"{path}[]" if path else "[]"
        for item in node:
            _walk_schema(item, next_path, found)


def build_schema(document: Dict[str, Any]) -> Dict[str, List[str]]:
    """Return a simple schema mapping JSON paths to observed types."""
    paths: Dict[str, Set[str]] = {}
    _walk_schema(document, "", paths)
    return {k: sorted(v) for k, v in sorted(paths.items())}


def normalize_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten eventProperties.* color buckets into a common event object.

    Example:
    {"eventProperties": {"Green": {"DateTime": "...", ...}}}
    becomes
    {"event": {"severity": "Green", "DateTime": "...", ...}}
    """
    event_props = entry.get("eventProperties") or {}

    # Take the first severity bucket, but keep the raw payload if present.
    severity, payload = (next(iter(event_props.items())) if event_props else (None, None))

    # Copy base fields to avoid mutating the original dict.
    normalized: Dict[str, Any] = {
        key: value
        for key, value in entry.items()
        if key != "eventProperties"
    }

    if severity:
        # Carry original message-level data plus normalized event block.
        normalized["event"] = {"severity": severity, **payload}
        if not SHOULD_EXCLUDE_RAW_EVENT_PROPERTIES:
            normalized["rawEventProperties"] = event_props  # keep original for traceability

    return normalized


def transform(document: Dict[str, Any]) -> Dict[str, Any]:
    """Apply normalization to every entry."""
    entries = document.get("entries", [])
    transformed = [normalize_entry(entry) for entry in entries]
    return {"entries": transformed}


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        handle.write("\n")


def main() -> None:
    raw = load_source()

    # Step 1 & 2: derive a path/type schema and persist it.
    schema = build_schema(raw)
    write_json(SCHEMA_PATH, schema)

    # Step 4 & 5: normalize and save PowerBI-friendly version.
    transformed = transform(raw)
    write_json(OUTPUT_PATH, transformed)

    # Step 6: lightweight validation parity checks.
    raw_count = len(raw.get("entries", []))
    new_count = len(transformed.get("entries", []))
    assert raw_count == new_count, (
        f"entry count changed during transform: {raw_count} -> {new_count}"
    )


if __name__ == "__main__":
    main()
