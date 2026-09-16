"""Publication helpers shared by the legacy arXiv transform and the full
DataCite transform.

These are the established conventions every published Parquet file follows:
which reference keys may become struct fields, how reference values are
stringified, and how DuckDB identifiers and paths are quoted. Changing any of
them changes published data, so they live in one place.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

# A reference key becomes a published struct field only when it matches this
# pattern; anything else is reported as a dropped key.
REFERENCE_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 ._:-]*$")


def stringify_value(value: Any) -> str:
    """Render one reference value as the published string: strings verbatim,
    anything else as compact, key-sorted, non-ASCII-preserving JSON."""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def quote_path(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"
