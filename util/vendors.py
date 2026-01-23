from __future__ import annotations

from typing import Iterable

VALID_VENDORS: set[str] = {
    "VendorA",
    "VendorB",
    "VendorC",
    "VendorD",
}

def normalize_vendor(v: str) -> str:
    return v.strip()

def validate_vendor(v: str, valid: Iterable[str] = VALID_VENDORS) -> str:
    v2 = normalize_vendor(v)
    valid_list = sorted(valid)

    if v2 in valid_list:
        return v2

    # small helpful hint without extra dependencies
    v2_low = v2.lower()
    near = [x for x in valid_list if x.lower().startswith(v2_low[:2])]
    hint = f" Did you mean one of: {near} ?" if near else ""

    raise ValueError(f"Unknown vendor '{v2}'. Allowed: {valid_list}.{hint}")
