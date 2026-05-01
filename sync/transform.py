"""Oracle row -> Zoho Creator payload mapping."""
from __future__ import annotations

from typing import Any, Mapping


def yn_to_bool(val: Any) -> bool | None:
    if val is None:
        return None
    if isinstance(val, str):
        s = val.strip().upper()
        if s == "Y":
            return True
        if s == "N":
            return False
        return None
    return bool(val)


# Names of columns selected by Worker R / B for the Items_Data form.
ITEMS_COLUMNS = (
    "SK1MCP", "SK1MYR", "SK1M1", "SK1M2", "SK1M3", "SK1M9",
    "SK1M11", "SK1M12", "SK1M13", "SK1M14", "SK1M16", "SK1M17",
    "SK1M18", "SK1M19", "SK1M20", "SK1M21", "SK1M22", "SK1M24",
    "SK1M29", "SK1M31", "SK1M32", "SK1M33", "SK1M34", "SK1M36",
    "SK1M37", "SK1M39", "SK1M40", "SK1M41", "SK1M261",
    "PS33M2", "PS33M4",
)

BRANCHES_COLUMNS = ("GRBRCP", "GRBRYR", "BN", "GRBR2", "GRBR3")


def _strip(val: Any) -> Any:
    return val.strip() if isinstance(val, str) else val


def _trunc(val: Any, maxlen: int) -> Any:
    """Truncate string values to fit Zoho's max field length (byte-based).

    Zoho Creator counts bytes, not characters, for Single-Line fields.
    Arabic/multibyte text needs byte-level truncation.
    """
    if not isinstance(val, str):
        return val
    # Fast path: ASCII-only strings
    if len(val) <= maxlen:
        encoded = val.encode("utf-8")
        if len(encoded) <= maxlen:
            return val
    # Trim one char at a time until byte length fits
    while len(val.encode("utf-8")) > maxlen:
        val = val[:-1]
    return val


def items_payload(row: Mapping[str, Any]) -> dict:
    """Map an SK1MF (+ PS33MF) row dict to the Items_Data Zoho payload."""
    g = lambda k: _strip(row.get(k))  # noqa: E731
    return {
        "Company":             g("SK1MCP"),
        "Year":                g("SK1MYR"),
        "Item_Code":           _trunc(g("SK1M1"), 20),
        "Item_Arabic_Name":    _trunc(g("SK1M2"), 40),
        "Item_English_Name":   _trunc(g("SK1M3"), 40),
        "SK1M9":               g("SK1M9"),
        "Posted_N_Year":       yn_to_bool(g("SK1M11")),
        "Imported_Item":       yn_to_bool(g("SK1M12")),
        "Expiry_Date":         yn_to_bool(g("SK1M13")),
        "Item_Tax":            g("SK1M14"),
        "Unit_1_Code":         g("SK1M16"),
        "Unit_2_Code":         g("SK1M17"),
        "Unit_3_Code":         g("SK1M18"),
        "Unit_4_Code":         g("SK1M19"),
        "Unit_1_Content":      g("SK1M20"),
        "Unit_2_Content":      g("SK1M21"),
        "Unit_3_Content":      g("SK1M22"),
        "Add_Code":            _trunc(g("SK1M24"), 20),
        "Parent_Item":         _trunc(g("SK1M29"), 20),
        "Salable_Item":        yn_to_bool(g("SK1M31")),
        "Purchasable_Item":    yn_to_bool(g("SK1M32")),
        "Productionable_Item": yn_to_bool(g("SK1M33")),
        "Report_Unit_Code":    g("SK1M34"),
        "Weight_Unit_Code":    _trunc(g("SK1M36"), 3),
        "Weight_of_Unit":      g("SK1M37"),
        "Sales_Unit_Code":     g("SK1M39"),
        "Batch_No":            yn_to_bool(g("SK1M40")),
        "Lot_No":              yn_to_bool(g("SK1M41")),
        "Eq_Code":             g("SK1M261"),
        "It_Filter_Type_Code": g("PS33M2"),
        "It_Filter_Code":      g("PS33M4"),
    }


def branches_payload(row: Mapping[str, Any]) -> dict:
    g = lambda k: _strip(row.get(k))  # noqa: E731
    return {
        "GRBRCP":              g("GRBRCP"),
        "GRBRYR":              g("GRBRYR"),
        "Branch_Code":         g("BN"),
        "Branch_Arabic_Name":  _trunc(g("GRBR2"), 20),
        "Branch_English_Name": _trunc(g("GRBR3"), 20),
    }

