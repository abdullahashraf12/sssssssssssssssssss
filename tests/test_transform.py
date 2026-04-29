from sync.transform import branches_payload, items_payload, yn_to_bool


def test_yn_to_bool_basic():
    assert yn_to_bool("Y") is True
    assert yn_to_bool("y") is True
    assert yn_to_bool("N") is False
    assert yn_to_bool("n") is False
    assert yn_to_bool(None) is None
    assert yn_to_bool("") is None
    assert yn_to_bool("X") is None


def test_yn_to_bool_strips_whitespace():
    assert yn_to_bool(" Y ") is True
    assert yn_to_bool(" N ") is False


def test_items_payload_full_row():
    row = {
        "SK1MCP": 1, "SK1MYR": 2025, "SK1M1": "ITM-1",
        "SK1M2": "اسم", "SK1M3": "Name", "SK1M9": 1,
        "SK1M11": "Y", "SK1M12": "N", "SK1M13": "Y",
        "SK1M14": 14, "SK1M16": 16, "SK1M17": 17, "SK1M18": 18, "SK1M19": 19,
        "SK1M20": 20, "SK1M21": 21, "SK1M22": 22,
        "SK1M24": "AC", "SK1M29": "PAR",
        "SK1M31": "Y", "SK1M32": "N", "SK1M33": "Y",
        "SK1M34": 34, "SK1M36": "KG", "SK1M37": 37, "SK1M39": 39,
        "SK1M40": "N", "SK1M41": "Y", "SK1M261": 261,
        "PS33M2": 5, "PS33M4": 6,
    }
    p = items_payload(row)
    assert p["Item_Code"] == "ITM-1"
    assert p["Item_Arabic_Name"] == "اسم"
    assert p["Posted_N_Year"] is True
    assert p["Imported_Item"] is False
    assert p["Salable_Item"] is True
    assert p["Batch_No"] is False
    assert p["Lot_No"] is True
    assert p["It_Filter_Code"] == 6


def test_items_payload_handles_nulls_and_whitespace():
    row = {"SK1M1": "  ITM-2  ", "SK1M11": None}
    p = items_payload(row)
    assert p["Item_Code"] == "ITM-2"
    assert p["Posted_N_Year"] is None


def test_branches_payload():
    row = {"GRBRCP": 1, "GRBRYR": 2025, "BN": 7,
           "GRBR2": "فرع", "GRBR3": "Branch"}
    p = branches_payload(row)
    assert p == {
        "GRBRCP": 1, "GRBRYR": 2025, "Branch_Code": 7,
        "Branch_Arabic_Name": "فرع", "Branch_English_Name": "Branch",
    }
