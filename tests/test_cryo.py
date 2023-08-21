from cryogen import cryo

name = "ethereum__contracts__12325000_to_12325999.parquet"


def test_extract_range():
    assert cryo.extract_range(name) == range(12325000, 12326000)


def test_replace_range():
    new_name = "ethereum__contracts__00001000_to_00002000.parquet"
    assert cryo.replace_range(name, range(1000, 2000)) == new_name
