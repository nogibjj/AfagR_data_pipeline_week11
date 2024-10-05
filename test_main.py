from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import select, update_query, delete_query


def test_extract():
    results = extract()
    assert results is not None


def test_load():
    results = load()
    assert results is not None


def test_readquery():
    assert select() == "Reading Success"


def test_updatequery():
    assert update_query() == "Update Success"


def test_deletequery():
    assert delete_query() == "Delete Success"


if __name__ == "__main__":
    test_extract()
    test_load()
    test_readquery()
    test_updatequery()
    test_deletequery()
