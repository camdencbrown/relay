"""Tests for src.utils"""

from src.utils import sanitize_table_name


def test_basic():
    assert sanitize_table_name("My Pipeline") == "my_pipeline"


def test_hyphens():
    assert sanitize_table_name("sales-data") == "sales_data"


def test_leading_digit():
    assert sanitize_table_name("2024 sales") == "t_2024_sales"


def test_special_chars():
    assert sanitize_table_name("users@v2!") == "usersv2"


def test_empty():
    assert sanitize_table_name("") == ""


def test_already_clean():
    assert sanitize_table_name("customers") == "customers"
