from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from retailpulse.transformations import (
    standardize_columns,
    classify_transactions,
    split_valid_and_rejected,
    transform_bronze_to_silver,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("RetailPulseTransformationTests")
        .getOrCreate()
    )
    yield session


@pytest.fixture
def bronze_df(spark):
    # Source-format rows covering all four transaction types,
    # a missing customer, a zero price, and a structurally invalid row.
    columns = [
        "Invoice", "StockCode", "Description", "Quantity",
        "InvoiceDate", "Price", "Customer ID", "Country", "SourceSheet"
    ]

    rows = [
        ("100001", "P001", "Standard product", "2",
         "2010-12-01 10:00:00", "10.00", "12345.0",
         "United Kingdom", "Year 2010"),

        ("C100002", "P002", "Credit product", "1",
         "2010-12-02 11:00:00", "5.00", "12346.0",
         "United Kingdom", "Year 2010"),

        ("100003", "B", "Adjust bad debt", "1",
         "2010-12-03 12:00:00", "-2.00", None,
         "United Kingdom", "Year 2010"),

        ("100004", "P004", "Returned product", "-1",
         "2010-12-04 13:00:00", "3.00", "12347.0",
         "France", "Year 2010"),

        ("A100005", "P005", "Accounting adjustment", "1",
         "2010-12-05 14:00:00", "0.00", None,
         "United Kingdom", "Year 2010"),

        ("100006", "P006", None, "1",
         "2010-12-06 15:00:00", "4.00", None,
         "France", "Year 2010"),

        ("100007", "P007", "Bad quantity", "not-a-number",
         "2010-12-07 16:00:00", "2.00", "12348.0",
         "France", "Year 2010"),

        ("100008", "P008", "Trim test  ", "1",
         "2010-12-08 17:00:00", "1.25", "12349.0",
         "France", "Year 2010"),
    ]

    return spark.createDataFrame(rows, columns)


def test_standardize_columns_and_types(spark, bronze_df):
    df = standardize_columns(bronze_df)

    assert "invoice" in df.columns
    assert "Invoice" not in df.columns

    row = df.filter(df.invoice == "100001").first()
    assert row.quantity == 2
    assert row.customer_id == 12345
    assert row.unit_price == Decimal("10.0000")
    assert row.invoice_date == datetime(2010, 12, 1, 10, 0, 0)

    trimmed = df.filter(df.invoice == "100008").first()
    assert trimmed.description == "Trim test"


def test_transaction_classification_and_precedence(bronze_df):
    classified, valid, rejected = transform_bronze_to_silver(bronze_df)

    actual = {
        row.invoice: row.transaction_type
        for row in classified.select(
            "invoice", "transaction_type"
        ).collect()
    }

    assert actual["100001"] == "STANDARD_TRANSACTION"
    assert actual["C100002"] == "CANCELLATION_OR_CREDIT"
    assert actual["100003"] == "ACCOUNTING_ADJUSTMENT"
    assert actual["100004"] == "RETURN_OR_NEGATIVE_QUANTITY"
    assert actual["A100005"] == "ACCOUNTING_ADJUSTMENT"

    # Accounting adjustment takes precedence over other conditions.
    assert actual["A100005"] != "STANDARD_TRANSACTION"

    assert valid.count() == 7
    assert rejected.count() == 1


def test_quality_flags_are_preserved(bronze_df):
    classified, valid, rejected = transform_bronze_to_silver(bronze_df)

    rows = {
        row.invoice: row
        for row in classified.select(
            "invoice", "description", "customer_id",
            "unit_price", "data_quality_flags"
        ).collect()
    }

    assert "Missing customer ID" in rows["100006"].data_quality_flags
    assert "Missing description" in rows["100006"].data_quality_flags
    assert "Zero unit price" in rows["A100005"].data_quality_flags
    assert "Negative unit price" in rows["100003"].data_quality_flags

    # These are quality flags, not automatic rejection reasons.
    assert valid.filter(valid.invoice == "100006").count() == 1
    assert valid.filter(valid.invoice == "A100005").count() == 1


def test_rejection_reason_for_invalid_quantity(bronze_df):
    classified, valid, rejected = transform_bronze_to_silver(bronze_df)

    invalid = rejected.select(
        "invoice", "rejection_reason"
    ).first()

    assert invalid.invoice == "100007"
    assert "Invalid quantity" in invalid.rejection_reason


def test_row_count_reconciliation(bronze_df):
    classified, valid, rejected = transform_bronze_to_silver(bronze_df)

    assert classified.count() == valid.count() + rejected.count()
    assert classified.count() == 8


def test_blank_text_becomes_null(bronze_df):
    from pyspark.sql import functions as F

    df = bronze_df.withColumn(
        "Description",
        F.when(F.col("Invoice") == "100001", F.lit("   "))
        .otherwise(F.col("Description"))
    )

    standardized = standardize_columns(df)
    row = standardized.filter(
        standardized.invoice == "100001"
    ).first()

    assert row.description is None
