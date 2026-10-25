import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

logger = logging.getLogger("retailpulse")


COLUMN_MAPPING = {
    "Invoice": "invoice",
    "StockCode": "stock_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date",
    "Price": "unit_price",
    "Customer ID": "customer_id",
    "Country": "country",
    "SourceSheet": "source_sheet",
}

TEXT_COLUMNS = [
    "invoice", "stock_code", "description", "country", "source_sheet"
]


def standardize_columns(bronze_df: DataFrame) -> DataFrame:
    """Standardize source column names, text, and data types."""
    df = bronze_df

    for old_name, new_name in COLUMN_MAPPING.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    missing = [
        name for name in COLUMN_MAPPING.values()
        if name not in df.columns
    ]
    if missing:
        raise ValueError(
            "Missing expected columns: " + ", ".join(missing)
        )

    for name in TEXT_COLUMNS:
        df = df.withColumn(
            name,
            F.when(
                F.trim(F.col(name)) == "",
                F.lit(None)
            ).otherwise(F.trim(F.col(name)))
        )

    return (
        df.withColumn("quantity", F.expr("try_cast(quantity AS INT)"))
        .withColumn(
            "invoice_date",
            F.to_timestamp("invoice_date", "yyyy-MM-dd HH:mm:ss")
        )
        .withColumn(
            "unit_price",
            F.col("unit_price").cast(DecimalType(12, 4))
        )
        .withColumn(
            "customer_id",
            F.regexp_replace(
                F.trim(F.col("customer_id")), r"\.0$", ""
            ).cast("long")
        )
    )


def classify_transactions(silver_df: DataFrame) -> DataFrame:
    """Classify transactions and attach data-quality flags."""
    return (
        silver_df.withColumn(
            "transaction_type",
            F.when(
                F.upper(F.col("invoice")).startswith("A")
                | (F.upper(F.col("stock_code")) == "B")
                | F.lower(F.col("description")).contains("adjust bad debt"),
                "ACCOUNTING_ADJUSTMENT"
            )
            .when(
                F.upper(F.col("invoice")).startswith("C"),
                "CANCELLATION_OR_CREDIT"
            )
            .when(
                F.col("quantity") < 0,
                "RETURN_OR_NEGATIVE_QUANTITY"
            )
            .otherwise("STANDARD_TRANSACTION")
        )
        .withColumn(
            "data_quality_flags",
            F.concat_ws(
                "; ",
                F.when(
                    F.col("customer_id").isNull(),
                    F.lit("Missing customer ID")
                ),
                F.when(
                    F.col("description").isNull(),
                    F.lit("Missing description")
                ),
                F.when(
                    F.col("unit_price") == 0,
                    F.lit("Zero unit price")
                ),
                F.when(
                    F.col("unit_price") < 0,
                    F.lit("Negative unit price")
                )
            )
        )
    )


def split_valid_and_rejected(
    classified_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Separate structural rejects from accepted records.

    Missing customers/descriptions and zero/negative prices remain quality
    flags; they are not, by themselves, structural rejection conditions.
    """
    df = classified_df.withColumn(
        "rejection_reason",
        F.concat_ws(
            "; ",
            F.when(F.col("invoice").isNull(), F.lit("Missing invoice")),
            F.when(F.col("stock_code").isNull(), F.lit("Missing stock code")),
            F.when(F.col("quantity").isNull(), F.lit("Invalid quantity")),
            F.when(
                F.col("invoice_date").isNull(),
                F.lit("Invalid invoice date")
            ),
            F.when(
                F.col("unit_price").isNull(),
                F.lit("Invalid unit price")
            ),
            F.when(F.col("country").isNull(), F.lit("Missing country"))
        )
    )

    rejected_df = df.filter(F.col("rejection_reason") != "")
    valid_df = df.filter(F.col("rejection_reason") == "").drop(
        "rejection_reason"
    )
    return valid_df, rejected_df


def transform_bronze_to_silver(
    bronze_df: DataFrame
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Return classified, accepted, and rejected DataFrames with logging."""
    logger.info("Starting Bronze-to-Silver transformation.")

    try:
        silver_df = standardize_columns(bronze_df)
        classified_df = classify_transactions(silver_df)
        valid_df, rejected_df = split_valid_and_rejected(classified_df)

        # Materialize counts so logged values reflect actual processing.
        classified_count = classified_df.count()
        accepted_count = valid_df.count()
        rejected_count = rejected_df.count()

        logger.info("Classified records: %s", classified_count)
        logger.info("Accepted records: %s", accepted_count)
        logger.info("Rejected records: %s", rejected_count)
        logger.info("Bronze-to-Silver transformation completed.")

        return classified_df, valid_df, rejected_df

    except Exception:
        logger.exception("Bronze-to-Silver transformation failed.")
        raise

