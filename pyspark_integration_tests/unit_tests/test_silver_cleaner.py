"""Tests for the Silver layer's cleansing rules (src/transform/silver_cleaner.py)."""
import pytest
from pyspark.sql import SparkSession

from src.transform.silver_cleaner import SilverCleaner

CONFIG = {"paths": {"silver": "/tmp/silver", "quarantine": "/tmp/quarantine"}}
COLUMNS = "trip_id INT, tripduration INT, starttime STRING, stoptime STRING, start_station_id INT, end_station_id INT, bikeid INT"


@pytest.fixture(scope="module")
def cleaner():
    spark = SparkSession.builder.master("local[1]").appName("silver-tests").getOrCreate()
    return SilverCleaner(spark, CONFIG)


def rows_to_df(cleaner, rows):
    return cleaner.spark.createDataFrame(rows, COLUMNS)


def test_valid_trip_passes_and_duplicates_are_dropped(cleaner):
    good = (1, 600, "2024-06-24 08:00:00", "2024-06-24 08:10:00", 10, 20, 111)
    clean, rejected = cleaner.clean_trips(rows_to_df(cleaner, [good, good]))
    assert clean.count() == 1
    assert rejected.count() == 0


@pytest.mark.parametrize(
    "bad_row, reason",
    [
        ((2, None, "2024-06-24 08:00:00", "2024-06-24 08:10:00", 10, 20, 111), "missing duration"),
        ((3, -5, "2024-06-24 08:00:00", "2024-06-24 08:10:00", 10, 20, 111), "negative duration"),
        ((4, 0, "2024-06-24 08:00:00", "2024-06-24 08:10:00", 10, 20, 111), "zero duration"),
        ((5, 600, None, "2024-06-24 08:10:00", 10, 20, 111), "missing start time"),
        ((6, 600, "2024-06-24 08:00:00", "2024-06-24 08:10:00", None, 20, 111), "missing start station"),
        ((7, 600, "2024-06-24 08:00:00", "2024-06-24 08:10:00", 10, None, 111), "missing end station"),
        ((8, 600, "2024-06-24 08:00:00", "2024-06-24 08:10:00", 10, 20, None), "missing bike id"),
    ],
)
def test_invalid_trips_are_rejected(cleaner, bad_row, reason):
    clean, rejected = cleaner.clean_trips(rows_to_df(cleaner, [bad_row]))
    assert clean.count() == 0, reason
    assert rejected.count() == 1, reason


def test_clean_and_rejected_account_for_every_unique_row(cleaner):
    rows = [
        (1, 600, "2024-06-24 08:00:00", "2024-06-24 08:10:00", 10, 20, 111),
        (2, 300, "2024-06-24 09:00:00", "2024-06-24 09:05:00", 11, 21, 112),
        (3, -1, "2024-06-24 10:00:00", "2024-06-24 10:05:00", 12, 22, 113),
    ]
    clean, rejected = cleaner.clean_trips(rows_to_df(cleaner, rows))
    assert clean.count() + rejected.count() == len(rows)
    assert sorted(r.trip_id for r in clean.collect()) == [1, 2]
    assert [r.trip_id for r in rejected.collect()] == [3]
