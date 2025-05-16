# Placeholder for actual tests

import logging
from typing import Any, Optional, Type
from unittest import mock

import pandas as pd

# Import functions from your main script
# Assuming your main script is in src/dwd_weather_pipeline/main.py
from src.dwd_weather_pipeline.main import DWD_BASE_URL  # Import DWD_BASE_URL for mocking
from src.dwd_weather_pipeline.main import (  # Add other functions you want to test here
    enrich_with_station_metadata,
    list_zip_files,
    transform_weather_data,
    validate_weather_data,
)


# Helper to capture log messages
class LogCapture:
    # Custom Handler to capture records
    class _CapturingHandler(logging.Handler):
        def __init__(self, records_list: list[logging.LogRecord]):
            super().__init__()
            self.records_list = records_list

        def emit(self, record: logging.LogRecord) -> None:
            self.records_list.append(record)

    def __init__(self) -> None:
        self.records: list[logging.LogRecord] = []
        # Use the custom handler
        self._capturing_handler = LogCapture._CapturingHandler(self.records)

    def __enter__(self) -> "LogCapture":
        # Set level on the handler itself if not set globally for the logger
        self._capturing_handler.setLevel(logging.WARNING)
        logging.getLogger().addHandler(self._capturing_handler)
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        logging.getLogger().removeHandler(self._capturing_handler)

    def messages(self) -> list[str]:
        return [record.getMessage() for record in self.records]


# Tests for transform_weather_data
def test_transform_weather_data_empty_df() -> None:
    df_empty = pd.DataFrame()
    transformed_df = transform_weather_data.fn(df_empty.copy())  # Use .fn() for Prefect tasks
    assert transformed_df.empty
    assert df_empty.equals(transformed_df)  # Ensure it returns the same empty df


def test_transform_weather_data_column_stripping_and_rename() -> None:
    data = {" STATIONS_ID ": [1], " MESS_DATUM ": [20230101], " WERT ": [10.0]}
    df = pd.DataFrame(data)
    expected_columns = ["STATIONS_ID", "DATE", "WERT"]
    transformed_df = transform_weather_data.fn(df.copy())
    assert list(transformed_df.columns) == expected_columns


def test_transform_weather_data_date_conversion_int() -> None:
    data = {"DATE": [20230101, 20230102]}
    df = pd.DataFrame(data, dtype="int64")  # Ensure original type is int64
    transformed_df = transform_weather_data.fn(df.copy())
    assert pd.api.types.is_datetime64_any_dtype(transformed_df["DATE"])
    assert transformed_df["DATE"].iloc[0] == pd.Timestamp("2023-01-01")


def test_transform_weather_data_date_conversion_str() -> None:
    data = {"DATE": ["20230101", "20230102"]}  # Dates as strings
    df = pd.DataFrame(data)
    transformed_df = transform_weather_data.fn(df.copy())
    assert pd.api.types.is_datetime64_any_dtype(transformed_df["DATE"])
    assert transformed_df["DATE"].iloc[0] == pd.Timestamp("2023-01-01")


def test_transform_weather_data_na_conversion() -> None:
    # Add a DATE column as the function expects it
    data = {
        "DATE": [20230101, 20230101, 20230101],
        "VALUE1": [1.0, -999.0, 3.0],
        "VALUE2": [-999, 5, 6],
    }
    df = pd.DataFrame(data)
    # Ensure DATE is string initially if it could be int, to simulate various inputs
    df["DATE"] = df["DATE"].astype(str)
    transformed_df = transform_weather_data.fn(df.copy())
    assert pd.isna(transformed_df["VALUE1"].iloc[1])
    assert pd.isna(transformed_df["VALUE2"].iloc[0])
    assert transformed_df["VALUE1"].iloc[0] == 1.0
    assert transformed_df["VALUE2"].iloc[1] == 5.0


# Tests for validate_weather_data
def test_validate_weather_data_empty_df() -> None:
    df_empty = pd.DataFrame()
    # For Prefect tasks, call the underlying function directly using .fn
    validated_df = validate_weather_data.fn(df_empty.copy())
    assert validated_df.empty


def test_validate_weather_data_valid_df() -> None:
    data = {
        "STATIONS_ID": ["00044"],
        "DATE": [pd.Timestamp("2023-01-01")],
        "QN_3": [1.0],
        "FX": [10.0],
        "FM": [9.0],
        "QN_4": [1.0],
        "RSK": [0.5],
        "RSKF": [1.0],
        "SDK": [8.0],
        "SHK_TAG": [0.0],
        "NM": [5.0],
        "VPM": [10.0],
        "TMK": [15.0],
        "UPM": [60.0],
        "TXK": [20.0],
        "TNK": [10.0],
        "TGK": [5.0],
        "EOR": ["eor"],
    }
    df = pd.DataFrame(data)
    df["DATE"] = pd.to_datetime(df["DATE"])

    validated_df = validate_weather_data.fn(df.copy())
    pd.testing.assert_frame_equal(validated_df, df)


def test_validate_weather_data_invalid_date_range() -> None:
    data = {
        "STATIONS_ID": ["00044"],
        "DATE": [pd.Timestamp("1800-01-01")],  # Invalid date
        "QN_3": [1.0],
        "FX": [10.0],
        "FM": [9.0],
        "QN_4": [1.0],
        "RSK": [0.5],
        "RSKF": [1.0],
        "SDK": [8.0],
        "SHK_TAG": [0.0],
        "NM": [5.0],
        "VPM": [10.0],
        "TMK": [15.0],
        "UPM": [60.0],
        "TXK": [20.0],
        "TNK": [10.0],
        "TGK": [5.0],
        "EOR": ["eor"],
    }
    df = pd.DataFrame(data)
    df["DATE"] = pd.to_datetime(df["DATE"])

    with LogCapture() as log_capture:
        validate_weather_data.fn(df.copy())
    assert any(
        "Some dates in data fall outside expected range." in msg for msg in log_capture.messages()
    )


# Tests for enrich_with_station_metadata
def test_enrich_with_station_metadata_empty_weather_df() -> None:
    df_weather_empty = pd.DataFrame(columns=["STATIONS_ID", "DATE"])
    df_meta = pd.DataFrame({"STATIONS_ID": [1], "NAME": ["Station A"]})
    enriched_df = enrich_with_station_metadata.fn(df_weather_empty.copy(), df_meta.copy())
    assert enriched_df.empty
    # Check cols from meta if weather_df had columns
    if not df_weather_empty.empty:
        assert "NAME" in enriched_df.columns


def test_enrich_with_station_metadata_successful_merge() -> None:
    df_weather = pd.DataFrame({"STATIONS_ID": [1, 2], "TEMP": [10, 12]})
    df_meta = pd.DataFrame({"STATIONS_ID": [1, 2], "NAME": ["Station A", "Station B"]})

    # Ensure STATIONS_ID is int as expected by the function after internal conversion
    df_weather["STATIONS_ID"] = df_weather["STATIONS_ID"].astype(int)
    df_meta["STATIONS_ID"] = df_meta["STATIONS_ID"].astype(int)

    enriched_df = enrich_with_station_metadata.fn(df_weather.copy(), df_meta.copy())
    assert "NAME" in enriched_df.columns
    assert len(enriched_df) == 2
    pd.testing.assert_frame_equal(
        enriched_df,
        pd.DataFrame({"STATIONS_ID": [1, 2], "TEMP": [10, 12], "NAME": ["Station A", "Station B"]}),
    )


# Tests for list_zip_files (requires mocking requests)
@mock.patch("src.dwd_weather_pipeline.main.requests.get")
def test_list_zip_files_no_filter(mock_get: mock.MagicMock) -> None:
    mock_response = mock.Mock()
    mock_response.text = """
    <html><body>
        <a href="tageswerte_KL_00001_abc.zip">link1</a>
        <a href="tageswerte_KL_00002_def.zip">link2</a>
        <a href="other_file.txt">link3</a>
        <a href="tageswerte_XX_00003_ghi.zip">link4_wrong_prefix</a>
    </body></html>
    """
    mock_get.return_value = mock_response

    zip_links = list_zip_files.fn()  # Call the underlying function
    assert len(zip_links) == 2
    assert "tageswerte_KL_00001_abc.zip" in zip_links
    assert "tageswerte_KL_00002_def.zip" in zip_links
    mock_get.assert_called_once_with(DWD_BASE_URL, timeout=10)


@mock.patch("src.dwd_weather_pipeline.main.requests.get")
def test_list_zip_files_with_station_ids_filter(mock_get: mock.MagicMock) -> None:
    mock_response = mock.Mock()
    mock_response.text = """
    <html><body>
        <a href="tageswerte_KL_00001_abc.zip">link1</a>
        <a href="tageswerte_KL_00044_def.zip">link2</a>
        <a href="tageswerte_KL_00003_xyz.zip">link3</a>
    </body></html>
    """
    mock_get.return_value = mock_response

    station_ids_to_filter = [44, 3]  # Filter for station 44 and 3
    zip_links = list_zip_files.fn(station_ids=station_ids_to_filter)
    assert len(zip_links) == 2
    assert "tageswerte_KL_00044_def.zip" in zip_links
    assert "tageswerte_KL_00003_xyz.zip" in zip_links
    mock_get.assert_called_once_with(DWD_BASE_URL, timeout=10)


@mock.patch("src.dwd_weather_pipeline.main.requests.get")
def test_list_zip_files_with_station_ids_filter_missing(mock_get: mock.MagicMock) -> None:
    mock_response = mock.Mock()
    mock_response.text = """
    <html><body>
        <a href="tageswerte_KL_00001_abc.zip">link1</a>
    </body></html>
    """
    mock_get.return_value = mock_response

    station_ids_to_filter = [44]  # Filter for station 44 which is not in mock html
    with LogCapture() as log_capture:
        zip_links = list_zip_files.fn(station_ids=station_ids_to_filter)

    assert len(zip_links) == 0
    assert any("No file found for station ID: 44" in msg for msg in log_capture.messages())
    mock_get.assert_called_once_with(DWD_BASE_URL, timeout=10)


# Add more tests as needed
# Example of a very simple placeholder test
def test_placeholder() -> None:
    assert True
