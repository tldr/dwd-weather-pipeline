# Placeholder for actual tests

import logging
from typing import Any, Optional, Type
from unittest import mock

import pandas as pd

# Import functions from your main script
# Assuming your main script is in src/dwd_weather_pipeline/main.py
from dwd_weather_pipeline.main import DWD_BASE_URL  # Import DWD_BASE_URL for mocking
from dwd_weather_pipeline.main import (  # Add other functions you want to test here
    enrich_with_station_metadata,
    list_zip_files,
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


# Tests for enrich_with_station_metadata
@mock.patch("dwd_weather_pipeline.main.get_run_logger")
def test_enrich_with_station_metadata_empty_weather_df(mock_get_run_logger: mock.MagicMock) -> None:
    mock_get_run_logger.return_value = logging.getLogger("test_logger_enrich_empty")
    df_weather_empty = pd.DataFrame(columns=["STATIONS_ID", "DATE"])
    df_meta = pd.DataFrame({"STATIONS_ID": [1], "NAME": ["Station A"]})
    enriched_df = enrich_with_station_metadata.fn(df_weather_empty.copy(), df_meta.copy())
    assert enriched_df.empty
    # Check cols from meta if weather_df had columns
    if not df_weather_empty.empty:
        assert "NAME" in enriched_df.columns


@mock.patch("dwd_weather_pipeline.main.get_run_logger")
def test_enrich_with_station_metadata_successful_merge(mock_get_run_logger: mock.MagicMock) -> None:
    mock_get_run_logger.return_value = logging.getLogger("test_logger_enrich_success")
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
        pd.DataFrame(
            {
                "STATIONS_ID": pd.Series([1, 2], dtype=pd.Int64Dtype()),
                "TEMP": [10, 12],
                "NAME": ["Station A", "Station B"],
            }
        ),
    )


# Tests for list_zip_files (requires mocking requests)
@mock.patch("dwd_weather_pipeline.main.get_run_logger")
@mock.patch("dwd_weather_pipeline.main.requests.get")
def test_list_zip_files_no_filter(
    mock_requests_get: mock.MagicMock, mock_get_run_logger: mock.MagicMock
) -> None:
    mock_get_run_logger.return_value = logging.getLogger("test_logger_list_zip_no_filter")
    mock_response = mock.Mock()
    mock_response.text = """
    <html><body>
        <a href="tageswerte_KL_00001_abc.zip">link1</a>
        <a href="tageswerte_KL_00002_def.zip">link2</a>
        <a href="other_file.txt">link3</a>
        <a href="tageswerte_XX_00003_ghi.zip">link4_wrong_prefix</a>
    </body></html>
    """
    mock_requests_get.return_value = mock_response

    zip_links = list_zip_files.fn()  # Call the underlying function
    assert len(zip_links) == 2
    assert "tageswerte_KL_00001_abc.zip" in zip_links
    assert "tageswerte_KL_00002_def.zip" in zip_links
    mock_requests_get.assert_called_once_with(DWD_BASE_URL, timeout=10)


@mock.patch("dwd_weather_pipeline.main.get_run_logger")
@mock.patch("dwd_weather_pipeline.main.requests.get")
def test_list_zip_files_with_station_ids_filter(
    mock_requests_get: mock.MagicMock, mock_get_run_logger: mock.MagicMock
) -> None:
    mock_get_run_logger.return_value = logging.getLogger("test_logger_list_zip_filter")
    mock_response = mock.Mock()
    mock_response.text = """
    <html><body>
        <a href="tageswerte_KL_00001_abc.zip">link1</a>
        <a href="tageswerte_KL_00044_def.zip">link2</a>
        <a href="tageswerte_KL_00003_xyz.zip">link3</a>
    </body></html>
    """
    mock_requests_get.return_value = mock_response

    station_ids_to_filter = [44, 3]  # Filter for station 44 and 3
    zip_links = list_zip_files.fn(station_ids=station_ids_to_filter)
    assert len(zip_links) == 2
    assert "tageswerte_KL_00044_def.zip" in zip_links
    assert "tageswerte_KL_00003_xyz.zip" in zip_links
    mock_requests_get.assert_called_once_with(DWD_BASE_URL, timeout=10)


@mock.patch("dwd_weather_pipeline.main.get_run_logger")
@mock.patch("dwd_weather_pipeline.main.requests.get")
def test_list_zip_files_with_station_ids_filter_missing(
    mock_requests_get: mock.MagicMock, mock_get_run_logger: mock.MagicMock
) -> None:
    mock_get_run_logger.return_value = logging.getLogger("test_logger_list_zip_missing")
    mock_response = mock.Mock()
    mock_response.text = """
    <html><body>
        <a href="tageswerte_KL_00001_abc.zip">link1</a>
    </body></html>
    """
    mock_requests_get.return_value = mock_response

    station_ids_to_filter = [44]  # Filter for station 44 which is not in mock html
    with LogCapture() as log_capture:
        zip_links = list_zip_files.fn(station_ids=station_ids_to_filter)

    assert len(zip_links) == 0
    assert any(
        "No file found for requested station ID: 00044" in msg for msg in log_capture.messages()
    )
    mock_requests_get.assert_called_once_with(DWD_BASE_URL, timeout=10)


# Add more tests as needed
# Example of a very simple placeholder test
def test_placeholder() -> None:
    assert True
