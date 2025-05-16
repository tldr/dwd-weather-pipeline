import argparse
import io
import logging
import zipfile
from pathlib import Path
from typing import List, Optional

import pandas as pd
import pandera as pa
import requests
from bs4 import BeautifulSoup
from pandera import Column, DataFrameSchema
from prefect import flow, get_run_logger, task
from prefect.futures import PrefectFuture
from prefect.task_runners import ConcurrentTaskRunner
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

# -------------------- Config --------------------
DWD_BASE_URL = (
    "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/recent/"
)
STATION_FILE = "KL_Tageswerte_Beschreibung_Stationen.txt"
LOCAL_DATA_DIR = Path("data")
LOCAL_DATA_DIR.mkdir(exist_ok=True)

# Pandera schema (defined once, used in the processing task)
WEATHER_DATA_SCHEMA = DataFrameSchema(
    {
        "STATIONS_ID": Column(int),
        "DATE": Column(pa.DateTime, nullable=True),
        "QN_3": Column(float, nullable=True),
        "FX": Column(float, checks=pa.Check.ge(0), nullable=True),
        "FM": Column(float, checks=pa.Check.ge(0), nullable=True),
        "QN_4": Column(float, nullable=True),
        "RSK": Column(float, checks=pa.Check.ge(0), nullable=True),
        "RSKF": Column(float, nullable=True),
        "SDK": Column(float, checks=pa.Check.ge(0), nullable=True),
        "SHK_TAG": Column(float, checks=pa.Check.ge(0), nullable=True),
        "NM": Column(float, checks=pa.Check.in_range(0, 8), nullable=True),
        "VPM": Column(float, checks=pa.Check.ge(0), nullable=True),
        "TMK": Column(float, checks=pa.Check.in_range(-80, 60), nullable=True),
        "UPM": Column(float, checks=pa.Check.in_range(0, 100), nullable=True),
        "TXK": Column(float, checks=pa.Check.in_range(-80, 60), nullable=True),
        "TNK": Column(float, checks=pa.Check.in_range(-80, 60), nullable=True),
        "TGK": Column(float, checks=pa.Check.in_range(-80, 60), nullable=True),
        "EOR": Column(str, nullable=True),
    },
    coerce=True,
    strict=False,
)


# -------------------- Retryable Downloader (accepts logger) --------------------
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.RequestException),
)
def download_file_content(url: str, logger: logging.Logger) -> bytes:
    logger.info(f"Downloading {url}")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.content


# -------------------- Task: Scrape ZIP File Names --------------------
@task
def list_zip_files(station_ids: Optional[List[int]] = None) -> List[str]:
    logger = get_run_logger()
    logger.info("Listing zip files from DWD...")
    try:
        resp = requests.get(DWD_BASE_URL, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch main directory listing from DWD: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    links = [a.get("href", "") for a in soup.find_all("a", href=True)]
    zip_links = [
        link
        for link in links
        if isinstance(link, str) and link.endswith(".zip") and "tageswerte_KL_" in link
    ]

    if station_ids:
        logger.info(f"Filtering for station IDs: {station_ids}")
        filtered_links = []
        found_ids_in_names = set()
        for z_link in zip_links:
            try:
                file_station_id = int(z_link.split("_KL_")[-1].split("_")[0])
                if file_station_id in station_ids:
                    filtered_links.append(z_link)
                    found_ids_in_names.add(file_station_id)
            except (IndexError, ValueError):
                logger.warning(f"Could not parse station ID from filename: {z_link}")

        missing = set(station_ids) - found_ids_in_names
        for m_id in missing:
            logger.warning(f"No file found for requested station ID: {m_id:05d}")
        zip_links = filtered_links

    if not zip_links:
        logger.warning("No ZIP files found matching criteria.")
    else:
        logger.info(f"Found {len(zip_links)} ZIP files to process.")
    return zip_links


# -------------------- Task: Extract, Parse, Validate, Transform Single ZIP --------------------
@task(
    retries=2,
    retry_delay_seconds=10,
)
def extract_parse_validate_transform_single_zip_task(
    zip_file_name: str, base_url: str
) -> Optional[pd.DataFrame]:
    logger = get_run_logger()
    full_url = base_url + zip_file_name
    station_id_from_filename: Optional[int] = None
    try:
        station_id_str = zip_file_name.split("_KL_")[-1].split("_")[0]
        if station_id_str.isdigit():
            station_id_from_filename = int(station_id_str)
        else:
            logger.warning(f"Could not parse numeric station ID from {zip_file_name}")
    except IndexError:
        logger.warning(f"Could not parse station ID from filename structure: {zip_file_name}")

    logger.info(
        f"Processing ZIP: {zip_file_name} for station ID (from filename): "
        f"{station_id_from_filename if station_id_from_filename else 'Unknown'}"
    )

    try:
        zip_bytes = download_file_content(full_url, logger)
    except requests.RequestException as e:
        logger.error(f"Failed to download {zip_file_name} after multiple retries: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred during download of {zip_file_name}: {e}")
        return None

    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            product_files = [
                name
                for name in zf.namelist()
                if name.startswith("produkt_klima_tag_") and name.endswith(".txt")
            ]
            if not product_files:
                logger.warning(f"No 'produkt_klima_tag_*.txt' file found in {zip_file_name}.")
                return None
            if len(product_files) > 1:
                logger.warning(
                    f"Multiple product files in {zip_file_name}: {product_files}. "
                    f"Using first: {product_files[0]}"
                )

            product_file_name = product_files[0]
            logger.info(f"Extracting and parsing {product_file_name} from {zip_file_name}")

            try:
                df = pd.read_csv(
                    io.BytesIO(zf.read(product_file_name)),
                    sep=";",
                    na_values=["-999"],
                    encoding="latin1",
                )
            except pd.errors.EmptyDataError:
                logger.warning(f"Product file {product_file_name} in {zip_file_name} is empty.")
                return None
            except Exception as e:
                logger.error(f"Pandas error parsing {product_file_name} in {zip_file_name}: {e}")
                return None

            if df.empty:
                logger.warning(
                    f"Parsed DataFrame from {product_file_name} in {zip_file_name} is empty."
                )
                return None

            df.columns = df.columns.str.strip().str.upper()

            if "MESS_DATUM" in df.columns:
                df.rename(columns={"MESS_DATUM": "DATE"}, inplace=True)

            if "DATE" not in df.columns:
                logger.error(
                    f"'DATE' (or 'MESS_DATUM') not found in {product_file_name} "
                    f"from {zip_file_name}."
                )
                return None

            if df["DATE"].dtype == "int64" or df["DATE"].dtype == "object":
                df["DATE"] = pd.to_datetime(
                    df["DATE"].astype(str), format="%Y%m%d", errors="coerce"
                )
            else:
                df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")

            if df["DATE"].isnull().all():
                logger.warning(
                    f"All 'DATE' values null after conversion in {product_file_name} "
                    f"from {zip_file_name}."
                )

            if "STATIONS_ID" not in df.columns:
                if station_id_from_filename is not None:
                    logger.info(
                        f"Adding 'STATIONS_ID' {station_id_from_filename} from filename "
                        f"to data from {zip_file_name}."
                    )
                    df["STATIONS_ID"] = station_id_from_filename
                else:
                    logger.error(
                        f"'STATIONS_ID' missing in data and not derivable "
                        f"from filename {zip_file_name}."
                    )
                    return None
            else:
                df["STATIONS_ID"] = pd.to_numeric(df["STATIONS_ID"], errors="coerce")

            try:
                logger.debug(f"Validating data from {zip_file_name} with Pandera schema.")
                if "STATIONS_ID" in df.columns:
                    df["STATIONS_ID"] = df["STATIONS_ID"].astype(float).astype(pd.Int64Dtype())

                validated_df = WEATHER_DATA_SCHEMA.validate(df)
                assert isinstance(validated_df, pd.DataFrame)  # Hint for mypy
                logger.info(
                    f"Successfully processed and validated data from {zip_file_name} "
                    f"({len(validated_df)} rows)."
                )
                return validated_df
            except pa.errors.SchemaErrors as e:
                logger.error(
                    f"Pandera schema validation failed for {zip_file_name}:\n{e.failure_cases}"
                )
                return None
            except Exception as e:
                logger.error(f"Unexpected error during Pandera validation for {zip_file_name}: {e}")
                return None

    except zipfile.BadZipFile:
        logger.error(f"Corrupt or invalid ZIP file encountered: {zip_file_name}")
        return None
    except Exception as e:
        logger.error(
            f"An unexpected error occurred while processing {zip_file_name}: {e}", exc_info=True
        )
        return None


# -------------------- Flow: Process a Single ZIP (Subflow) --------------------
@flow
def process_single_zip_subflow(zip_file_name: str) -> PrefectFuture[Optional[pd.DataFrame]]:
    logger = get_run_logger()
    logger.info(f"Subflow for ZIP '{zip_file_name}' started, submitting task.")

    task_future = extract_parse_validate_transform_single_zip_task.submit(
        zip_file_name=zip_file_name, base_url=DWD_BASE_URL
    )
    return task_future


# -------------------- Flow: Download & Extract All (Parent Flow) --------------------
@flow(task_runner=ConcurrentTaskRunner(max_workers=10))
def download_and_process_all_zips_parent_flow(zip_names: list[str]) -> List[pd.DataFrame]:
    logger = get_run_logger()
    all_processed_dfs: List[pd.DataFrame] = []
    futures = []

    if not zip_names:
        logger.info("Parent flow received no zip names to process.")
        return []

    logger.info(f"Parent flow starting to process {len(zip_names)} ZIP files.")
    for zip_file_name in zip_names:
        station_id_str = "unknown_station"
        try:
            parsed_id = zip_file_name.split("_KL_")[-1].split("_")[0]
            if parsed_id.isdigit():
                station_id_str = f"{int(parsed_id):05d}"
        except (IndexError, ValueError):
            logger.debug(
                f"Could not parse station ID from zip_file_name for flow naming: {zip_file_name}"
            )

        task_future_from_subflow = process_single_zip_subflow.with_options(
            name=f"Process Station {station_id_str} (File: {zip_file_name})"
        )(zip_file_name=zip_file_name)
        futures.append(task_future_from_subflow)

    logger.info(
        f"All {len(futures)} ZIP processing subflows submitted. " f"Waiting for task results..."
    )

    for i, future in enumerate(futures):
        original_zip_name = zip_names[i]
        try:
            df_result: Optional[pd.DataFrame] = future.result()

            if isinstance(df_result, pd.DataFrame) and not df_result.empty:
                all_processed_dfs.append(df_result)
                logger.info(
                    f"Collected DataFrame ({len(df_result)} rows) "
                    f"from task for {original_zip_name}."
                )
            elif df_result is None:
                logger.warning(
                    f"Task for {original_zip_name} returned None " f"(no data or handled error)."
                )

        except Exception as e:
            logger.error(
                f"Task execution for {original_zip_name} (submitted by subflow) failed: {e}",
                exc_info=True,
            )

    logger.info(
        f"All task futures processed. Collected {len(all_processed_dfs)} non-empty DataFrames."
    )
    return all_processed_dfs


# -------------------- Task: Station Metadata --------------------
@task
def get_station_metadata() -> Optional[pd.DataFrame]:
    logger = get_run_logger()
    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    url = DWD_BASE_URL + STATION_FILE
    local_path = LOCAL_DATA_DIR / STATION_FILE

    logger.info(
        f"Fetching station metadata from {url if not local_path.exists() else str(local_path)}"
    )

    if not local_path.exists():
        try:
            content = download_file_content(url, logger)
            with open(local_path, "wb") as f:
                f.write(content)
            logger.info(f"Downloaded and saved station metadata to {local_path}")
        except requests.RequestException as e:
            logger.error(f"Failed to download station metadata: {e}")
            return None
        except Exception as e:
            logger.error(f"Error saving station metadata to {local_path}: {e}")
            return None

    colspecs = [
        (0, 5),  # Stations_id (e.g., "00044 ")
        (6, 14),  # von_datum (e.g., "19690101")
        (15, 23),  # bis_datum (e.g., "20250514")
        (24, 38),  # Stationshoehe (e.g., "44")
        (39, 50),  # geoBreite (e.g., "52.9336")
        (51, 60),  # geoLaenge (e.g., "8.2370")
        (61, 101),  # Stationsname (e.g., "Großenkneten")
        (102, 142),  # Bundesland (e.g., "Niedersachsen")
        (143, 147),  # Abgabe (e.g., "Frei")
    ]
    names = [
        "STATIONS_ID",
        "FROM_DATE",
        "TO_DATE",
        "ALTITUDE",
        "LAT",
        "LON",
        "STATION_NAME",
        "STATE",
        "STATUS",
    ]

    try:
        df_meta = pd.read_fwf(
            local_path,
            colspecs=colspecs,
            names=names,
            skiprows=2,
            encoding="latin1",
        )
        if df_meta.empty:
            logger.warning("Station metadata file was parsed as empty.")
            return None

        df_meta["STATIONS_ID"] = pd.to_numeric(df_meta["STATIONS_ID"], errors="coerce").astype(
            pd.Int64Dtype()
        )
        df_meta["ALTITUDE"] = pd.to_numeric(df_meta["ALTITUDE"], errors="coerce")
        df_meta["LAT"] = pd.to_numeric(df_meta["LAT"], errors="coerce")
        df_meta["LON"] = pd.to_numeric(df_meta["LON"], errors="coerce")
        df_meta["FROM_DATE"] = pd.to_datetime(
            df_meta["FROM_DATE"].astype(str), format="%Y%m%d", errors="coerce"
        )
        df_meta["TO_DATE"] = pd.to_datetime(
            df_meta["TO_DATE"].astype(str), format="%Y%m%d", errors="coerce"
        )

        string_cols = ["STATION_NAME", "STATE", "STATUS"]
        for col in string_cols:
            df_meta[col] = df_meta[col].astype(str).str.strip()

        df_meta.dropna(subset=["STATIONS_ID"], inplace=True)
        logger.info(f"Successfully loaded and processed station metadata: {df_meta.shape}")
        return df_meta
    except Exception as e:
        logger.error(f"Error processing station metadata file {local_path}: {e}")
        return None


# -------------------- Task: Enrich --------------------
@task
def enrich_with_station_metadata(
    df_weather: pd.DataFrame, df_meta: Optional[pd.DataFrame]
) -> pd.DataFrame:
    logger = get_run_logger()
    if df_weather.empty:
        logger.info("Enrichment skipped: main weather dataframe is empty.")
        return df_weather
    if df_meta is None or df_meta.empty:
        logger.warning("Enrichment skipped: station metadata is empty or not available.")
        return df_weather

    if "STATIONS_ID" not in df_weather.columns:
        logger.error("Cannot enrich: 'STATIONS_ID' missing in weather data.")
        return df_weather
    if "STATIONS_ID" not in df_meta.columns:
        logger.error("Cannot enrich: 'STATIONS_ID' missing in metadata.")
        return df_weather

    df_weather["STATIONS_ID"] = pd.to_numeric(df_weather["STATIONS_ID"], errors="coerce").astype(
        pd.Int64Dtype()
    )
    df_merged = df_weather.merge(df_meta, on="STATIONS_ID", how="left")
    logger.info(
        f"Enriched data. Original shape: {df_weather.shape}, Merged shape: {df_merged.shape}"
    )
    return df_merged


# -------------------- Task: Save Output --------------------
@task
def save_to_parquet(df: pd.DataFrame, output_path_str: str) -> None:
    logger = get_run_logger()
    if df.empty:
        logger.warning("Skipping save: final dataframe is empty.")
        return

    output_path = Path(output_path_str)
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        logger.info(f"Successfully saved final data ({df.shape}) to {output_path}")
    except Exception as e:
        logger.error(f"Failed to save data to {output_path}: {e}")


# -------------------- Flow: Main DWD Pipeline --------------------
@flow(name="DWD Weather Pipeline - Refactored")
def dwd_weather_pipeline(output_path: str, station_ids: Optional[List[int]] = None) -> None:
    logger = get_run_logger()
    logger.info(
        f"DWD Weather Pipeline started. Output: {output_path}, "
        f"Stations: {station_ids if station_ids else 'all'}"
    )

    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)

    zip_filenames = list_zip_files(station_ids=station_ids)

    if not zip_filenames:
        logger.warning(
            "No ZIP files found to process based on criteria. Pipeline terminating early."
        )
        return

    processed_dfs = download_and_process_all_zips_parent_flow(zip_names=zip_filenames)

    if not processed_dfs:
        logger.warning(
            "No data was successfully processed from any ZIP files. Pipeline terminating."
        )
        return

    logger.info(f"Concatenating {len(processed_dfs)} processed DataFrames.")
    try:
        combined_df = pd.concat(processed_dfs, ignore_index=True)
    except Exception as e:
        logger.error(f"Failed to concatenate DataFrames: {e}. Pipeline terminating.")
        return

    if combined_df.empty:
        logger.warning(
            "Combined DataFrame is empty after processing all files. No data to enrich or save."
        )
        return

    logger.info(f"Combined weather data shape: {combined_df.shape}")

    station_metadata_df = get_station_metadata()
    final_df = enrich_with_station_metadata(df_weather=combined_df, df_meta=station_metadata_df)

    save_to_parquet(df=final_df, output_path_str=output_path)
    logger.info("DWD Weather Pipeline finished.")


# -------------------- Main Execution Block --------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DWD Weather Pipeline - Refactored")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level for the main script execution (default: INFO)",
    )
    parser.add_argument(
        "--output-path",
        default="data/dwd_weather_data.parquet",
        help="Path to save the output parquet file (default: data/dwd_weather_data.parquet)",
    )
    parser.add_argument(
        "--station-ids",
        nargs="*",
        type=int,
        default=None,
        help=(
            "List of specific station IDs to process (e.g., 1048 433). "
            "If not provided, processes all recent stations."
        ),
    )
    args = parser.parse_args()

    script_logger = logging.getLogger(__name__)
    numeric_log_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_log_level, int):
        numeric_log_level = logging.INFO
        script_logger.warning(f"Invalid log level: {args.log_level}. Defaulting to INFO.")

    logging.basicConfig(
        level=numeric_log_level, format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
    )

    script_logger.info(
        f"Starting DWD Weather Pipeline script with log level {args.log_level.upper()}"
    )
    script_logger.info(f"Output path: {args.output_path}")
    station_ids_log_str = args.station_ids if args.station_ids is not None else "all recent"
    script_logger.info(f"Station IDs to process: {station_ids_log_str}")

    dwd_weather_pipeline(output_path=args.output_path, station_ids=args.station_ids)
