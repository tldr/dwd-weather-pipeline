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
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from prefect import flow, task

# -------------------- Config --------------------
DWD_BASE_URL = (
    "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/recent/"
)
STATION_FILE = "KL_Tageswerte_Beschreibung_Stationen.txt"
LOCAL_DATA_DIR = Path("data")
RAW_ARCHIVE_DIR = Path("raw_data")
EXTRACT_DIR = LOCAL_DATA_DIR / "extracted"
LOCAL_DATA_DIR.mkdir(exist_ok=True)
RAW_ARCHIVE_DIR.mkdir(exist_ok=True)
EXTRACT_DIR.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# -------------------- Retryable Downloader --------------------
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.RequestException),
)
def download_file(url: str) -> bytes:
    logger.info(f"Downloading {url}")
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.content


# -------------------- Task: Scrape ZIP Files --------------------
@task
def list_zip_files(station_ids: Optional[List[int]] = None) -> List[str]:
    logger.info("Listing zip files from DWD...")
    resp = requests.get(DWD_BASE_URL, timeout=10)
    soup = BeautifulSoup(resp.text, "html.parser")
    links = [a.get("href", "") for a in soup.find_all("a", href=True)]
    zip_links = [
        link
        for link in links
        if isinstance(link, str) and link.endswith(".zip") and "tageswerte_KL_" in link
    ]
    if station_ids:
        filtered_links = [
            z for z in zip_links if any(f"_{station_id:05d}_" in z for station_id in station_ids)
        ]
        missing = set(station_ids) - {int(z.split("_")[-2]) for z in filtered_links}
        for m in missing:
            logger.warning(f"No file found for station ID: {m}")
        zip_links = filtered_links
    logger.info(f"Found {len(zip_links)} ZIP files to process")
    return zip_links


# -------------------- Task: Extract a Single ZIP --------------------
@task
def extract_zip_file(zip_file: str) -> list[str]:
    url = DWD_BASE_URL + zip_file
    extracted_files = []
    try:
        zip_bytes = download_file(url)
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            for name in zf.namelist():
                if name.endswith(".txt"):
                    out_path = EXTRACT_DIR / name
                    with open(out_path, "wb") as f:
                        f.write(zf.read(name))
                    extracted_files.append(str(out_path))
    except Exception as e:
        logger.error(f"Error processing {zip_file}: {e}")
    return extracted_files


# -------------------- Task: Download & Extract All --------------------
@task
def download_and_extract_all(zip_links: list[str]) -> list[str]:
    all_extracted = []
    for zip_file in zip_links:
        extracted = extract_zip_file(zip_file)
        all_extracted.extend(extracted)
    logger.info(f"Extracted {len(all_extracted)} .txt files")
    return all_extracted


# -------------------- Task: Parse and Concatenate --------------------
@task
def parse_weather_files(files: list[str]) -> pd.DataFrame:
    dfs: list[pd.DataFrame] = []
    for file_path in files:
        try:
            # Skip metadata files
            if "Metadaten_" in file_path:
                continue

            # Only process produkt_klima_tag files
            if "produkt_klima_tag" not in file_path:
                continue

            # Read the file with -999 as NA value
            df = pd.read_csv(file_path, sep=";", na_values=["-999"], encoding="latin1")
            if df.empty:
                logger.warning(f"File {file_path} is empty. Skipping.")
                continue
            if "STATIONS_ID" not in df.columns:
                logger.warning(f"File {file_path} missing expected columns. Skipping.")
                continue
            # Convert station ID to string
            df["STATIONS_ID"] = df["STATIONS_ID"].astype(str)
            dfs.append(df)
        except Exception as e:
            logger.error(f"Error parsing {file_path}: {e}")
    if not dfs:
        logger.error("No valid weather data files were parsed.")
        return pd.DataFrame(columns=["STATIONS_ID", "DATE"])
    combined = pd.concat(dfs, ignore_index=True)
    logger.info(f"Combined dataset shape: {combined.shape}")
    return combined


# -------------------- Task: Validate Weather --------------------
@task
def validate_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        logger.warning("Validation skipped: empty dataframe.")
        return df
    if df["DATE"].min() < pd.Timestamp("1900-01-01") or df["DATE"].max() > pd.Timestamp.now():
        logger.warning("Some dates in data fall outside expected range.")
    schema = DataFrameSchema(
        {
            "STATIONS_ID": Column(str),
            "DATE": Column(pa.DateTime),
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
    return schema.validate(df)  # type: ignore[no-any-return]


# -------------------- Task: Transform --------------------
@task
def transform_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        logger.warning("Transformation skipped: empty dataframe.")
        return df
    # Strip whitespace from column names
    df.columns = df.columns.str.strip().str.upper()
    df.rename(columns={"MESS_DATUM": "DATE"}, inplace=True)

    # Convert date column to datetime, handling both string and integer formats
    if df["DATE"].dtype == "int64":
        df["DATE"] = pd.to_datetime(df["DATE"].astype(str), format="%Y%m%d")
    else:
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")

    # Replace -999 values with NaN
    numeric_cols = df.select_dtypes(include=["float64", "int64"]).columns
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col].replace(-999, pd.NA), errors="coerce")

    return df


# -------------------- Task: Station Metadata --------------------
@task
def get_station_metadata() -> pd.DataFrame:
    url = DWD_BASE_URL + STATION_FILE
    local_path = LOCAL_DATA_DIR / STATION_FILE
    if not local_path.exists():
        content = download_file(url)
        with open(local_path, "wb") as f:
            f.write(content)

    # Column specifications based on actual data rows (skipping header and separator)
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
    df = pd.read_fwf(local_path, colspecs=colspecs, names=names, skiprows=2, encoding="latin1")

    # Convert numeric columns
    df["STATIONS_ID"] = pd.to_numeric(
        df["STATIONS_ID"].astype(str).str.strip(), errors="coerce"
    ).astype(int)
    df["ALTITUDE"] = pd.to_numeric(df["ALTITUDE"].astype(str).str.strip(), errors="coerce")
    df["LAT"] = pd.to_numeric(df["LAT"].astype(str).str.strip(), errors="coerce")
    df["LON"] = pd.to_numeric(df["LON"].astype(str).str.strip(), errors="coerce")

    # Convert dates from YYYYMMDD format to datetime
    df["FROM_DATE"] = pd.to_datetime(df["FROM_DATE"].astype(str).str.strip(), format="%Y%m%d")
    df["TO_DATE"] = pd.to_datetime(df["TO_DATE"].astype(str).str.strip(), format="%Y%m%d")

    # Clean up string columns
    string_cols = ["STATION_NAME", "STATE", "STATUS"]
    for col in string_cols:
        df[col] = df[col].astype(str).str.strip()

    return df


# -------------------- Task: Enrich --------------------
@task
def enrich_with_station_metadata(df_weather: pd.DataFrame, df_meta: pd.DataFrame) -> pd.DataFrame:
    if df_weather.empty:
        logger.warning("Enrichment skipped: empty weather data.")
        return df_weather

    # Log data types before merge
    logger.info(f"Weather data STATIONS_ID type: {df_weather['STATIONS_ID'].dtype}")
    logger.info(f"Metadata STATIONS_ID type: {df_meta['STATIONS_ID'].dtype}")

    # Ensure both DataFrames have the same type for STATIONS_ID
    df_weather["STATIONS_ID"] = df_weather["STATIONS_ID"].astype(int)
    df_meta["STATIONS_ID"] = df_meta["STATIONS_ID"].astype(int)

    # Log data types after conversion
    logger.info(
        f"After conversion - Weather data STATIONS_ID type: {df_weather['STATIONS_ID'].dtype}"
    )
    logger.info(f"After conversion - Metadata STATIONS_ID type: {df_meta['STATIONS_ID'].dtype}")

    df_merged = df_weather.merge(df_meta, on="STATIONS_ID", how="left")
    logger.info(f"Merged data shape: {df_merged.shape}")
    return df_merged


# -------------------- Task: Save Output --------------------
@task
def save_to_parquet(df: pd.DataFrame, output_path: str) -> None:
    """Save DataFrame to parquet format."""
    if df.empty:
        logger.warning("Skipping save: empty dataframe.")
        return
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved data to {output_path}")


# -------------------- Flow --------------------
@flow(name="DWD Weather Pipeline - Full ZIP Import")
def dwd_weather_pipeline(output_path: str, station_ids: Optional[List[int]] = None) -> None:
    zip_links = list_zip_files(station_ids)
    if not zip_links:
        logger.error("No ZIP files found to process. Pipeline terminating.")
        return
    extracted = download_and_extract_all(zip_links)
    df = parse_weather_files(extracted)
    df = transform_weather_data(df)
    df = validate_weather_data(df)
    meta = get_station_metadata()
    enriched = enrich_with_station_metadata(df, meta)
    save_to_parquet(enriched, output_path)


# -------------------- Main --------------------
if __name__ == "__main__":
    dwd_weather_pipeline("data/dwd_all_stations.parquet", [44])
