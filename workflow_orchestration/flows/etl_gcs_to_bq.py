from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

from etl_web_to_gcs import clear_data_folder


@task(retries=3)
def extract_from_gcs(data_folder: Path) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"uk_crimes"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"{data_folder}")

    return Path(f"{data_folder}/uk_crimes/")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning
    - 'Reported by' and 'Falls within' colums are the same so 'Falls within' is going to delete
    - Columns ['Crime ID', 'LSOA code', 'Context'] are not meaningfull so they are also going to delete
    - Delete rows with Nan values
    """
    df = pd.read_parquet(path)
    cols_to_drop = ["Crime ID", "LSOA code", "Context", "Falls within"]
    df.drop(cols_to_drop, axis=1, inplace=True)
    clean_df = df.dropna()
    final_df = clean_df.rename(
        columns={
            "Reported by": "Reported_by",
            "LSOA name": "LSOA_name",
            "Crime type": "Crime_type",
            "Last outcome category": "Last_outcome_category",
        }
    )
    return final_df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="uk_crimes_data.uk_crimes_data_all",
        project_id="de-zoomcamp-project-380514",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    data_folder = Path(
        "/home/bartoszsklodowski/Documents/Projekty treningowe/data-engineering/de_zoomcamp_project/workflow_orchestration/data"
    )
    path = extract_from_gcs(data_folder)
    datasets = path.glob("*")
    for dataset in datasets:
        df = transform(dataset)
        write_bq(df)

    clear_data_folder(path)


if __name__ == "__main__":
    etl_gcs_to_bq()
