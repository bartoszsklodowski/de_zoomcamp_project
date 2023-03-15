from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta
import zipfile
import os


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_dataset(dataset_url: str, data_folder: Path) -> None:
    """Read crime data zip file from web into data folder"""
    os.system(f"wget -P {data_folder} {dataset_url}")
    return 


@task(log_prints=True)
def unzip_dataset(data_folder: Path) -> None:
    """Unzip crime data file"""
    zip_path = f"{data_folder}/archive.zip"

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall()
    return


@task(retries=3)
def fetch(dataset_path: Path) -> pd.DataFrame:
    """Read crimes data from data folder into pandas DataFrame"""
    df = pd.read_csv(dataset_path)
    return df


@task()
def write_csv_to_parquet(df: pd.DataFrame, dataset_path: Path) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"{dataset_path}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=f"uk_crimes/{path}")
    return


@task()
def clear_data_folder(dataset_folder: Path) -> None:
    """Clear data folder"""

    # Check if the file exists
    if dataset_folder.exists():
        # Delete the file
        dataset_folder.unlink()
        print("File deleted successfully.")
    else:
        print("File does not exist.")


@flow()
def prepare_datasets(dataset_url, dataset_folder):

    download_dataset(dataset_url, dataset_folder)
    unzip_dataset(dataset_folder)


@flow()
def etl_web_to_gcs(dataset_path: Path) -> None:
    """The main ETL function"""
    df = fetch(dataset_path)
    path = write_csv_to_parquet(df, dataset_path)
    write_gcs(path)


@flow()
def etl_parent_flow():
    dataset_url = "https://www.kaggle.com/datasets/marshuu/crimes-in-uk-2023/download?datasetVersionNumber=1"
    dataset_folder = Path("workflow_orchestration/flows/data")
    prepare_datasets(dataset_url, dataset_folder)

    dataset_paths = dataset_folder.glob('*.csv')
    for path in dataset_paths:
        etl_web_to_gcs(path)

    clear_data_folder(dataset_folder)


if __name__ == "__main__":
    etl_parent_flow()
