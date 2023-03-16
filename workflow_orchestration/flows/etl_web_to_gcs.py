import zipfile
import os
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def download_dataset(data_folder: Path) -> None:
    """Read crime data zip file from web into data folder"""
    os.chdir(data_folder)
    os.system(f"kaggle datasets download -d marshuu/crimes-in-uk-2023")
    return


@task(log_prints=True)
def unzip_dataset(data_folder: Path, dataset_name: str) -> None:
    """Unzip crime data file"""
    zip_path = f"{data_folder}/{dataset_name}"

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall("./")
    return


@task(retries=3)
def fetch(file_name: Path) -> pd.DataFrame:
    """Read crimes data from data folder into pandas DataFrame"""
    df = pd.read_csv(file_name)
    return df


@task()
def write_csv_to_parquet(df: pd.DataFrame, file_name: Path) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"{file_name.stem}.parquet")
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
    # Check if the folder exists
    if dataset_folder.exists():
        # Delete the files
        files = dataset_folder.glob("*")
        for file in files:
            file.unlink()
        print("Files deleted successfully.")
    else:
        print("Folder doesn't exist does not exist.")


@flow()
def prepare_datasets(dataset_url, dataset_folder):
    dataset_name = "crimes-in-uk-2023.zip"
    download_dataset(dataset_url, dataset_folder, dataset_name)
    unzip_dataset(dataset_folder, dataset_name)


@flow()
def etl_web_to_gcs(file_name: Path) -> None:
    """The main ETL function"""
    df = fetch(file_name)
    path = write_csv_to_parquet(df, file_name)
    write_gcs(path)


@flow()
def etl_parent_flow():
    dataset_url = "https://www.kaggle.com/datasets/marshuu/crimes-in-uk-2023/download?datasetVersionNumber=1"

    # Set path to data folder
    dataset_folder = Path(
        "/home/bartoszsklodowski/Documents/Projekty treningowe/data-engineering/de_zoomcamp_project/workflow_orchestration/data"
    )
    prepare_datasets(dataset_url, dataset_folder)

    dataset_names = dataset_folder.glob("*.csv")
    for file in dataset_names:
        etl_web_to_gcs(file)

    clear_data_folder(dataset_folder)


if __name__ == "__main__":
    etl_parent_flow()
