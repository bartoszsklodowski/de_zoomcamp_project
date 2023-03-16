# Project of a neural network predicting prices of the S&P 500 stock index

The aim of the project was to design the best possible neural network model to forecast the prices of the S&P 500 stock index, using historical data from 1982 to 2023.

The main libraries used to design the network were TensorFlow and Keras

## Description

Steps taken in the project:

* EDA(Exploratory Data Analysis) - graphical analysis of the data, validation and creation of a valid dataset for training and testing of the neural network
* feature engineering - development of a function that creates financially relevant features from the dataset received
* scaling of features and creation of training and testing sets
* creation, compilation, training and evaluation of the initial model
* tuning the parameters of the neural network using the hparams module and Tensorboards
* creation, compilation, training and evaluation of the final model with the best params
* comparison of actual and predict values on the plot

## Getting Started

> Live demo: [S&P 500 stock index](https://bartoszsklodowski-sp500-stock-values.streamlit.app/) 

### Dependencies

* Linux-Ubuntu 20.04
* Python 3.9
* Terraform v1.4.0
* Google Cloud SDK 421.0.0
* Anaconda 2022.10

### Installing

Steps to run:
Create conda environment:
'''bash
conda create -n environment_name python=3.9
'''

Install packages from requirements file using command:
'''bash
pip install -r requirements.txt
'''
Create an account on GCP
Create gcp project
Setup a service account for this project and download the JSON authentication key files
    Permissios to set:
        BigQuery Admin
        Storage Object Admin
        Compute Admin
    Enable these APIs for your project:
        * https://console.cloud.google.com/apis/library/iam.googleapis.com
        * https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com
    Set the environment variable to point to the auth keys
    ```bash
    export GOOGLE_APPLICATION_CREDENTIALS="<path/to/authkeys>.json"
    ```
    ```bash
    gcloud auth application-default login
    ```

Run Terraform commands to set cloud assets in folder `./infrastructure`
```bash
terraform init
terraform plan
terraform apply
```

#### Workflow orchestration

#### Kaggle
    * create Kaggle account
    * create API Token and put `kaggle.json` to a directory: `./home/.kaggle`

#### Prefect
Set gcp information in `make_gcp_blocks.py` file and file paths in others python scripts
Run python scripts:
```bash
prefect start orion
python blocks/make_gcp_blocks.py
python flows/etl_web_to_gcs.py
python flows/etl_gcs_to_bq.py
```


#### The first thing to do is to clone the repository:

```sh
$ git clone https://github.com/bartoszsklodowski/stock_values_NN.git
$ cd stock_values_NN
```

#### Create a virtual environment to install dependencies in and activate it:

```sh
$ python3 -m venv .venv
$ source .venv/bin/activate
```

#### Then install the dependencies:

```sh
(.venv)$ pip install -r requirements.txt
```
Note the `(.venv)` in front of the prompt. This indicates that this terminal
session operates in a virtual environment set up by `virtualenv`.


## Author

Bartosz Skłodowski 
