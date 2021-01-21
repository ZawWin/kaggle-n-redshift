# Airflow Pipeline From Kaggle to AWS Redshift

This project is an outcome of Udacity data engineering program capstone project. 

## DESCRIPTION ##

The goal of this capstone project is to enrich US Zipcode data with hospitals locations, population size, income information and zip code classification. The source data comes from Kaggle with minimal data transformation performed throughout the pipeline. Below is the list of data tables that are included in this project.

![Image](https://github.com/ZawWin/kaggle-n-redshift/blob/main/images/Zip%20Code%20ERD.jpg)

## Requirements ##
You will need to install:
* Python 3.6 and above. You can install it via anaconda distribution here at: https://www.anaconda.com/products/individual
* Additional python libraries - kaggle
* Apache-airflow 2.0

## Installation and Setup ##
1. First download anaconda distribution from URL above and install it to your computer. No fancy command line required for this step. The great thing about using this installation method is, the installation includes most commonly used python libraries pandas, numpy, scipy and so forth. Minimal additional setup required.

2. Install kaggle package by following the steps given on Kaggle website: https://www.kaggle.com/docs/api 
   * This step is required to use Kaggle API to download their dataset via python.
   
3. Install apache-airflow-2.0 by following the steps below.
   * First, open the terminal and type in the command.
  ```
    > conda create -n "airflow" python=3.7
    > conda activate airflow
  ```
  * Create a home folder where you would like your project files to be based at.
  ```
    > mkdir ~/Desktop/airflow
  ```
  * Set your airflow home to be that folder.
  ```
    > export AIRFLOW_HOME='~/Desktop/airflow'
  ```
  * Now you are ready to install apache_airflow.
  ```
    > pip install apache_airflow
  ```
  * Since you will be connecting to AWS environment, including s3 and redshift, you will also need to install these. (This step only applies to apache airflow 2.0)
  ```
    > pip install apache-airflow-providers-postgres
    > pip install apache-airflow-providers-amazon
  ```
  * You are now ready to start firing up the airflow.
  ```
    > airflow db init
    > airflow scheduler
  ```
  * While the scheduler is running (let it run), open a new terminal window and follow the steps.
  ```
    > conda activate airflow
    > export AIRFLOW_HOME = ~/Desktop/airflow
    > airflow webserver -p 8080
  ```
  * You can now go to localhost:8080 to access airflow UI.
  * For the first time starting airflow, it will require you to create a username and password. This is the new functionality with Airflow 2.0.
  * Open another new terminal window. Follow the same steps.
  ```
    > conda activate airflow
    > export AIRFLOW_HOME = ~/Desktop/airflow
    > airflow users create [-h] -e EMAIL -f FIRSTNAME -l LASTNAME [-p PASSWORD] -r
                     ROLE [--use-random-password] -u USERNAME
   ```
   * After the user is created, you can then go back to Airflow UI and log in with the newly created user.
   
## Running the job with Airflow ##
   * Download the current repository into ~/Desktop/airflow folder you created. That is your airflow home directory. When you go back to Airflow UI, you will see an error message. That's okay. Because you haven't created your redshift connection and s3_bucket in airflow yet.
   * Create your 's3_bucket' under variables in Airflow UI. Name exactly 's3_bucket' for variable name. The code will look for that name.
   * Under connections in Airflow UI, create the two following connections:
        * Amazon Web Services connection. Make sure the name is 'aws_credentials'.
        * Postgres connection. Make sure the name is 'redshift'.
   * If everything runs as expected, you should see error message clear up and see your dag in Dag list.
    
    
## Workflow ##

### Conceptual Diagram ###

![Image](https://github.com/ZawWin/kaggle-n-redshift/blob/main/images/Conceptual%20dataflow%20diagram.png)

### Actual Data Flow Diagram ###

![Image](https://github.com/ZawWin/kaggle-n-redshift/blob/main/images/Dataflow%20Diagram.png)

## Use Cases ##

What questions can be answered using this dataset? 

- Number of hospitals in low income zip area?
- Total Number of population and Average Income in low income zip code?
- Hospital locations and contact information along with their ratings?
- Correlation between number of hospitals and total number of population in each zip code and determine underserved areas.     
