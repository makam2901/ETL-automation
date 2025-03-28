# ETL Automation Pipeline
This project is about how create an ETL pipeline and execute it automatically according to frequency without any human interference.

## Project Overview
This project generates a demographic analysis based on census data of each year. The pipeline would run every year when the data gets uploaded on the respective portal. The following set of tasks gets triggered once this data is uploaded each year.

<img width="1195" alt="image" src="https://github.com/user-attachments/assets/95b63d18-2540-4e61-9226-52058c25ad00" />


## Set Up
### 1. GCP 
- Google Cloud Composer with Airflow was set up with DAGs folder.
- MongoDB connection was created in Composer.
- All the required libraries and packages were installed in the composer.
- Google Cloud Storage was set up with project bucket.

### 2. MongoDB
- MongoDB Atlas was set up with a new cluster created.
- GCP is linked to the cluster.
- MongoDB Compass is linked to Atlas to locally view the aggregations.

### 3. Airflow
- DAG once created is uploaded in GCS bucket of the composer inside DAGs folder.
- Cross check for errors in Airflow UI launched from composer.

## DAG
The DAG consists of following components 
### 1. Helper functions
- Function to create a connection client for MongoDB
- Function to create a connection client for Google Cloud Storage

### 2. Tasks
- One task for each process defined in the above process flow diagram.
- There are single dependencies. Task 1 >> Task 2 >> ... >> Task 5
