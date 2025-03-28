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

## Results
### Before:
#### GCS - results folder
<img width="1432" alt="image" src="https://github.com/user-attachments/assets/722a6e46-edb1-45e1-a439-2d93c6e82094" />

#### MongoDB - database
<img width="1421" alt="image" src="https://github.com/user-attachments/assets/48b020cb-3f25-47a6-84f3-d2216ef14ddb" />

### After:
#### GCS - results folder
<img width="1430" alt="image" src="https://github.com/user-attachments/assets/67f7fb15-fb16-46cb-b048-f0edaf0e3ddb" />

#### MongoDB - database
<img width="1424" alt="image" src="https://github.com/user-attachments/assets/5c09d170-457e-4e80-9a2d-83f0543cfc2a" />

### Plots example
<img width="1432" alt="image" src="https://github.com/user-attachments/assets/f4a1975f-34d4-4e8e-8e37-f4bf1546e7b0" />

### Logs of Tasks
<img width="1479" alt="image" src="https://github.com/user-attachments/assets/96ba0be2-e5b3-4a56-a007-51e1fa2e4ab8" />
<img width="1467" alt="image" src="https://github.com/user-attachments/assets/414c2555-0626-4123-89bc-182d2788a0f8" />
<img width="1461" alt="image" src="https://github.com/user-attachments/assets/0fd278e0-6cee-454f-b365-7baee5bf9674" />









