# Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

# Python
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt

# MongoDB
from pymongo import MongoClient

# Google
from google.cloud import storage

# System
from io import BytesIO

# Define default arguments
default_args = {
    "owner": "group_13",
    "start_date": datetime(2025, 2, 21),
    "retries": 0,
}

# Connection Functions
def connect_to_mongoatlas():
    conn_id = 'mongo_atlas'
    print("Connecting to MongoDB...")
    try:
        conn = BaseHook.get_connection(conn_id)
        mongo_conn_str = f"mongodb+srv://{conn.login}:{conn.password}@{conn.host}/"
        print("Connected to MongoDB Atlas cluster")
        client = MongoClient(mongo_conn_str)
        return client
    except Exception as e:
        print("Connection to MongoDB Atlas cluster failed")
        raise Exception("MongoDB connection failed")
    
# Upload to GCS Bucket
def upload_to_gcs(data, content_type: str, bucket_name: str, file_name: str):
    """Upload data to Google Cloud Storage using default credentials."""
    try:
        print('Entered the upload chunk')
        client = storage.Client()  # Uses Cloud Composer's default credentials
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_string(data, content_type=content_type)
        print(f"Data uploaded to GCS successfully: gs://{bucket_name}/{file_name}")
    except Exception as e:
        print(f"Failed to upload to GCS: {e}")
    client.close()

# Task 1: Load and Clean Data from GCS, then Store in MongoDB
def load_and_clean_data():
    year = 2009
    # year = datetime.now().year # updates every year
    gcs_bucket = "msds-697-final-project-group-13"
    gcs_path = f"gs://{gcs_bucket}/sdoh_data/{year}/SDOH_{year}_COUNTY_1_0.xlsx"

    
    mongo_db = f"year{year}"
    mongo_collection = f"county_health"

    # Read Excel file from GCS
    df = pd.read_excel(gcs_path, sheet_name='Data', engine='openpyxl')
    print(f'Imported excel data of shape {df.shape}')

    df_info = pd.DataFrame({
        'Column': df.columns,
        'Non-Null Count': df.notnull().sum().values,
        'Dtype': df.dtypes.values,
        'Unique Value Count': df.nunique().values,
        'Missing Count': df.isna().sum().values,
        'Missing Proportion': df.isna().sum().values / len(df)
    })
    missing_over_X_percent = df_info[df_info['Missing Proportion'] > 0.5]
    print(f'{len(missing_over_X_percent)} columns out of {df.shape[1]} are missing over 50% of data')
    cols_to_drop = missing_over_X_percent['Column'].to_list()
    cols_to_drop.append('YEAR')
    df = df.drop(cols_to_drop, axis=1)

    # Store cleaned data in MongoDB
    client = connect_to_mongoatlas()
    db = client[mongo_db]
    collection = db[mongo_collection]
    collection.insert_many(df.to_dict(orient="records"))
    print(f"Cleaned data stored in MongoDB collection: {mongo_collection}")

    # Stop Spark session
    client.close()

# Task 2: Aggregate and Store Data
def aggregate_and_store():
    year = 2009
    # year = datetime.now().year # updates every year

    db_name = f'year{year}'
    collection = f'county_health'

    client = connect_to_mongoatlas()
    db = client[db_name]
    print(f'Collection "{collection}" is accessed')
    
    c = db[collection]

    # Aggregation setup
    print("Starting Aggregation Process")

    # Step 1: Find number of documents
    print('Step 1/6')
    doc_count = c.count_documents({})
    
    print(f"There are a total of {doc_count} records in this collection")

    # Step 2: Find total population above 18 per county
    print('Step 2/6')
    output2 = 'pop_above_18'
    pipeline2 = [
        {
            "$group": {
                "_id": "$COUNTY",
                "total_population_above_18": {
                    "$sum": "$ACS_TOT_CIVIL_POP_ABOVE18"
                }
            }
        },
        {
            "$sort": { "total_population_above_18": -1 }
        },
        {
            "$limit": 10
        }
    ]
    agg2 = list(c.aggregate(pipeline2, allowDiskUse=True))
    db[output2].insert_many(agg2)

    # Step 3: Find the top 10 counties with the highest total population
    print('Step 3/6')
    output3 = 'highest_pop'
    pipeline3 = [
        {
            "$group": {
                "_id": "$COUNTY",
                "total_population": {
                     "$sum": "$ACS_TOT_POP_WT"
                }
            }
        },
        {
            "$sort": {
                "total_population":-1
            }
        },
        {
            "$limit": 10
        }
    ]
    agg3 = list(c.aggregate(pipeline3, allowDiskUse=True))
    db[output3].insert_many(agg3)

    # Step 4: Find the average number of households per state
    print('Step 4/6')
    output4 = 'avg_household'
    pipeline4 = [
        {
            "$group": {
                "_id": "$STATE",
                "avg_households": {
                    "$avg": "$ACS_TOT_HH" 
                }
            }
        }
    ]
    agg4 = list(c.aggregate(pipeline4, allowDiskUse=True))
    db[output4].insert_many(agg4)

    # Step 5: Get population distribution by age group in each state
    print('Step 5/6')
    output5 = 'age_pop'
    pipeline5 = [
        {
            "$group": {
                "_id": "$STATE",
                "pop_above_5": {
                    "$sum": "$ACS_TOT_POP_ABOVE5" 
                },
                "pop_above_15": {
                    "$sum": "$ACS_TOT_POP_ABOVE15"
                },
                "pop_above_25": {
                    "$sum": "$ACS_TOT_POP_ABOVE25"
                }
            }
        }
    ]
    agg5 = list(c.aggregate(pipeline5, allowDiskUse=True))
    db[output5].insert_many(agg5)

    # Step 6: Find states with the highest number of unemployed civilians
    print('Step 6/6')
    output6 = 'unemp_pop'
    pipeline6 = [
        {
            "$group": {
                "_id": "$STATE",
                "unemployed": {
                    "$sum": "$ACS_TOT_CIVIL_EMPLOY_POP"
                }
            }
        },
        {
            "$sort": {
                "unemployed": -1
            }
        },
        {
            "$limit": 10
        }
    ]
    agg6 = list(c.aggregate(pipeline6, allowDiskUse=True))
    db[output6].insert_many(agg6)

    client.close()

# Task 3: Generate Analysis and Upload Plot to GCS
def get_analysis():
    year = 2009
    # year = datetime.now().year # updates every year
    client = connect_to_mongoatlas()
    db_name = f'year{year}'
    db = client[db_name]

    # Plot 1
    print('Generating Plot 1/3')
    input1 = 'age_pop'
    collection1 = db[input1]

    df1 = pd.DataFrame(list(collection1.find()))
    fig1, ax1 = plt.subplots(figsize=(10, 5))

    colors = ["mediumseagreen", "blue", "salmon"]
    for col, color in zip(["pop_above_5", "pop_above_15", "pop_above_25"], colors):
        ax1.plot(df1["_id"], df1[col], marker='o', label=col, color=color)

    ax1.set_xlabel("State")
    ax1.set_ylabel("Population")
    ax1.set_title("Population by Age Group")
    ax1.legend()
    ax1.set_xticks(range(len(df1)))
    ax1.set_xticklabels(df1["_id"], rotation=90)
    ax1.grid(alpha=0.15) 
    
    img_byte_arr1 = BytesIO()
    print('Saving Figure 1')
    fig1.savefig(img_byte_arr1, format='png')
    img_byte_arr1.seek(0)

    upload_to_gcs(
        img_byte_arr1.getvalue(),
        'image/png',
        'msds-697-final-project-group-13',
        'results/population_by_age.png'
    )

    # Plot 2
    print('Generating Plot 2/3')
    input2 = 'avg_household'
    collection2 = db[input2]

    df2 = pd.DataFrame(list(collection2.find())).dropna()
    fig2, ax2 = plt.subplots(figsize=(10, 5))
    ax2.bar(df2["_id"], df2["avg_households"], color='skyblue')    
    ax2.set_xlabel("State")
    ax2.set_ylabel("Average Households")
    ax2.set_title("Average Number of Households by State")
    ax2.set_xticks(range(len(df2)))
    ax2.set_xticklabels(df2["_id"], rotation=90)
    ax2.grid(alpha=0.15)

    img_byte_arr2 = BytesIO()
    print('Saving Figure 2')
    fig2.savefig(img_byte_arr2, format='png')
    img_byte_arr2.seek(0)

    upload_to_gcs(
        img_byte_arr2.getvalue(),
        'image/png',
        'msds-697-final-project-group-13',
        'results/average_household_by_state.png'
    )

    # Plot 3
    print('Generating Plot 3/3')
    input3 = 'highest_pop'
    input4 = 'pop_above_18'
    input5 = 'unemp_pop'

    collection3 = db[input3]
    collection4 = db[input4]
    collection5 = db[input5]

    df3 = pd.DataFrame(list(collection3.find()))
    df4 = pd.DataFrame(list(collection4.find()))
    df5 = pd.DataFrame(list(collection5.find()))

    fig3, ax3 = plt.subplots(3, figsize=(10,15))
    fig3.subplots_adjust(hspace=0.4, wspace=0.3)

    ax3[0].bar(df3["_id"], df3["total_population"], color='purple')
    ax3[0].set_xlabel("County")
    ax3[0].set_ylabel("Total Population")
    ax3[0].set_title("Total Population By County")
    ax3[0].tick_params(axis='x', labelsize=7, rotation=20)
    ax3[0].grid(alpha=0.15)

    ax3[1].bar(df4["_id"], df4["total_population_above_18"], color='mediumseagreen')
    ax3[1].set_xlabel("County")
    ax3[1].set_ylabel("Total Population Above Age 18")
    ax3[1].set_title("Top 10 Counties by Total Population Above Age 18")
    ax3[1].tick_params(axis='x', labelsize=7, rotation=20)
    ax3[1].grid(alpha=0.15)

    ax3[2].bar(df5["_id"], df5["unemployed"], color='salmon')
    ax3[2].set_xlabel("State")
    ax3[2].set_ylabel("Unemployed Population")
    ax3[2].set_title("Unemployed Population by State")
    ax3[2].tick_params(axis='x', labelsize=7, rotation=20)
    ax3[2].grid(alpha=0.15)
    
    img_byte_arr3 = BytesIO()
    print('Saving Figure 3')
    fig3.savefig(img_byte_arr3, format='png')
    img_byte_arr3.seek(0)

    upload_to_gcs(
        img_byte_arr3.getvalue(),
        'image/png',
        'msds-697-final-project-group-13',
        'results/general_analysis.png'
    )
    client.close()

# Define the DAG
with DAG(
    dag_id="census_analysis",
    default_args=default_args,
    description="ETL pipeline for SDH data",
    schedule_interval="@once", # "0 0 1 2 *" for generalized schedule
    catchup=False) as dag:

    # Task 1
    load_and_clean_task = PythonOperator(
        task_id="load_and_clean_data",
        python_callable=load_and_clean_data
    )

    # Task 2
    aggregate_task = PythonOperator(
        task_id="aggregate_and_store",
        python_callable=aggregate_and_store
    )

    # Task 3
    analysis_task = PythonOperator(
        task_id="get_analysis",
        python_callable=get_analysis
    )
    
    load_and_clean_task >> aggregate_task >> analysis_task

