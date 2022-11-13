from airflow import DAG
from datetime import datetime
import pandas as pd
import numpy as np
from airflow.operators.python import PythonOperator
# from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
# from airflow.providers.google.cloud.operators.gcs import (
#     GCSFileTransformOperator,
# )

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

default_dag_args = {
    "start_date": datetime.today(),
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    "email_on_failure": False,
    "email_on_retry": False,
}
bq_dataset = "edx_enrollments"
bq_table = "Information_temp"
gcp_bucket = "mj-dataset-bucket"
gcs_data_dst = "edx_data.csv"
conn = 'airflow_test'


def transform_data():
    df = pd.read_csv("/opt/airflow/dags/repo/datasets/edx_enrollments.csv", header=0, sep=',')
    df = df.replace('\\N', "")
    df.to_csv("/tmp/edx_data.csv", index=False)


with DAG(
        "edx_ETL_DAG",
        # Continue to run DAG once per day
        schedule_interval='@once',
        default_args=default_dag_args,
) as dag:
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        # op_kwargs={'gcs_temp_dst': gcs_temp_dst,
        #            'gcs_data_dst': gcs_data_dst},
    )

    upload_dataset_gcs = LocalFilesystemToGCSOperator(
        task_id='file_to_gcs',
        src='/tmp/edx_data.csv',
        dst=gcs_data_dst,
        bucket=gcp_bucket,
        gcp_conn_id=conn,
    )
    """
        BigQuery dataset creation
        Create the dataset to store the sample data tables.
    
    """
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=bq_dataset, gcp_conn_id=conn,
    )
    """    
        #### Create Table for edx_data in BigQuery
        #### Transfer data from GCS to BigQuery
        Moves the data uploaded to GCS in the previous step to BigQuery
    
    """
    transfer_edx_data = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket=gcp_bucket,
        source_objects=[gcs_data_dst],
        skip_leading_rows=1,
        destination_project_dataset_table="{}.{}".format(bq_dataset, bq_table),
        gcp_conn_id=conn,
        field_delimiter=',',
        schema_fields=[
            {"name": "Course_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "Course_Short_Title", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Course_Long_Title", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Userid_DI", "type": "STRING", "mode": "REQUIRED"},
            {"name": "Registered", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "Viewed", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "Explored", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "Certified", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "Country", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LoE_DI", "type": "STRING", "mode": "NULLABLE"},
            {"name": "YoB", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "Age", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "Gender", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Grade", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "nevents", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ndays_act", "type": "STRING", "mode": "NULLABLE"},
            {"name": "nplay_video", "type": "STRING", "mode": "NULLABLE"},
            {"name": "nchapters", "type": "STRING", "mode": "NULLABLE"},
            {"name": "nforum_posts", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "roles", "type": "STRING", "mode": "NULLABLE"},
            {"name": "incomplete_flag", "type": "STRING", "mode": "NULLABLE"},
        ],
        source_format="CSV",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        allow_jagged_rows=True,
    )

    transform_data >> create_dataset >> upload_dataset_gcs >> transfer_edx_data
