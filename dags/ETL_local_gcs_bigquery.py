from airflow import DAG
from datetime import datetime
# from airflow.models.baseoperator import chain
# from airflow.operators.dummy_operator import DummyOperator
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
bq_dataset = "cardio_data_upload"
bq_table = "cardio"
gcp_bucket = "mj-dataset-bucket"
gcp_data_dst = "cardio_data.csv"
conn = 'airflow_test'

with DAG(
    "ETL_upload_to_BigQuery",
    # Continue to run DAG once per day
    schedule_interval= '@once',
    default_args=default_dag_args,
)as dag:
    """
        #### BigQuery dataset creation
        Create the dataset to store the sample data tables.
        """
    upload_dataset_gcs = LocalFilesystemToGCSOperator(
        task_id='file_to_gcs',
        src='/opt/airflow/dags/repo/datasets/cardio_train.csv',
        dst= gcp_data_dst,
        bucket= gcp_bucket,
        gcp_conn_id=conn,
    )
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=bq_dataset, gcp_conn_id = conn,
    )

    """
    #### Transfer data from GCS to BigQuery
    Moves the data uploaded to GCS in the previous step to BigQuery
    """
    transfer_cardio_data = GCSToBigQueryOperator(
        task_id="cardio_data_gcs_to_bigquery",
        bucket=gcp_bucket,
        source_objects=[gcp_data_dst],
        skip_leading_rows=1,
        destination_project_dataset_table="{}.{}".format(bq_dataset, bq_table),
        gcp_conn_id=conn,
        field_delimiter = ';',
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "age", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "gender", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "height", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "weight", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "ap_hi", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "ap_lo", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "cholesterol", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "gluc", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "smoke", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "alco", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "active", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "cardio", "type": "INTEGER", "mode": "NULLABLE"},
        ],
        source_format="CSV",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        allow_jagged_rows=True,
    )


upload_dataset_gcs >> create_dataset >> transfer_cardio_data