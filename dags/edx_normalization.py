from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.operators.bigquery import (

    BigQueryExecuteQueryOperator,
    BigQueryDeleteTableOperator,
)

default_dag_args = {
    "start_date": datetime.today(),
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    "email_on_failure": False,
    "email_on_retry": False,
}

conn = 'airflow_test'

with DAG(
    "edx_normalization_dag",
    # Continue to run DAG once per day
    schedule_interval= '@once',
    default_args=default_dag_args,
)as dag:
    create_information_table = BigQueryExecuteQueryOperator(
        task_id="create_information_table",
        sql= "sql_scripts/split_course_tuple.sql",
        use_legacy_sql=False,
        gcp_conn_id=conn,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
    )
    create_course_details_table = BigQueryExecuteQueryOperator(
        task_id="create_course_details_table",
        sql="sql_scripts/create_table_course_details.sql",
        use_legacy_sql=False,
        gcp_conn_id=conn,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
    )
    create_user_details_table = BigQueryExecuteQueryOperator(
        task_id="create_user_details_table",
        sql="sql_scripts/create_table_user_details.sql",
        use_legacy_sql=False,
        gcp_conn_id=conn,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
    )
    create_user_courses_table = BigQueryExecuteQueryOperator(
        task_id="create_user_courses_table",
        sql="sql_scripts/create_table_user_courses.sql",
        use_legacy_sql=False,
        gcp_conn_id=conn,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
    )
    delete_information_temp_table = BigQueryDeleteTableOperator(
        task_id="delete_table",
        deletion_dataset_table="data-eng1.edx_enrollments.Information_temp",
        gcp_conn_id=conn,
        ignore_if_missing=True,
    )


    create_information_table >> create_course_details_table  >> create_user_details_table >> create_user_courses_table >> delete_information_temp_table