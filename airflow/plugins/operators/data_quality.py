from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.contrib.hooks.aws_hook import AwsHook

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 tables=[],
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.tables=tables
        self.checks=checks

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        for table in self.tables:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            
            
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")

        
        for i, check in enumerate(self.checks):
            records = redshift.get_records(check['test_sql'])
            if not check['expected_result'] == records[0][0]:
                raise ValueError(f"Data quality check #{i} failed. the table contains nulls in thier primary key ")
            
            self.log.info(f"Data quality check #{i} passed. the table contains 0 nulls in thier primary key")
