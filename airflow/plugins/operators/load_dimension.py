from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.contrib.hooks.aws_hook import AwsHook

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 SQL_statement="",
                 append_only="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.SQL_statement=SQL_statement
        self.append_only=append_only

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        if not self.append_only:
            self.log.info("Delete {} dimension table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table)) 
        self.log.info("Insert data from staging table into {} dimension table".format(self.table))
        redshift.run("INSERT INTO {} {}".format(self.table,self.SQL_statement))
        self.log.info(' NOTE : LoadDimensionOperator RAN SUCCESSFULLY')
