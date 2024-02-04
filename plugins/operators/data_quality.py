from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)  
        self.log.info('Data Quality Starting...')
        for table in self.tables:
            try:
                self.log.info('Checking data quality for table: {}'.format(table))
                records = redshift.get_records("SELECT COUNT(*) FROM {}".format(table))
                num_records = records[0][0]
                if len(records) < 1 or len(records[0]) <1 or records[0][0] == 0:
                    self.log.error("Data quality check failed: {} has no value".format(table))
                self.log.info('Data quality check passed: {} records found in table {}'.format(num_records,table))
            except Exception as e:
                self.log.error('Data quality check failed: Table {}, Error: {}'.format(table, str(e)))
                raise
        self.log.info('Data Quality checks completed successfully.')



