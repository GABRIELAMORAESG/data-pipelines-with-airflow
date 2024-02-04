from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateSchemaOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 skip=False,
                 *args, **kwargs):        

        super(CreateSchemaOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.skip = skip
        
    def execute(self, context):
        if not self.skip:
            self.log.info("Creating public schema")        
            redshift = PostgresHook(self.redshift_conn_id)  
            redshift.run(self.sql)
            self.log.info("public schema created successfully")
        else:
            self.log.info("Skipping creation of public schema")
