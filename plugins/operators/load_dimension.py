from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 table="",
                 append_only="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.append_only = append_only


    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)   

        try:
            if not self.append_only:
                self.log.info("Delete {} dimension table".format(self.table))
                redshift.run("DELETE FROM {}".format(self.table))  
                
            self.log.info("Inserting data into {} dimension table".format(self.table))
            insert_statement = "INSERT INTO {} \n{}".format(self.table, self.sql)
            self.log.info("Running SQL: \n{}".format(insert_statement))
            redshift.run(insert_statement)
            self.log.info("Data successfully inserted into {} dimension table".format(self.table))
            
        except Exception as e:
            self.log.error("An error occurred while loading data into {} dimension table: {}".format(self.table, str(e)))
            raise