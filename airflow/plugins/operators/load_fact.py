from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    insert_sql="""
        INSERT INTO {}
        {};
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 source="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.source = source

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id) 
        
        
        formatted_sql=LoadFactOperator.insert_sql.format(
            self.table,
            self.source
        )
        
        self.log.info(f"Executing {formatted_sql}")
        redshift.run(formatted_sql)

