"""Operator to check if field, usually date, exists in table.

The class inherits the BaseOperator.
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class RedshiftTableCheck(BaseOperator):
    """Airflow operator to check if date partition exists in a Redshift Table."""

    ui_color = '#DA5984'
    template_fields = ("ds",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 schema: str = "",
                 table: str = "",
                 dt_field: str = "snapshot_date",
                 *args, **kwargs) -> None:
        """
        Class initialization

        :param redshift_conn_id: (Optional) Amazon Redshift connection id
        :param schema: (Optional) Schema name
        :param table: (Optional) Table name
        :param dt_field: (Optional) Field to check
        """
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.schema = schema
        self.table = table
        self.dt_field = dt_field
        self.ds = kwargs['params']['end_date']

    def execute(self, context) -> None:
        """Check if a specific date field exists in table."""
        redshift = PostgresHook(self.redshift_conn_id)
        # execute query
        query = f"SELECT True " \
                f"FROM {self.schema}.{self.table} " \
                f"WHERE {self.dt_field}='{self.ds}' " \
                f"LIMIT 1"
        self.log.info(query)
        records = redshift.get_records(query)
        result = records[0][0]
        if not result:
            raise ValueError(f"Date check failed: '{self.ds}' not "
                             f"found in {self.schema}.{self.table}")
        self.log.info(f"Date check passed for '{self.ds}'")
