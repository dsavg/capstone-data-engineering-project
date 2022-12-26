"""Redshift operator is the PostgresOperator with params and sql templatable."""
from airflow.operators.postgres_operator import PostgresOperator

class RedshiftOperator(PostgresOperator):
    """Inherit PostgresOperator and add templated fields."""
    template_fields = ('sql','params')
