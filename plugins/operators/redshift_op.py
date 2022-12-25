"""Redshift operator is the PostgresOperator with params and query templatable."""
from airflow.operators.postgres_operator import PostgresOperator

class RedshiftOperator(PostgresOperator):
    template_fields = ('sql','params')
