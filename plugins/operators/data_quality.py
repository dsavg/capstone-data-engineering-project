"""
Data Quality Operator.
The operator's main functionality is to receive one or more SQL
based test cases along with the expected results and execute the tests.
For each, the test result and expected result will to be checked and if
there is no match, the operator will raise an exception and the task will
retry and fail eventually.
"""

from typing import Dict, List

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# TO DO: currently the query_check_dict is a dict with key string and value list
# should change to list of dictionaries like
# https://airflow.apache.org/docs/apache-airflow/1.10.1/_modules/airflow/contrib/operators/emr_add_steps_operator.html


class DataQualityOperator(BaseOperator):
    """
    Executes SQL queries for data quality checks.
    :param redshift_conn_id: (Optional) Amazon Redshift connection id
    :param query_checks: (Optional) list of dictionaries containing with keys
        - 'query': query to check
        - 'operation': mathematical operation to check (=,<,>)
        - 'value': value to check the query results against
        data check queries and expected results
    """

    ui_color = '#59D8DA'
    template_fields = ("query_checks",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 query_checks: List[Dict] = None,
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query_checks = query_checks

    def execute(self, context) -> None:
        """Run data quality check."""
        redshift = PostgresHook(self.redshift_conn_id)
        if self.query_checks is not None:
            for qc in self.query_checks:
                query = qc['query']
                operator = qc['operation']
                v = qc['value']
                # execute query
                records = redshift.get_records(qc['query'])
                num_records = records[0][0]
                if operator == '=' and num_records != v:
                    raise ValueError(f"Data quality check failed! "
                                     f"{query} returned {num_records}, "
                                     f"expected {v}")
                elif operator == '>' and num_records <= v:
                    raise ValueError(f"Data quality check failed! "
                                     f"{query} returned {num_records}, "
                                     f"expected greater than {v}")
                elif operator == '<' and num_records >= v:
                    raise ValueError(f"Data quality check failed! "
                                     f"{query} returned {num_records}, "
                                     f"expected less than {v}")
                self.log.info(f"Data quality check {query} passed with "
                              f"{num_records} records")