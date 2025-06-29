import time
from datetime import timedelta
from airflow.models import BaseOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.operators.python import PythonOperator


class PostgresInsertOperator(BaseOperator):
    template_fields = ["_source_query", "_filter_clause"]

    @apply_defaults
    def __init__(
        self,
        target: str,
        source: str = None,
        source_query: str = None,
        key_cols: list = None,  # not used in INSERT
        custom_filter: str = None,
        operation_type: str = "INSERT",
        delete_by_key_val: dict = None,
        params: dict = None,
        postgres_conn_id: str = "postgres_conn_id",
        # execution_timeout: int = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.source = source
        self._source_query = source_query
        self.target = target
        self.key_cols = key_cols or []
        self._custom_filter = custom_filter
        self._params = params
        self.operation_type = operation_type.upper()
        self.postgres_conn_id = postgres_conn_id
        self._delete_by_key_val = delete_by_key_val
        self._filter_clause = self._get_filter_clause()
        # self._execution_timeout = execution_timeout

    def _get_filter_clause(self) -> str:
        delete_condition = " AND ".join([f"{col} = {val}" for col, val in self._delete_by_key_val.items()]) if self._delete_by_key_val else ""

        filter_parts = []
        if self._custom_filter and self._custom_filter.strip():
            filter_parts.append(self._custom_filter.strip())
        if delete_condition:
            filter_parts.append(delete_condition)

        self._filter_clause = "WHERE " + " AND ".join(filter_parts) if filter_parts else ""

        return self._filter_clause

        # if self._delete_by_key_val:
        #     self._custom_filter = "where " + " AND ".join([f"{col} = {val}" for col, val in self._delete_by_key_val.items()])

    # @property
    # def filter_clause(self):
    #     return self._custom_filter
    #
    # @filter_clause.setter
    # def filter_clause(self, val):
    #     if self._delete_by_key_val:
    #         self._custom_filter = "where " + " AND ".join([f"{col} = {val}" for col, val in self._delete_by_key_val.items()])
    #     else:
    #         self._custom_filter = ""

    def execute(self, context):
        if self.operation_type != "INSERT":
            raise ValueError("Only INSERT operation is supported in this operator")

        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Extract schema and table
        target_schema, target_table = self._parse_table(self.target)

        # Get target table columns
        cursor.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (target_schema, target_table))

        columns = [row[0] for row in cursor.fetchall()]
        if not columns:
            raise ValueError(f"No columns found for table {self.target}")

        # Generate SQL
        col_list = ', '.join(columns)
        if self._source_query:
            insert_sql = f"{self._source_query} {self._filter_clause}"
        else:
            insert_sql = f"""
                INSERT INTO {self.target} ({col_list})
                SELECT {col_list} FROM {self.source} {self._filter_clause}
        """

        # insert_sql = f"""
        #     INSERT INTO {self.target}
        #     SELECT * FROM {self.source}
        # """

        self.log.info(f"Running INSERT SQL:\n{insert_sql}")

        # op = PostgresOperator(
        #     task_id=f"{self.task_id}_run_insert",
        #     sql=insert_sql,
        #     postgres_conn_id=self.postgres_conn_id,
        #     execution_timeout=self._execution_timeout
        # )
        # return op.execute(context)

        time.sleep(10)

        rows_affected = pg_hook.run(sql=insert_sql, parameters=self._params, autocommit=True)
        self.log.info(f"INSERT operation to {self.target} completed successfully. Rows affected: {rows_affected}")

        return rows_affected

    def _parse_table(self, full_table_name):
        """Parses schema.table or just table"""
        parts = full_table_name.split('.')
        if len(parts) == 2:
            return parts[0], parts[1]
        elif len(parts) == 1:
            return 'public', parts[0]
        else:
            raise ValueError(f"Invalid table format: {full_table_name}")
