from airflow.models import BaseOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults


class PostgresInsertOperator(BaseOperator):
    template_fields = ["_source_query"]

    @apply_defaults
    def __init__(
        self,
        target: str,
        source: str = None,
        source_query: str = None,
        key_cols: list = None,  # not used in INSERT
        custom_filter: str = None,
        operation_type: str = "INSERT",
        params: dict = None,
        postgres_conn_id: str = "postgres_conn_id",
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
            insert_sql = self._source_query
        else:
            insert_sql = f"""
                INSERT INTO {self.target} ({col_list})
                SELECT {col_list} FROM {self.source} {self._custom_filter}
        """

        # insert_sql = f"""
        #     INSERT INTO {self.target}
        #     SELECT * FROM {self.source}
        # """

        self.log.info(f"Running INSERT SQL:\n{insert_sql}")

        # Run using PostgresOperator
        op = PostgresOperator(
            task_id=f"{self.task_id}_run_insert",
            sql=insert_sql,
            postgres_conn_id=self.postgres_conn_id
        )
        return op.execute(context)

    def _parse_table(self, full_table_name):
        """Parses schema.table or just table"""
        parts = full_table_name.split('.')
        if len(parts) == 2:
            return parts[0], parts[1]
        elif len(parts) == 1:
            return 'public', parts[0]
        else:
            raise ValueError(f"Invalid table format: {full_table_name}")
