import psycopg2
from psycopg2 import sql


class DataQualityChecks:
    def __init__(self, db_config, params):
        """
        Initialize the DataQualityChecks class with database configuration.

        :param db_config: Dictionary with database connection details.
        :param params: Dictionary with check parameters.
        """
        self._db_config = db_config
        self._metric_name = None
        self._table_name = None
        self._key_by_val = None
        self._key_cols = None
        self._threshold = None
        self._criticality = None

        self.metric_name = params.get("metric_name")
        self.table_name = params.get("table_name")
        self.key_by_val = params.get("key_by_val", {})
        self.key_cols = params.get("key_cols")
        self.threshold = {
            "lower": params.get("threshold", {}).get("lower", 0),            # Default lower to 0
            "upper": params.get("threshold", {}).get("upper", float('inf'))  # Default upper to infinite
        }
        self.criticality = params.get("criticality", True)

    @property
    def metric_name(self):
        return self._metric_name

    @metric_name.setter
    def metric_name(self, value):
        if not value:
            raise ValueError("metric_name cannot be empty")
        self._metric_name = value

    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, value):
        if not value:
            raise ValueError("tableName cannot be empty")
        self._table_name = value

    @property
    def key_by_val(self):
        return self._key_by_val

    @key_by_val.setter
    def key_by_val(self, value):
        if not isinstance(value, dict):
            raise ValueError("key_by_val must be a dictionary")
        self._key_by_val = value

    @property
    def key_cols(self):
        return self._key_cols

    @key_cols.setter
    def key_cols(self, value):
        if self._metric_name == "RCountCheck":
            self._key_cols = []
            return
        if not isinstance(value, list):
            raise ValueError("key_cols must be a list")
        if not value and self._metric_name == "DuplicateCheck":
            raise ValueError("key_cols is must for DuplicateCheck")

        self._key_cols = value

    @property
    def threshold(self):
        return self._threshold

    @threshold.setter
    def threshold(self, value):
        lower = value.get("lower", 0)
        upper = value.get("upper", float('inf'))
        self._threshold = {"lower": lower, "upper": upper}

    @property
    def criticality(self):
        return self._criticality

    @criticality.setter
    def criticality(self, value):
        if not isinstance(value, bool):
            raise ValueError("criticality must be a boolean value")
        self._criticality = value

    def get_db_connection(self):
        """Establish and return a database connection."""
        return psycopg2.connect(
            host=self._db_config["host"],
            database=self._db_config["database"],
            user=self._db_config["user"],
            password=self._db_config["password"],
        )

    def execute_query(self, query, params):
        """Execute a query with the provided parameters and return the results."""
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()

    def check_threshold(self, count):
        """Check if the count breaches the threshold and raise exception if critical."""
        if count < self.threshold["lower"] or count > self.threshold["upper"]:
            message = f"{self.metric_name} check failed. Count: {count}, Threshold: {self.threshold}, Criticality: {self.criticality}"
            raise Exception(message)
        else:
            print(f"{self.metric_name} check successful!!!")

    def construct_where_clause(self):
        """
        Constructs the WHERE clause dynamically based on `key_by_val`.
        :return: A tuple containing the WHERE clause as a string and a list of parameters.
        """
        where_conditions = []
        params = []
        for column, value in self._key_by_val.items():
            if isinstance(value, dict):
                # Handle range conditions
                for condition, range_value in value.items():
                    condition_map = {"gte": ">=", "lt": "<", "lte": "<="}
                    if condition in condition_map:
                        where_conditions.append(f"{column} {condition_map[condition]} %s")
                        params.append(range_value)
            else:
                # Handle equality conditions
                where_conditions.append(f"{column} = %s")
                params.append(value)

        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        return where_clause, params

    def perform_row_count_check(self):
        """Perform a row count check and validate against thresholds."""

        print(f"DQ check running for {self.metric_name}...")
        if not self.table_name:
            raise ValueError("The 'tableName' parameter is mandatory for RowCountCheck.")

        where_clause, params = self.construct_where_clause()
        query = sql.SQL("""
            SELECT COUNT(1)
            FROM {table}
            WHERE {where_clause}
        """).format(
            table=sql.Identifier(self.table_name), where_clause=sql.SQL(where_clause)
        )

        result = self.execute_query(query, params)
        count = result[0][0] if result else 0
        self.check_threshold(count)

    def perform_duplicate_check(self):
        """Perform a duplicate check and validate against thresholds."""

        print(f"DQ check running for {self.metric_name}...")
        if not self.table_name or not self.key_cols:
            raise ValueError("The 'tableName' and 'key_cols' parameters are mandatory for DuplicateCheck.")

        key_cols_str = ",".join(self.key_cols)
        where_clause, params = self.construct_where_clause()
        query = sql.SQL("""
            SELECT COUNT(1)
            FROM (
                SELECT {key_cols_str}, COUNT(1) as duplicate_count
                FROM {table}
                WHERE {where_clause}
                GROUP BY {key_cols_str}
                HAVING COUNT(1) > 1
            ) as duplicate_records
        """).format(
            key_cols_str=sql.Identifier(key_cols_str),
            table=sql.Identifier(self.table_name),
            where_clause=sql.SQL(where_clause)
        )

        result = self.execute_query(query, params)
        count = result[0][0] if result else 0
        self.check_threshold(count)

    def run_checks(self):
        """Run the appropriate check based on the metric_name."""
        try:
            if self.metric_name == "RCountCheck":
                self.perform_row_count_check()
            elif self.metric_name == "DuplicateCheck":
                self.perform_duplicate_check()
            else:
                raise ValueError(f"metric not provided aor not supported: {self.metric_name}")
        except Exception as e:
            self.handle_exception(e)

    def handle_exception(self, exception):
        """Handle exception based on the check's criticality."""
        if self._criticality:
            raise exception
        else:
            print(f"Non-critical issue: {str(exception)}")

'''
# Example usage
DB_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "postgres",
    "port": 5432
}

params_list = [
    {
        "metric_name": "RCountCheck",
        "table_name": "session_tgt",
        "key_by_val": {"inserted_dt": {"gte": "2025-01-01", "lt": "2025-02-01"}, "data": "data1"},
        "threshold": {"lower": 1},
        "criticality": True,
    },
    {
        "metric_name": "DuplicateCheck",
        "table_name": "session_tgt",
        "key_by_val": {"inserted_dt": {"gte": "2025-01-01", "lt": "2025-02-01"}},
        "key_cols": ["data"],
        "threshold": {"upper": 0},
        "criticality": True,
    }
]

if __name__ == "__main__":
    for params in params_list:
        dq = DataQualityChecks(DB_CONFIG, params)
        dq.run_checks()

'''