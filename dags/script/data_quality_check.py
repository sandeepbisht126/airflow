import psycopg2
from psycopg2 import sql

from script.utils import CheckType


class DataQualityChecks:
    def __init__(self, db_config):
        """
        Initialize the DataQualityChecks class with database configuration.

        :param db_config: Dictionary with database connection details.
        """
        self._params = None
        self.db_config = db_config

    def get_db_connection(self):
        """Establish and return a database connection."""
        return psycopg2.connect(
            host=self.db_config["host"],
            database=self.db_config["database"],
            user=self.db_config["user"],
            password=self.db_config["password"],
        )

    def execute_query(self, query, params):
        """Execute a query with the provided parameters and return the results."""
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()

    @staticmethod
    def check_threshold(count, threshold, criticality, dq_type):
        """
        Check if a value breaches the threshold and raise an exception if critical.

        :param count: The count value to check.
        :param threshold: Threshold dictionary with "upper" and "lower" limits.
        :param criticality: Criticality of the check (True for critical, False for non-critical).
        :param dq_type: The type of check (e.g., RowCount or Duplicate).
        """
        threshold = {
            "lower": threshold.get("lower", 0),            # Default lower to 0
            "upper": threshold.get("upper", float('inf'))  # Default upper to infinite
        }

        if count < threshold["lower"] or count > threshold["upper"]:
            message = f"{dq_type.value} check failed. Count: {count}, Threshold: {threshold}, Criticality: {criticality}"
            if criticality:
                raise Exception(message)
            else:
                print(f"Warning: {message} (Non-critical)")

    @staticmethod
    def construct_where_clause(key_by_val):
        """
        Constructs the WHERE clause dynamically based on `key_by_val`.

        :param key_by_val: A dictionary of key-value pairs where the key is a column
                            and the value can be a single value or a range.
        :return: A tuple containing the WHERE clause as a string and a list of parameters.
        """
        where_conditions = []
        params = []

        for column, value in key_by_val.items():
            if isinstance(value, dict):
                # Handle range (e.g., inserted_dt >= from_param and inserted_dt < to_param)
                for condition, range_value in value.items():
                    condition_map = {
                        "gte": ">=",
                        "lt": "<",
                        "lte": "<=",
                    }
                    if condition in condition_map:
                        where_conditions.append(f"{column} {condition_map[condition]} %s")
                        params.append(range_value)
            else:
                # Handle equality (e.g., inserted_dt = value)
                where_conditions.append(f"{column} = %s")
                params.append(value)

        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        return where_clause, params

    def perform_row_count_check(self, dq_params: dict):
        """
        Perform a row count check and validate against thresholds.

        :param dq_params: Dictionary containing parameters:
                             - tableName (mandatory)
                             - key_by_val (filters)
                             - threshold (upper and lower bounds)
                             - criticality (True for critical, False for non-critical)
        """
        table_name = dq_params.get("tableName")
        if not table_name:
            raise ValueError("The 'tableName' parameter is mandatory for RowCountCheck.")

        key_by_val = dq_params.get("key_by_val", {})
        threshold = dq_params.get("threshold", {"lower": 0, "upper": float('inf')})
        criticality = dq_params.get("criticality", True)

        where_clause, params = self.construct_where_clause(key_by_val)
        query = sql.SQL("""
            SELECT COUNT(1)
            FROM {table}
            WHERE {where_clause}
        """).format(
            table=sql.Identifier(table_name), where_clause=sql.SQL(where_clause)
        )

        result = self.execute_query(query, params)
        count = result[0][0] if result else 0
        self.check_threshold(count, threshold, criticality, CheckType.ROW_COUNT)

    def perform_duplicate_check(self, dq_params: dict):
        """
        Perform a duplicate check and validate against thresholds.

        :param dq_params: Dictionary containing parameters:
                             - tableName (mandatory)
                             - key_by_val (filters)
                             - key_col (mandatory)
                             - threshold (upper and lower bounds)
                             - criticality (True for critical, False for non-critical)
        """
        table_name = dq_params.get("tableName")
        key_col = dq_params.get("key_col")
        if not table_name:
            raise ValueError("The 'tableName' parameter is mandatory for DuplicateCheck.")
        if not key_col:
            raise ValueError("The 'key_col' parameter is mandatory for DuplicateCheck.")

        key_by_val = dq_params.get("key_by_val", {})
        threshold = dq_params.get("threshold", {"lower": 0, "upper": float('inf')})
        criticality = dq_params.get("criticality", True)

        where_clause, params = self.construct_where_clause(key_by_val)
        query = sql.SQL("""
            SELECT COUNT(1)
            FROM (
                SELECT {key_col}, COUNT(1) as duplicate_count
                FROM {table}
                WHERE {where_clause}
                GROUP BY {key_col}
                HAVING COUNT(1) > 1
            ) as duplicate_records
        """).format(
            key_col=sql.Identifier(key_col),
            table=sql.Identifier(table_name),
            where_clause=sql.SQL(where_clause)
        )

        result = self.execute_query(query, params)
        count = result[0][0] if result else 0
        self.check_threshold(count, threshold, criticality, CheckType.DUPLICATE)

    def run_row_count_check(self):
        """Handle RowCountCheck logic."""
        self.perform_row_count_check(self._params["RcountCheck"])

    def run_duplicate_check(self):
        """Handle DuplicateCheck logic."""
        self.perform_duplicate_check(self._params["DuplicateCheck"])

    def run_checks(self, params):
        """
        Run all configured data quality checks.

        :param params: Dictionary containing check configurations.
        """
        self._params = params
        all_checks_passed = True  # Track overall result
        messages = []             # Track non-critical warnings

        if "RcountCheck" in self._params:
            try:
                self.run_row_count_check()
            except Exception as e:
                all_checks_passed = self.handle_exception(e, self._params["RcountCheck"])

        if "DuplicateCheck" in self._params:
            try:
                self.run_duplicate_check()
            except Exception as e:
                all_checks_passed = self.handle_exception(e, self._params["DuplicateCheck"])

        if not all_checks_passed:
            print("Non-critical issues found:", messages)
        else:
            print("All data quality checks passed.")

    def handle_exception(self, exception, check_params):
        """Handle exception based on the check's criticality."""
        if check_params.get("criticality", True):
            raise exception
        else:
            print(f"Non-critical issue: {str(exception)}")
            return False

'''

# Example usage
db_config = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "postgres",
    "port": 5432
}

params = {
    "RcountCheck": {
        "tableName": "session_tgt",
        "key_by_val": {"inserted_dt": {"gte": "2025-01-01", "lt": "2025-02-01"}, "data": "data1"},
        "threshold": {"lower": 1},
        "criticality": True
    },
    "DuplicateCheck": {
        "tableName": "session_tgt",
        "key_by_val": {"inserted_dt": {"gte": "2025-01-01", "lt": "2025-02-01"}},
        "key_col": "data",
        "threshold": {"upper": 0},
        "criticality": True
    }
}

# Initialize the class and run checks
data_quality = DataQualityChecks(db_config)
data_quality.run_checks(params)
'''