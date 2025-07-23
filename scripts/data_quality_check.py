import psycopg2
from psycopg2 import sql


class DataQualityChecks:
    def __init__(self, db_config):
        """
        Initialize the DataQualityChecks class with database configuration.

        :param db_config: Dictionary with database connection details.
        """
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
        conn = None
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute(query, params)
            return cursor.fetchall()
        finally:
            if conn:
                conn.close()

    def check_threshold(self, count, threshold, criticality, check_type):
        """
        Check if a value breaches the threshold and raise an exception if critical.

        :param count: The count value to check.
        :param threshold: Threshold dictionary with "upper" and "lower" limits.
        :param criticality: Criticality of the check (True for critical, False for non-critical).
        :param check_type: The type of check (e.g., RowCount or Duplicate).
        """
        # Default thresholds
        threshold.setdefault("lower", 0)
        threshold.setdefault("upper", 0)

        if count < threshold["lower"] or count > threshold["upper"]:
            message = (
                f"{check_type} check failed. Count: {count}, "
                f"Threshold: {threshold}, Criticality: {criticality}"
            )
            if criticality:
                raise Exception(message)
            else:
                print(f"Warning: {message} (Non-critical)")

    def perform_row_count_check(self, table_name, key_by_val, threshold, criticality):
        """
        Perform a row count check and validate against thresholds.

        :param table_name: Name of the table to query.
        :param key_by_val: Filters for the query (e.g., dt and country).
        :param threshold: Threshold for row count (upper and lower).
        :param criticality: Criticality of the check (True for critical, False for non-critical).
        """
        query = sql.SQL("""
            SELECT COUNT(1)
            FROM {table}
            WHERE dt = %s AND country = %s
        """).format(table=sql.Identifier(table_name))

        result = self.execute_query(query, (key_by_val["dt"], key_by_val["country"]))
        count = result[0][0]

        # Check threshold
        self.check_threshold(count, threshold, criticality, "RowCount")

    def perform_duplicate_check(self, table_name, key_by_val, key_col, threshold, criticality):
        """
        Perform a duplicate check and validate against thresholds.

        :param table_name: Name of the table to query.
        :param key_by_val: Filters for the query (e.g., dt and country).
        :param key_col: Column to check for duplicates.
        :param threshold: Threshold for duplicate count (upper and lower).
        :param criticality: Criticality of the check (True for critical, False for non-critical).
        """
        query = sql.SQL("""
            SELECT COUNT(1)
            FROM (
                SELECT {key_col}, COUNT(1) as duplicate_count
                FROM {table}
                WHERE dt = %s AND country = %s
                GROUP BY {key_col}
                HAVING COUNT(1) > 1
            ) as duplicate_records
        """).format(
            key_col=sql.Identifier(key_col),
            table=sql.Identifier(table_name)
        )

        result = self.execute_query(query, (key_by_val["dt"], key_by_val["country"]))
        count = result[0][0] if result else 0

        # Check threshold
        self.check_threshold(count, threshold, criticality, "Duplicate")

    def run_checks(self, params):
        """
        Run all configured data quality checks.

        :param params: Dictionary containing check configurations.
        """
        all_checks_passed = True  # Track overall result
        messages = []  # Track non-critical warnings

        if "RcountCheck" in params:
            rcount = params["RcountCheck"]
            try:
                self.perform_row_count_check(
                    rcount["tableName"],
                    rcount["key_by_val"],
                    rcount.get("threshold", {"upper": 0, "lower": 0}),
                    rcount.get("criticality", True),
                )
            except Exception as e:
                if rcount.get("criticality", True):
                    raise
                else:
                    all_checks_passed = False
                    messages.append(str(e))

        if "DuplicateCheck" in params:
            dupcheck = params["DuplicateCheck"]
            try:
                self.perform_duplicate_check(
                    dupcheck["tableName"],
                    dupcheck["key_by_val"],
                    dupcheck["key_col"],
                    dupcheck.get("threshold", {"upper": 0, "lower": 0}),
                    dupcheck.get("criticality", True),
                )
            except Exception as e:
                if dupcheck.get("criticality", True):
                    raise
                else:
                    all_checks_passed = False
                    messages.append(str(e))

        if not all_checks_passed:
            print("Non-critical issues found:", messages)
        else:
            print("All data quality checks passed.")
