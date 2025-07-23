from custom_operator import PostgresInsertOperator


class PostgresStepInsertOperator(PostgresInsertOperator):
    """
    Executes a sequence of Postgres insert steps, each defined by a dictionary of parameters.
    """
    template_fields = ["steps"]

    def __init__(self, steps: list, **kwargs):
        """
        :param steps: List of dicts, each dict containing the parameters for a step.
        """
        if not steps or not isinstance(steps, list):
            raise ValueError("`steps` must be a non-empty list of dictionaries.")

        self.steps = steps
        self._set_step_attributes(steps[0])
        super().__init__(target=self.target, **kwargs)

    def _set_step_attributes(self, step: dict):
        """
        Helper to set instance attributes from a step dict.
        """
        self.sql = step.get("source_query")
        self.params = step.get("params", {})
        self.source = step.get("source")
        self._source_query = step.get("source_query")
        self.target = step.get("target")
        self.key_cols = step.get("key_cols")
        self._custom_filter = step.get("custom_filter")
        self._params = step.get("params")
        self.operation_type = step.get("operation_type", 'INSERT')
        self.postgres_conn_id = step.get("postgres_conn_id")

    def execute(self, context):
        for i, step in enumerate(self.steps, 1):
            self.log.info(f"Running step {i}/{len(self.steps)} ...")
            self._set_step_attributes(step)
            super().execute(context)
        self.log.info("All steps completed successfully!")
