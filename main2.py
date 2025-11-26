import json
import logging
from typing import Any, Dict

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from google.cloud import secretmanager


class ConnectionTestOptions(PipelineOptions):
    """Pipeline options for SQL Server connectivity validation."""

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--gcp_project", required=True, help="GCP project where the secret resides")
        parser.add_argument("--secret_id", required=True, help="Secret Manager ID containing SQL connection JSON")
        parser.add_argument(
            "--database_override",
            default=None,
            help="Optional database name override (defaults to value in secret payload)",
        )
        parser.add_argument(
            "--query",
            default="SELECT TOP 1 TABLE_NAME FROM INFORMATION_SCHEMA.TABLES",
            help="SQL query executed to validate the connection",
        )
        parser.add_argument(
            "--output_gcs_path",
            default=None,
            help="Optional gs:// bucket/prefix for JSONL results",
        )
        parser.add_argument("--log_results", default="true")
        parser.add_argument("--run_id", default="sqlserver_connection_test")


def _normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y", "t"}


def _gcs_join(*parts: str) -> str:
    cleaned = [s.strip("/") for s in parts if s]
    if not cleaned:
        return ""
    prefix = cleaned[0]
    if prefix.startswith("gs://"):
        prefix = prefix[5:]
    return "gs://" + "/".join([prefix] + cleaned[1:])


def get_sql_config(secret_id: str, project_id: str) -> Dict[str, Any]:
    logging.info("Accessing secret '%s' in project '%s'", secret_id, project_id)
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    resp = client.access_secret_version(request={"name": name})
    payload = resp.payload.data.decode("utf-8")
    logging.info("Secret retrieved successfully.")
    return json.loads(payload)


def build_connection_string(cfg: Dict[str, Any]) -> str:
    driver = cfg.get("driver", "ODBC Driver 17 for SQL Server")
    server = cfg.get("privateIPaddress") or cfg.get("publicIPaddress") or cfg.get("server")
    if not server:
        raise ValueError("Cannot determine SQL Server host from secret payload.")
    database = cfg["database"]
    username = cfg["username"]
    password = cfg["password"]
    port = cfg.get("port", 1433)
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server},{port};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=no;TrustServerCertificate=yes;"
    )


class ConnectionTestDoFn(beam.DoFn):
    """Runs the configured SQL query and emits a result dict or raises on error."""

    def __init__(self, default_project: str, expected_min_rows: int = 1):
        self._default_project = default_project
        self._expected_rows = max(1, int(expected_min_rows or 1))

    def process(self, test_case: Dict[str, Any]):
        import pyodbc

        name = test_case.get("name") or test_case.get("secret_id") or "unnamed"
        project = test_case.get("gcp_project") or self._default_project
        secret_id = test_case.get("secret_id")
        if not secret_id:
            raise ValueError(f"Test case '{name}' missing secret_id.")

        expected_min_rows = self._expected_rows
        query = test_case.get("query") or "SELECT TOP 1 1"
        override_database = test_case.get("database_override")

        cfg = get_sql_config(secret_id, project)
        if override_database:
            cfg = dict(cfg)
            cfg["database"] = override_database
        conn_str = build_connection_string(cfg)

        logging.info("Executing connection test '%s' against project=%s, database=%s", name, project, cfg["database"])
        with pyodbc.connect(conn_str, timeout=30) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchmany(expected_min_rows or 1)
            row_count = len(rows)
            if row_count < expected_min_rows:
                raise RuntimeError(
                    f"Test '{name}' returned {row_count} rows but expected at least {expected_min_rows}."
                )

        logging.info("Connection test '%s' succeeded; rows returned=%s", name, row_count)
        yield {
            "name": name,
            "project": project,
            "secret_id": secret_id,
            "query": query,
            "rows_returned": row_count,
            "database": cfg["database"],
        }


def run_pipeline(opts: ConnectionTestOptions, pipeline_options: PipelineOptions) -> None:
    log_results = _normalize_bool(getattr(opts, "log_results", True))
    with beam.Pipeline(options=pipeline_options) as p:
        test_definition = {
            "name": getattr(opts, "run_id", "sqlserver_connection_test"),
            "secret_id": opts.secret_id,
            "query": getattr(opts, "query", None),
            "database_override": getattr(opts, "database_override", None),
            "gcp_project": opts.gcp_project,
        }

        results = (
            p
            | "CreateTestCase" >> beam.Create([test_definition])
            | "ExecuteSQLTest" >> beam.ParDo(ConnectionTestDoFn(default_project=opts.gcp_project))
        )

        if _normalize_bool(log_results):
            _ = results | "LogResults" >> beam.Map(lambda r: logging.info("SQL test result: %s", r) or r)

        output_path = getattr(opts, "output_gcs_path", None)
        if output_path:
            sink_path = _gcs_join(output_path, f"connection_test_{getattr(opts, 'run_id', 'run')}")
            results | "WriteResults" >> beam.io.WriteToText(
                sink_path, file_name_suffix=".jsonl", shard_name_template=""
            )


def run(argv=None):
    logging.basicConfig(level=logging.INFO)
    pipeline_options = PipelineOptions(argv, save_main_session=True)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    opts = pipeline_options.view_as(ConnectionTestOptions)
    run_pipeline(opts, pipeline_options)


if __name__ == "__main__":
    run()
