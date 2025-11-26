import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import logging
from google.cloud import secretmanager
import pyodbc


class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--project_id",
            required=True,
            help="GCP project ID where the secret lives.",
        )
        parser.add_argument(
            "--secret_id",
            required=True,
            help="Secret ID in Secret Manager (without version).",
        )
        parser.add_argument(
            "--output_path",
            required=True,
            help="GCS prefix where the text files will be written, e.g. gs://my-bucket/output/pyodbc_test",
        )


class TestPyodbcAndSecretDoFn(beam.DoFn):
    def __init__(self, project_id: str, secret_id: str):
        self.project_id = project_id
        self.secret_id = secret_id
        self.client = None
        self.secret_value = None

    def setup(self):
        # Solo para ver en logs que pyodbc está disponible
        logging.info("pyodbc version: %s", getattr(pyodbc, '__version__', 'unknown'))

        # Leer el secreto una vez por worker
        self.client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{self.project_id}/secrets/{self.secret_id}/versions/latest"
        response = self.client.access_secret_version(request={"name": name})
        self.secret_value = response.payload.data.decode("utf-8")
        logging.info("Secret retrieved in setup; length=%d", len(self.secret_value))

    def process(self, _):
        yield f"pyodbc import OK, version={getattr(pyodbc, '__version__', 'unknown')}"
        yield f"Secret length: {len(self.secret_value)}"
        yield f"Secret (first 50 chars): {self.secret_value[:50]}"


def run(argv=None):
    pipeline_options = PipelineOptions(
        argv,
        save_main_session=True
    )
    pipeline_options.view_as(SetupOptions).save_main_session = True
    opts = pipeline_options.view_as(CustomOptions)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Start" >> beam.Create([None])
            | "Test pyodbc + secret" >> beam.ParDo(TestPyodbcAndSecretDoFn(
                project_id=opts.project_id,
                secret_id=opts.secret_id,
            ))
            | "WriteToGCS" >> beam.io.WriteToText(
                file_path_prefix=opts.output_path,
                file_name_suffix=".txt",
                num_shards=1,
            )
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
