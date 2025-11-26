import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import logging


class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--output_path",
            required=True,
            help="GCS prefix where the text files will be written, e.g. gs://my-bucket/output/test",
        )


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
            | "CreateMessages" >> beam.Create(["hello", "dataflow"])
            | "WriteToGCS" >> beam.io.WriteToText(
                file_path_prefix=opts.output_path,
                file_name_suffix=".txt",
                num_shards=1,
            )
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
