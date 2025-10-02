from prefect import task
from apache_beam import DoFn, Pipeline, ParDo
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners import DirectRunner, DataflowRunner
from prefect_gcp.beam.beam_pipeline import BeamPipeline
from mlp_conf.config import MlpConfig
from mlp_conf.argparse import MlpArgumentParser
import apache_beam as beam


class MyTransform(DoFn):
    def process(self, element):
        # Implement your transformation logic here
        yield element


@task
def run_beam_pipeline(input_data):
    with Pipeline() as pipeline:
        output = (
            pipeline
            | "Read Input" >> beam.Create(input_data)
            | "Transform Data" >> ParDo(MyTransform())
            # Add more transformations as needed
        )
    return output


class DataflowRunner:
    """Runs the Beam pipeline using DirectRunner or DataflowRunner."""

    def __init__(self, runner: str, project: str, region: str, temp_location: str):
        self.runner = runner
        self.project = project
        self.region = region
        self.temp_location = temp_location

    def run(self):
        """Run the Beam pipeline."""
        pipeline_options = PipelineOptions(
            runner=self.runner,
            project=self.project,
            region=self.region,
            temp_location=self.temp_location,
        )
        with beam.Pipeline(options=pipeline_options) as pipeline:
            BeamPipeline.build_pipeline(pipeline)


def main():
    # Load configuration
    cfg = MlpConfig("project.cfg")

    # Set up argument parser
    parser = MlpArgumentParser(cfg, description="Run Apache Beam pipeline with Dataflow")
    parser.add_argument("--runner", type=str, help="Beam runner to use (e.g., DirectRunner, DataflowRunner)")
    parser.add_argument("--project", type=str, help="GCP project ID")
    parser.add_argument("--region", type=str, help="GCP region")
    parser.add_argument("--temp_location", type=str, help="GCS bucket for temporary files")

    # Parse arguments
    args = parser.parse_args()

    # Run the Dataflow pipeline
    runner = DataflowRunner(
        runner=args.runner,
        project=args.project,
        region=args.region,
        temp_location=args.temp_location,
    )
    runner.run()


if __name__ == "__main__":
    main()