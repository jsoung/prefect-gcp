import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from prefect import flow, task
from mlp_conf.config import MlpConfig
from mlp_conf.argparse import MlpArgumentParser
from prefect_gcp.data.process import Process


@task
def create_pipeline_options(runner: str, project: str, region: str, temp_location: str):
    """Create Apache Beam pipeline options."""
    options = {
        "runner": runner,
        "project": project,
        "region": region,
        "temp_location": temp_location,
    }
    return PipelineOptions(flags=[], **{k: v for k, v in options.items() if v is not None})


@task
def run_beam_pipeline(pipeline_options: PipelineOptions):
    """Run the Apache Beam pipeline."""
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Extract Data" >> beam.Create(Process.extract_data())
            | "Transform Data" >> beam.Map(Process.transform_data)
            | "Load Data" >> beam.Map(Process.load_data)
        )


@flow
def beam_pipeline(runner: str, project: str, region: str, temp_location: str):
    """Prefect flow to run the Beam pipeline."""
    # Create pipeline options
    pipeline_options = create_pipeline_options(runner, project, region, temp_location)

    # Run the Beam pipeline
    run_beam_pipeline(pipeline_options)


def main():
    # Load configuration
    cfg = MlpConfig("project.cfg")

    # Set up argument parser
    parser = MlpArgumentParser(cfg, description="Run Apache Beam pipeline with Prefect")
    parser.add_argument("--runner", type=str, help="Beam runner to use (e.g., DirectRunner, DataflowRunner)")
    parser.add_argument("--project", type=str, help="GCP project ID")
    parser.add_argument("--region", type=str, help="GCP region")
    parser.add_argument("--temp_location", type=str, help="GCS bucket for temporary files")

    # Parse arguments
    args = parser.parse_args()

    # Run the Prefect flow
    beam_pipeline(
        runner=args.runner,
        project=args.project,
        region=args.region,
        temp_location=args.temp_location,
    )


if __name__ == "__main__":
    main()