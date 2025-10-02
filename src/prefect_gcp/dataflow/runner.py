from prefect import flow, task
from prefect_gcp.dataflow.dataflow_runner import DataflowRunner
from mlp_conf.config import MlpConfig
from mlp_conf.argparse import MlpArgumentParser


@task
def run_dataflow_pipeline(runner: str, project: str, region: str, temp_location: str):
    """Run the Dataflow pipeline."""
    dataflow_runner = DataflowRunner(runner, project, region, temp_location)
    dataflow_runner.run()


@flow
def prefect_flow(runner: str, project: str, region: str, temp_location: str):
    """Prefect flow to orchestrate the Dataflow pipeline."""
    run_dataflow_pipeline(runner, project, region, temp_location)


def main():
    # Load configuration
    cfg = MlpConfig("project.cfg")

    # Set up argument parser
    parser = MlpArgumentParser(cfg, description="Run Prefect flow for Dataflow pipeline")
    parser.add_argument("--runner", type=str, help="Beam runner to use (e.g., DirectRunner, DataflowRunner)")
    parser.add_argument("--project", type=str, help="GCP project ID")
    parser.add_argument("--region", type=str, help="GCP region")
    parser.add_argument("--temp_location", type=str, help="GCS bucket for temporary files")

    # Parse arguments
    args = parser.parse_args()

    # Run the Prefect flow
    prefect_flow(
        runner=args.runner,
        project=args.project,
        region=args.region,
        temp_location=args.temp_location,
    )


if __name__ == "__main__":
    main()
