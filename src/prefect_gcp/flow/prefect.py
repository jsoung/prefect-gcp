import argparse
import logging

from prefect import flow, task
from prefect.server.schemas.schedules import CronSchedule

from prefect_gcp.beam.pipeline import Pipeline as BeamPipeline


@task
def run_beam_pipeline_task():
    """
    Prefect task that runs the Apache Beam pipeline.

    Returns:
        str: Status message indicating pipeline completion.
    """
    logger = logging.getLogger(__name__)

    try:
        # Initialize the Beam pipeline using your existing implementation
        beam_pipeline = BeamPipeline()

        # Create pipeline options
        pipeline_options = beam_pipeline.create_pipeline_options()

        # Run the pipeline
        beam_pipeline.run_beam_pipeline(pipeline_options)

        logger.info("Beam pipeline completed successfully via Prefect")
        return "Pipeline completed successfully"

    except Exception as e:
        logger.error(f"Beam pipeline failed: {e}")
        raise


@flow(name="prefect-gcp-dataflow")
def dataflow_processing_flow():
    """
    Prefect flow that orchestrates the Beam pipeline execution.

    Returns:
        str: Result from the pipeline task.
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting Prefect flow for Beam pipeline")

    # Run the Beam pipeline as a Prefect task
    result = run_beam_pipeline_task()

    logger.info("Prefect flow completed")
    return result


@flow(name="prefect-gcp-batch-processing")
def batch_processing_flow(input_files: list = None):
    """
    More complex Prefect flow for batch processing multiple files.

    Args:
        input_files (list): List of input file paths to process.

    Returns:
        list: Results from processing each file.
    """
    logger = logging.getLogger(__name__)

    if not input_files:
        # Use default from configuration if no files specified
        logger.info("No input files specified, using default configuration")
        return run_beam_pipeline_task()

    results = []
    for input_file in input_files:
        logger.info(f"Processing file: {input_file}")
        # Here you could modify the configuration dynamically
        # or pass parameters to customize the pipeline
        result = run_beam_pipeline_task()
        results.append(result)

    return results


def main():
    """
    Main entry point for running or deploying Prefect flows.

    Provides a command-line interface to either run a flow immediately for testing
    or deploy a flow to be managed and scheduled by a Prefect server.

    Usage:
        python prefect.py run [--flow-name <name>]
        python prefect.py deploy [--flow-name <name>] [--schedule "0 5 * * *"]
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="Run or deploy Prefect flows for GCP Dataflow.")
    parser.add_argument(
        "action", choices=["run", "deploy"], help="Action to perform: 'run' the flow locally or 'deploy' it to Prefect."
    )
    parser.add_argument(
        "--flow-name",
        default="dataflow",
        choices=["dataflow", "batch"],
        help="The name of the flow to run or deploy. Defaults to 'dataflow'.",
    )
    parser.add_argument(
        "--schedule",
        type=str,
        default=None,
        help="A cron string for scheduling the deployment (e.g., '0 5 * * *' for 5 AM daily).",
    )
    args = parser.parse_args()

    flows = {
        "dataflow": dataflow_processing_flow,
        "batch": batch_processing_flow,
    }
    selected_flow = flows[args.flow_name]

    if args.action == "run":
        logger.info(f"Running the '{selected_flow.name}' flow...")
        try:
            # For a local run, you might pass default parameters if needed
            result = selected_flow() if args.flow_name == "dataflow" else selected_flow(input_files=[])
            logger.info(f"Flow run completed with result: {result}")
        except Exception as e:
            logger.error(f"Flow run failed: {e}", exc_info=True)
            raise

    elif args.action == "deploy":
        logger.info(f"Deploying the '{selected_flow.name}' flow...")
        schedule = CronSchedule(cron=args.schedule, timezone="UTC") if args.schedule else None
        selected_flow.serve(name=f"deployment-{selected_flow.name}", schedule=schedule)


if __name__ == "__main__":
    main()
