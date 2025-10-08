import argparse
import logging
import sys

from prefect import flow, task

from prefect_gcp.beam.pipeline import Pipeline as BeamPipeline
from prefect_gcp.vertex.job import Job as VertexJob


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


@task
def run_vertex_job_task():
    """
    Prefect task that runs the Vertex AI hyperparameter tuning job.

    Returns:
        str: Status message indicating job completion.
    """
    logger = logging.getLogger(__name__)
    try:
        logger.info("Initializing Vertex AI job.")
        # The VertexJob class reads its configuration from project.cfg
        vertex_job = VertexJob()
        vertex_job.run(sync=True)  # Run synchronously to wait for completion
        logger.info("Vertex AI job completed successfully via Prefect.")
        return "Vertex AI job completed successfully"
    except Exception as e:
        logger.error(f"Vertex AI job failed: {e}")
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


@flow(name="prefect-gcp-e2e-ml-pipeline")
def e2e_ml_pipeline_flow():
    """
    Prefect flow that orchestrates the full ML pipeline:
    1. Run the Beam/Dataflow job to process data.
    2. Run the Vertex AI job to train a model.

    Returns:
        str: Final status message.
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting end-to-end ML pipeline flow.")

    data_processing_result = run_beam_pipeline_task()
    training_result = run_vertex_job_task(wait_for=[data_processing_result])

    return training_result


def main():
    """
    Main entry point for running or deploying Prefect flows.

    Provides a command-line interface to either run a flow immediately for testing
    or deploy a flow to be managed and scheduled by a Prefect server.

    Usage:
        run-prefect-flow run [--flow-name <name>]
        run-prefect-flow deploy [--flow-name <name>] [--schedule "0 5 * * *"]
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="Run or deploy Prefect flows for GCP Dataflow.")
    parser.add_argument(
        "action",
        choices=["run", "deploy"],
        help="Action to perform: 'run' the flow locally or 'deploy' it to Prefect.",
    )
    parser.add_argument(
        "--flow-name",
        default="dataflow",
        choices=["dataflow", "batch", "e2e"],
        help="The name of the flow to run or deploy. Defaults to 'dataflow'. 'e2e' for the full ML pipeline.",
    )
    parser.add_argument(
        "--schedule",
        type=str,
        default=None,
        help="A cron string for scheduling the deployment (e.g., '0 5 * * *' for 5 AM daily).",
    )
    args = parser.parse_args()

    # Remove the 'action' positional argument from sys.argv
    # This prevents it from being passed to downstream parsers like MlpArgumentParser
    sys.argv.pop(1)

    flows = {
        "dataflow": dataflow_processing_flow,
        "batch": batch_processing_flow,
        "e2e": e2e_ml_pipeline_flow,
    }
    selected_flow = flows[args.flow_name]

    if args.action == "run":
        logger.info(f"Running the '{selected_flow.name}' flow...")
        try:
            # For a local run, you might pass default parameters if needed
            result = selected_flow()
            logger.info(f"Flow run completed with result: {result}")
        except Exception as e:
            logger.error(f"Flow run failed: {e}", exc_info=True)
            raise

    elif args.action == "deploy":
        logger.info(f"Deploying the '{selected_flow.name}' flow...")
        selected_flow.deploy(
            name=f"deployment-{selected_flow.name}",
            work_pool_name="default-agent-pool",  # Specify a work pool if not using the default
            cron=args.schedule,
            timezone="UTC" if args.schedule else None,
        )


if __name__ == "__main__":
    main()
