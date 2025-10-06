from prefect import task, flow
import logging
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
    Main entry point for running Prefect flows.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    logger.info("Starting Prefect-GCP integration")
    
    # Run the simple dataflow processing flow
    try:
        result = dataflow_processing_flow()
        logger.info(f"Flow result: {result}")
    except Exception as e:
        logger.error(f"Flow failed: {e}")
        raise


if __name__ == "__main__":
    main()