import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from mlp_conf.argparse import MlpArgumentParser

from prefect_gcp.data.process import Process  # Import the Process class


class DataProcessingDoFn(beam.DoFn):
    """A ParDo class that processes data using the Process class."""

    def __init__(self) -> None:
        """Initialize the DataProcessingDoFn class."""
        super().__init__()
        self.processor = None  # Initialize as None
        self.args = MlpArgumentParser().parse_args()  # Parse command line arguments

    def setup(self):
        """Set up resources that are not serializable."""
        self.processor = Process()  # Initialize the Process class here

    def process(self, element: str):
        """
        Process each element using the Process class.

        Args:
            element (str): A single line of data from the CSV file.

        Yields:
            str: The processed data.
        """
        try:
            # Use the Process class to transform the data
            transformed_data = self.processor.transform_data(element)
            yield transformed_data
        except Exception as e:
            logging.error(f"Error processing element: {element}. Error: {e}")
            raise


class Pipeline:
    """Encapsulates the Apache Beam pipeline logic."""

    def __init__(self, config_file: str = "project.cfg") -> None:
        """
        Initialize the Pipeline class with configuration and logging.

        Args:
            config_file (str): Path to the configuration file. Defaults to "project.cfg".
        """
        # Set up logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(logging.StreamHandler())

        # Parse command line arguments
        self.args = MlpArgumentParser().parse_args()
        self.data_processor = Process()

        # Validate required arguments
        if not self.args.data_input:
            raise ValueError("The --data_input argument is required.")
        if self.args.data_dataflow_runner != "DirectRunner" and not self.args.project_id:
            raise ValueError("The --project_id argument is required for non-DirectRunner pipelines.")

    def create_pipeline_options(self) -> PipelineOptions:
        """
        Create Apache Beam pipeline options.

        Returns:
            PipelineOptions: The Apache Beam pipeline options.
        """
        # Base options
        options = {"runner": self.args.data_dataflow_runner}

        # Add GCP-specific options only if the runner is DataflowRunner
        if self.args.data_dataflow_runner == "DataflowRunner":
            options.update({
                "project": self.args.project_id,
                "region": self.args.project_region,
                "temp_location": self.args.project_temp_location,
                "sdk_container_image": self.args.data_dataflow_sdk_container_image,
                "worker_harness_container_image": self.args.data_dataflow_sdk_container_image,
                "machine_type": self.args.data_dataflow_machine_type,
                "num_workers": self.args.data_dataflow_num_workers,
                "max_num_workers": self.args.data_dataflow_max_num_workers,
            })

        self.logger.info(f"Pipeline options for Dataflow: {options}")

        # Filter out None values and create PipelineOptions
        return PipelineOptions(flags=[], **{k: v for k, v in options.items() if v is not None})

    def run_beam_pipeline(self, pipeline_options: PipelineOptions) -> None:
        """
        Run the Apache Beam pipeline.

        Args:
            pipeline_options (PipelineOptions): The options for the Beam pipeline.
        """
        self.logger.info("Starting the Beam pipeline...")

        data_train_path = self.data_processor.download_data(self.args.data_train)
        data_test_path = self.data_processor.download_data(self.args.data_test)

        try:
            with beam.Pipeline(options=pipeline_options) as p:
                # Create a branch for processing the training data
                train_data = p | "Read Train CSV" >> beam.io.ReadFromText(data_train_path, skip_header_lines=1)
                processed_train = train_data | "Process Train Data" >> beam.ParDo(DataProcessingDoFn())
                processed_train | "Write Train Output" >> beam.io.WriteToText(self.args.data_train_processed)

                # Create a second branch for processing the test data
                test_data = p | "Read Test CSV" >> beam.io.ReadFromText(data_test_path, skip_header_lines=1)
                processed_test = test_data | "Process Test Data" >> beam.ParDo(DataProcessingDoFn())
                processed_test | "Write Test Output" >> beam.io.WriteToText(self.args.data_test_processed)

            self.logger.info("Beam pipeline completed successfully")
        except Exception as e:
            self.logger.error(f"Pipeline failed with error: {e}")
            raise RuntimeError("Beam pipeline execution failed.") from e

    def run(self) -> None:
        """
        Run the Beam pipeline using arguments from the command line.
        """
        # Create pipeline options
        pipeline_options = self.create_pipeline_options()

        self.logger.debug(pipeline_options.get_all_options())  # For debugging purposes

        # Run the Beam pipeline
        self.run_beam_pipeline(pipeline_options)


def main() -> None:
    """
    Main entry point for the script. Initializes the Pipeline class and runs the pipeline.
    """
    pipeline = Pipeline()
    pipeline.run()


if __name__ == "__main__":
    main()
