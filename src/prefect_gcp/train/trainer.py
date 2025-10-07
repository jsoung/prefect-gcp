import logging

from google.cloud import aiplatform
from mlp_conf.argparse import MlpArgumentParser


class Trainer:
    """
    A class to configure and run a Vertex AI HyperparameterTuningJob.

    This class encapsulates the logic for setting up hyperparameter search spaces,
    defining worker specifications, and launching a tuning job on Google Cloud's
    Vertex AI platform.
    """

    def __init__(self):
        """
        Initializes the Trainer.

        This method parses arguments from the command line and configuration files
        to set up the trainer.
        """
        self.args = MlpArgumentParser().parse_args()  # Parse command line arguments
        self.logger = logging.getLogger(__name__)

    def _define_parameters(self) -> dict:
        """Defines the hyperparameter search space."""
        return {
            "learning_rate": aiplatform.hyperparameter_tuning.DoubleParameterSpec(
                min=self.args.train_hparam_learning_rate_min, max=self.args.train_hparam_learning_rate_max, scale="log"
            ),
            "momentum": aiplatform.hyperparameter_tuning.DoubleParameterSpec(
                min=self.args.train_hparam_momentum_min, max=self.args.train_hparam_momentum_max, scale="linear"
            ),
            "num_units": aiplatform.hyperparameter_tuning.IntegerParameterSpec(
                min=self.args.train_hparam_num_units_min, max=self.args.train_hparam_num_units_max, scale="linear"
            ),
        }

    def _define_metric_spec(self) -> dict:
        """Defines the metric to optimize."""
        return {self.args.train_metric_name: self.args.train_metric_goal}

    def _define_worker_pool_specs(self) -> list:
        """Defines the worker pool specifications for each trial."""
        return [
            {
                "machine_spec": {
                    "machine_type": self.args.train_machine_type,
                    # "accelerator_type": self.args.train_accelerator_type,
                    # "accelerator_count": self.args.train_accelerator_count,
                },
                "replica_count": 1,
                "container_spec": {
                    "image_uri": self.args.train_image_uri,
                    "command": ["run-datflow"],
                },
            }
        ]

    def run(self, max_trial_count: int = 20, parallel_trial_count: int = 5, sync: bool = True):
        """
        Creates and runs the hyperparameter tuning job.

        Args:
            max_trial_count (int): The maximum number of trials to run.
            parallel_trial_count (int): The number of trials to run in parallel.
            sync (bool): Whether to run the job synchronously (blocking).
        """
        self.logger.info(f"Initializing HyperparameterTuningJob: {self.args.train_display_name}")

        # Define the custom job that the tuning job will run for each trial
        custom_job = aiplatform.CustomJob(
            display_name=f"{self.args.train_display_name}-trial",
            project=self.args.project_id,
            location=self.args.project_region,
            worker_pool_specs=self._define_worker_pool_specs(),
            staging_bucket=self.args.project_temp_location,
        )

        tuning_job = aiplatform.HyperparameterTuningJob(
            display_name=self.args.train_display_name,
            custom_job=custom_job,
            # hyperparameter_tuning_algorithm is deprecated and ignored. The service default is used.
            metric_spec=self._define_metric_spec(),
            parameter_spec=self._define_parameters(),
            max_trial_count=max_trial_count,
            parallel_trial_count=parallel_trial_count,
        )

        self.logger.info("Starting tuning job...")
        tuning_job.run(sync=sync)

        self.logger.info("Tuning job completed.")
        self.logger.info(f"Trials: {tuning_job.trials}")


def main():
    """Main entry point for running the training job."""
    logging.basicConfig(level=logging.INFO)
    # Example usage:
    # All configuration is now loaded from project.cfg and project.override.cfg
    trainer = Trainer()
    trainer.run()


if __name__ == "__main__":
    main()
