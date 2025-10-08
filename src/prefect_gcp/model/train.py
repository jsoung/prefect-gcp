import argparse
import logging

import tensorflow as tf
from hypertune import HyperTune


class Trainer:
    """A class to train and evaluate a TensorFlow model for MNIST."""

    def __init__(self):
        """Initializes the Trainer, parses arguments, and sets up logging."""
        self.args = self._parse_args()
        self.logger = logging.getLogger(__name__)
        self.model = None
        self.x_train, self.y_train, self.x_test, self.y_test = None, None, None, None

    def _parse_args(self) -> argparse.Namespace:
        """Parses command-line arguments for hyperparameters."""
        parser = argparse.ArgumentParser()
        parser.add_argument("--learning_rate", type=float, default=0.01, help="Learning rate for the optimizer.")
        parser.add_argument("--num_units", type=int, default=64, help="Number of units in the hidden layer.")
        parser.add_argument("--epochs", type=int, default=5, help="Number of training epochs.")
        parser.add_argument("--metric_name", type=str, default="accuracy", help="The name of the metric to report.")
        return parser.parse_args()

    def _load_data(self):
        """Loads and prepares the MNIST dataset."""
        self.logger.info("Loading and preparing MNIST dataset...")
        (self.x_train, self.y_train), (self.x_test, self.y_test) = tf.keras.datasets.mnist.load_data()
        self.x_train, self.x_test = self.x_train / 255.0, self.x_test / 255.0

    def _build_model(self):
        """Builds the Keras Sequential model."""
        self.logger.info(f"Building model with {self.args.num_units} hidden units...")
        self.model = tf.keras.models.Sequential(
            [
                tf.keras.layers.Flatten(input_shape=(28, 28)),
                tf.keras.layers.Dense(self.args.num_units, activation="relu"),
                tf.keras.layers.Dropout(0.2),
                tf.keras.layers.Dense(10, activation="softmax"),
            ]
        )

    def _report_metric(self, epochs: int):
        """Evaluates the model and reports the metric to Vertex AI HyperTune."""
        self.logger.info("Evaluating model and reporting metric to HyperTune...")
        loss, accuracy = self.model.evaluate(self.x_test, self.y_test, verbose=0)
        self.logger.info(f"Final validation accuracy: {accuracy:.4f}")

        hpt = HyperTune()
        hpt.report_hyperparameter_tuning_metric(
            hyperparameter_metric_tag=self.args.metric_name, metric_value=accuracy, global_step=epochs
        )

    def run(self):
        """Orchestrates the training, and evaluation process."""
        self._load_data()
        self._build_model()

        self.logger.info(f"Compiling model with learning rate: {self.args.learning_rate}")
        self.model.compile(
            optimizer=tf.keras.optimizers.Adam(learning_rate=self.args.learning_rate),
            loss="sparse_categorical_crossentropy",
            metrics=["accuracy"],
        )

        epochs = self.args.epochs
        self.logger.info(f"Starting model training for {epochs} epochs...")
        self.model.fit(self.x_train, self.y_train, epochs=epochs, validation_data=(self.x_test, self.y_test))
        self.logger.info("Model training completed.")

        self._report_metric(epochs=epochs)


def main():
    """Main entry point for the training script."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    try:
        trainer = Trainer()
        trainer.run()  # All arguments are parsed internally
    except Exception as e:
        logging.getLogger(__name__).error(f"An error occurred during training: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
