# Prefect-GCP

![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![TensorFlow](https://img.shields.io/badge/TensorFlow-2.17.1-FF6F00?logo=tensorflow)
![Apache Beam](https://img.shields.io/badge/Apache%20Beam-2.x-orange)
![Google Cloud](https://img.shields.io/badge/Google%20Cloud-Dataflow-blue)
![Prefect](https://img.shields.io/badge/Prefect-3.x-purple)
![Docker](https://img.shields.io/badge/Docker-supported-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![GitHub Copilot](https://img.shields.io/badge/Built%20with-GitHub%20Copilot-brightgreen)

A prototype project demonstrating the integration of [Apache Beam](https://beam.apache.org/get-started/quickstart/python/) with [Prefect](https://docs.prefect.io/v3/get-started) on [Google Cloud Platform](https://console.cloud.google.com/). This project serves as a foundation for building scalable data processing pipelines with modern orchestration and distributed processing frameworks.

This project was developed as an experiment using [GitHub Copilot](https://github.com/features/copilot), utilizing both GPT-4o and Claude Sonnet 4 models (see Copilot [AI model comparison](https://docs.github.com/en/copilot/reference/ai-models/model-comparison)).

---

## Project Structure
```
prefect-gcp
├── src
│   └── prefect_gcp
│       ├── beam
│       │   └── pipeline.py
│       ├── data
│       │   └── process.py
│       ├── flow
│       │   └── prefect.py
│       ├── model
│       │   └── train.py
│       └── vertex
│           └── job.py
├── tests
│   ├── unit
│   └── adhoc
├── requirements.in
├── requirements.txt
├── tox.ini
├── pyproject.toml
├── LICENSE
└── README.md
```

## Prerequisites

- Python versions between 3.10 and 3.12
- [Docker Desktop](https://docs.docker.com/desktop/) or [Colima](https://github.com/abiosoft/colima)
- Google Cloud SDK (gcloud CLI)
- Access to a Google Cloud Project with the following APIs enabled:
  - Dataflow API
  - Artifact Registry API
  - Cloud Storage API

## Environment Setup

Create a Python environment
```bash
conda create -n prefect-gcp python=3.12
conda activate prefect-gcp
```

or
```bash
python -m venv prefect-gcp
source prefect-gcp/bin/activate  # On Windows: prefect-gcp\Scripts\activate
```

Install the required Python packages
```
pip install -r requirements.txt
```

Install the modules for this project
```
pip install -e .
```

Configure Google Cloud
```
gcloud auth application-default login
```

Set the Google Cloud project ID
```
gcloud config set project YOUR_PROJECT_ID
```

## Configuration
Create a `project.override.cfg` file to customize settings for your environment:

```ini
[project]
id = your-gcp-project-id
region = us-central1
temp_location = gs://your-bucket/temp/

[data]
input = gs://your-bucket/data/input.csv
output = gs://your-bucket/data/output/
dataflow.runner = DirectRunner  # Use DirectRunner for local testing
```

## Docker Setup
Build the custom container image for Dataflow, update the path to the artifact registry accordingly
```bash
./scripts/image_build.sh
```

## Usage

You can either run the Apache Beam pipeline directly, or use Prefect to orchistrate the pipeline as part of a workflow.

### Beam Pipeline
To run the Apache Beam pipeline directly with `DirectRunner` on your laptop:
```bash
run-dataflow
```

To run it with Dataflow on Google Cloud, comment out the following line in `project.override.cfg`:
```ini
; dataflow.runner = DirectRunner
```
and then use the same command:
```bash
run-dataflow
```

### Vertex AI Training Job
To submit a hyperparameter tuning job directly to Vertex AI, you can use the run-vertex-job command. This script reads the configuration from `project.cfg` and `project.override.cfg` to define and launch the training job.
```bash
run-vertex-job
```

### Prefect Orchestrated Execution
This project uses the `run-prefect-flow` command to orchestrate a multi-step machine learning pipeline with Prefect. The primary flow, e2e, consists of two main steps:

1. **Data Processing**: Runs a Dataflow job to process and prepare the data.
1. **Model Training**: Runs a Vertex AI hyperparameter tuning job to train a model on the processed data.

While single-step flows for data processing are also available, the main orchestration demonstrates the end-to-end workflow.

#### Run a Flow Locally
To execute a flow immediately for testing or debugging, use the `run` action:

Run the default `dataflow_processing_flow` to process data with Dataflow:
```bash
run-prefect-flow run
```

Run the 'batch_processing_flow'
```bash
run-prefect-flow run --flow-name batch
```

#### Deploy to Prefect Server
First, start a Prefect server (if running locally):
```bash
prefect server start
```

Deploy the flow:
```bash
prefect deploy src/prefect_gcp/flow/prefect.py:dataflow_processing_flow --name "dataflow-pipeline"
```

Run the deployed flow:
```bash
prefect deployment run "prefect-gcp-dataflow/dataflow-pipeline"
```

#### Monitor in Prefect UI
Access the Prefect UI at `http://localhost:4200` to monitor flow runs, view logs, and manage deployments.

#### Prefect Cloud
If you would like to try [Prefect Cloud](https://app.prefect.cloud/account/), the steps will be similar. First, follow the instructions to create an account, a workspace, and an API key. Then

```bash
prefect cloud login -k YOUR_API_KEY
```

Make sure the setup works
```bash
prefect cloud workspace ls
```

And then
```bash
run-prefect-flow deploy --flow-name prefect-gcp-dataflow --schedule "0 5 * * *"
```

## Development Notes
### AI Assistant Experience
This project was developed as an experiment with AI coding assistants. Initially, I used GitHub Copilot, leveraging both the GPT-4o and Claude Sonnet 4 models.

* **GitHub Copilot (Paid Subscription)**:
  * Strengths: Excellent for generating boilerplate code, project structures, and configuration templates.
  * Limitations: Struggled with domain-specific Apache Beam and Dataflow configurations, often requiring cross-reference with official documentation. Standalone ChatGPT sometimes provided more accurate solutions.

* **Gemini Code Assist (Free)**:
  * After the initial development, I discovered and switched to Gemini Code Assist. My experience has been that it is at least as good as, if not better than, the paid Copilot offerings. It provides high-quality, context-aware assistance without a subscription fee.

* **Key Takeaway & Recommendation**: While AI assistants are powerful, domain expertise remains crucial. For developers exploring this public project, I recommend starting with the free and highly capable Gemini Code Assist. Consider a paid tool only if you find a compelling, specific reason to do so.

## Resources
* [Apache Beam Documentation](https://beam.apache.org/documentation/)
* [Prefect Documentation](https://docs.prefect.io/)
* [Google Cloud Dataflow](https://cloud.google.com/dataflow/docs)
* [GitHub Copilot](https://github.com/features/copilot)
