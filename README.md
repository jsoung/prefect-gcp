# Prefect-GCP

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
│       └── flows
│           └── prefect.py
├── tests
│   ├── unit
│   │   └── test_process.py
│   ├── scripts
│   │   ├── test_local_file.py
│   │   └── test_gcs_file.py
├── configs
│   ├── development.yaml
│   ├── production.yaml
│   └── staging.yaml
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

## Usage
To run the sample pipeline with `DirectRunner` on your laptop
```bash
run-dataflow
```

To run it with Dataflow on Google Cloud, comment out the following line in `project.override.cfg`
```INI
; dataflow.runner = DirectRunner
```
and then use the same command
```bash
run-dataflow
```

## Docker Setup

Build the custom container image for Dataflow:
```bash
./scripts/image_build.sh
```

## Development Notes
### GitHub Copilot Experience
This project was developed as an experiment with GitHub Copilot, providing insights into AI-assisted development:

* **Strengths:**
  * Excellent at generating project structure and boilerplate code
  * Helpful for creating configuration files and documentation templates
  * Good at suggesting Python packaging best practices
* **Limitations:**
  * Struggled with domain-specific Apache Beam and Dataflow configurations
  * Required frequent reference to official documentation for complex integrations
  * ChatGPT (outside VS Code) often provided more accurate solutions than integrated Copilot models

* **Key Takeaway:** While AI coding assistants excel at scaffolding and common patterns, domain expertise and official documentation remain crucial for specialized frameworks and cloud services.

## Resources
* [Apache Beam Documentation](https://beam.apache.org/documentation/)
* [Prefect Documentation](https://docs.prefect.io/)
* [Google Cloud Dataflow](https://cloud.google.com/dataflow/docs)
* [GitHub Copilot](https://github.com/features/copilot)
