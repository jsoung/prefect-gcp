# Prefect-GCP

This is a prototype project that integrates [Apache Beam](https://beam.apache.org/get-started/quickstart/python/) with [Prefect](https://docs.prefect.io/v3/get-started) on [Google Cloud](https://console.cloud.google.com/). The project is designed to serve as a foundation for building data processing pipelines using modern orchestration and data processing tools.

---

## Project Structure
```
prefect-gcp
├── src
│   ├── beam
│   │   └── pipeline.py
│   ├── data
│   │   └── process.py
│   ├── dataflow
│   │   └── runner.py
│   ├── flows
│   │   └── prefect.py
│   ├── utils
│   │   └── helpers.py
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

## Installation

To set up the project, clone the repository and install the required dependencies:

1. Install `pip-tools`:
   ```bash
   pip install pip-tools
   ```
2. Clone the repository:
   ```bash
   git clone <repository-url>
   cd prefect-gcp
   ```
3. Install the required packages:
   ```bash
   pip-sync requirements.txt
   ```

## Configuration

Configuration settings are managed using YAML files located in the `configs` directory. You can modify the `development.yaml`, `production.yaml`, and `staging.yaml` files to suit your environment.

## Usage

To run the sample pipeline, execute the following command:

```bash
python pipelines/sample_pipeline.py
```

## Contributing

Contributions are welcome! Please submit a pull request or open an issue for any enhancements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.