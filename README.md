This is a prototype project that integrates Apache Beam with Prefect on Google Cloud Platform (GCP). This project serves as a foundation for building data processing pipelines using modern orchestration and data processing tools.

### Project Structure

```
prefect-gcp
├── src
│   ├── prefect_gcp
│   │   ├── flows
│   │   │   └── beam_pipeline.py
│   │   ├── tasks
│   │   │   ├── beam_tasks.py
│   │   │   └── gcp_tasks.py
│   │   ├── config
│   │   │   └── settings.py
│   │   ├── utils
│   │   │   └── helpers.py
│   │   └── pipelines
│   │       └── sample_pipeline.py
├── configs
│   ├── development.yaml
│   ├── production.yaml
│   └── staging.yaml
├── docker
│   ├── Dockerfile
│   └── docker-compose.yml
├── requirements.in
├── requirements.txt
├── setup.py
├── pyproject.toml
└── README.md
```

### Installation

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

### Configuration

Configuration settings are managed using YAML files located in the `configs` directory. You can modify the `development.yaml`, `production.yaml`, and `staging.yaml` files to suit your environment.

### Usage

To run the sample pipeline, execute the following command:

```bash
python pipelines/sample_pipeline.py
```

### Contributing

Contributions are welcome! Please submit a pull request or open an issue for any enhancements or bug fixes.

### License

This project is licensed under the MIT License. See the LICENSE file for more details.