# Official Google-hosted Beam SDK harness image for Dataflow custom containers
FROM apache/beam_python3.12_sdk:2.68.0

# Set working directory inside the container
WORKDIR /prefect-gcp

# Copy dependency metadata from the project root
COPY requirements.txt pyproject.toml VERSION.txt project.cfg ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your source code
COPY src/ ./src/

# Install your code in editable mode (optional)
RUN pip install --no-cache-dir .
