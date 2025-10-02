FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code into the container
COPY src/ ./src/
COPY pipelines/ ./pipelines/
COPY configs/ ./configs/

# Set the entry point for the container
CMD ["python", "-m", "prefect_gcp.flows.beam_pipeline"]