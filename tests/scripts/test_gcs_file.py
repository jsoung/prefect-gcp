from prefect_gcp.data.process import Process

if __name__ == "__main__":
    process = Process()
    gcs_path = "gs://your-bucket/path/to/csv/files"
    data = process.load_data(gcs_path)
    print(data)