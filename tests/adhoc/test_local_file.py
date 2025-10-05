from prefect_gcp.data.process import Process

if __name__ == "__main__":
    process = Process()
    file_path = "fixture/iris.csv"
    data = process.load_data(file_path)
    print(data)