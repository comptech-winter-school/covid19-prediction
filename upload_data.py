import boto3
from pathlib import Path
from envparse import env

env.read_envfile()

bucket_name = "covid-19"
session = boto3.session.Session()
s3 = session.client(
    aws_access_key_id=env("ACCESS_KEY_ID"),
    aws_secret_access_key=env("SECRET_ACCESS_KEY_ID"),
    service_name="s3",
    endpoint_url="https://storage.yandexcloud.net",
)

directory = Path("tmp")
for pattern in ["*.csv", "*.json", "*.pickle", "*.parquet"]:
    for file in directory.glob(pattern):
        s3.upload_file(str(file.absolute()), bucket_name, file.name)
