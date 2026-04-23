from datetime import timedelta
import io
from minio import Minio
from minio.error import S3Error
from minio_config import config
from tabulate import tabulate
import pandas as pd


def main():
    client = Minio(
        "localhost:9000",
        access_key=config["ACCESS_KEY"],
        secret_key=config["SECRET_KEY"],
        secure=False,
    )

    objects = client.list_objects(
        bucket_name="bronze",
        recursive=True,
    )

    for obj in objects:
        if "nyc_taxis_files" in obj.object_name:
            print(
                f"Object: {obj.object_name}, Size: {obj.size} bytes, Last Modified: {obj.last_modified}"
            )
            url = client.presigned_get_object(
                "bronze",
                obj.object_name,
                expires=timedelta(hours=100),
            )

            data = pd.read_parquet(url)

            for _, row in data.iterrows():
                vendor_id = str(row["VendorID"])
                pickup_datetime = str(row["tpep_pickup_datetime"])
                pickup_datetime_formatted = pickup_datetime.replace(" ", "_").replace(
                    ":", "-"
                )
                file_name = f"trip_{vendor_id}_{pickup_datetime_formatted}.json"
                record = row.to_json()
                record_bytes = record.encode("utf-8")
                record_stream = io.BytesIO(record_bytes)
                record_stream_length = len(record_bytes)

                client.put_object(
                    bucket_name="nyc-taxis-records",
                    object_name=f"nyc_taxi_record/{file_name}",
                    data=record_stream,
                    length=record_stream_length,
                    content_type="application/json",
                )

                print(f"Uploaded record to nyc-taxis-records/{file_name}")

        break


if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("Error occurred.", exc)
