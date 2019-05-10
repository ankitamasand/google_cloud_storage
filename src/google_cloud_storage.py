import os
import sys
import traceback

from google.cloud import storage

from constants import GOOGLE_CREDS_FILE, BUCKET, UPLOAD_FILE, BLOB_PATH, DOWNLOAD_FILE


class GoogleCloudStorage:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def upload_file(self, file_path, blob_path):
        sys.stdout.write(" file({file_path}) to Google Cloud Store => blob ({blob_path})\n"
                         .format(file_path=file_path, blob_path=blob_path))

        try:
            client = storage.Client()
            bucket = client.get_bucket(self.bucket_name)
            blob = bucket.blob(blob_path)

            blob.upload_from_filename(filename=file_path)

        except Exception as err:
            sys.stdout.write("Failed copying file({file_path}) to Google Cloud Store => blob ({blob_path})\n"
                             .format(file_path=file_path, blob_path=blob_path))

            print traceback.format_exc()
            raise Exception(str(err))

        sys.stdout.write("Completed copying file({file_path}) to Google Cloud Store => blob ({blob_path})\n"
                         .format(file_path=file_path, blob_path=blob_path))

    def download_blob(self, blob_path, download_file):
        sys.stdout.write("Start downloading cloud file ({blob_path}) to download file ({download_file}) \n"
                         .format(blob_path=blob_path, download_file=download_file))

        try:
            client = storage.Client()
            bucket = client.get_bucket(self.bucket_name)
            blob = bucket.get_blob(blob_path)

            if blob and blob.exists():
                with open(download_file, "w") as file_obj:
                    blob.download_to_file(file_obj)
            else:
                raise Exception("Blob does not exist on Google Storage")

        except Exception as err:
            sys.stdout.write("Failed downloading cloud file ({blob_path}) to download file ({download_file}) \n"
                             .format(blob_path=blob_path, download_file=download_file))

            print traceback.format_exc()
            raise Exception(str(err))

        sys.stdout.write("Completed downloading cloud file ({blob_path}) to download file ({download_file}) \n"
                         .format(blob_path=blob_path, download_file=download_file))


if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_CREDS_FILE

    gcs_obj = GoogleCloudStorage(bucket_name=BUCKET)

    gcs_obj.upload_file(UPLOAD_FILE, BLOB_PATH)
    gcs_obj.download_blob(BLOB_PATH, DOWNLOAD_FILE)
