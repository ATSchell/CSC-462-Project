import os
from azure.storage.blob import BlobServiceClient

# This is a used for grabbing files from arbutus directory
# Need to add ability to name Azure directory based on name of retrieved dataset
def run():
    SOURCE_FILE = '/home/user1/phase1/Output/processed/'
    connection_string = "<Enter Connection String Here"
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client("test")
    for f in os.listdr(SOURCE_FILE):
        with open(SOURCE_FILE+str(f), "rb") as data:
            blob_client = container_client.upload_blob(name='klinaklini_march_20_2019/'+str(f), data=data)

if __name__ == '__main__':
    run()