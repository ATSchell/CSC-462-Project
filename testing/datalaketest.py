from azure.storage.blob import BlobServiceClient

# Source Image to upload
SOURCE_FILE = 'cooltestimage.png'

# Connection string retived for container, should be kept private, replace REDACTED With your string
connection_string = "<REDACTED>"

# Create new connection client to blob service
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Choose container in service to connect to 
container_client = blob_service_client.get_container_client("test")

# Do the actual upload, preserving file name and type.
with open(SOURCE_FILE, "rb") as data:
    blob_client = container_client.upload_blob(name= SOURCE_FILE, data=data)
