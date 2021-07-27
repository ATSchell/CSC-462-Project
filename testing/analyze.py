import os
import folium
from folium import plugins
import rasterio as rio
from rasterio import plot
from rasterio.warp import calculate_default_transform, reproject, Resampling
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.colors import ListedColormap
import matplotlib.colors as colors
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

os.mkdir('./blobdata')

try:
    #import data from AZURE blob storage
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING') # environment variable needs to be set beforehand.
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_name = "eodata"
    container_client = blob_service_client.get_container_client(container_name)
    
    cmap = ListedColormap(["white","grey","blue","tan","springgreen","green","darkgreen"])
    norm = colors.BoundaryNorm([-1,-0.5,0,0.1,0.3,0.6,0.8,1],7)
    
    i =0
    blob_list = container_client.list_blobs()
    fig,ax = plt.subplots(1, len(blob_list), figsize=(10,5))
    for blob in blob_list:
        blob_client = container_client.get_blob_client(blob)
        download_file_path = "./blobdata/processedimg" + str(i) + ".tiff"
        downloadfile = open(download_file_path, "wb")
        downloadfile.write(blob_client.download_blob().readall())
        downloadfile.close()

        with rio.open(download_file_path, 'r') as mapfile:
            map = mapfile.read()
        
        map_plot = ax[0][i].imshow(map, cmap = cmap, norm= norm)

        legend_labels = {"white":"snow", "grey":"snow", "blue":"water bodies", "tan": "rocks and sparse vegetation", "springgreen":"shallow vegetation", "green":"dense vegetation", "darkgreen": "very dense vegetation"}
        patches = [Patch(color=color, label = label) for color, label in legend_labels.items()]

        ax[0][i].legend( handles=patches, bbox_to_anchor(1.35,1), facecolor="white")
        i = i + 1
    plt.show()

except Exception as ex:
    print('Exception: ')
    print(ex)

