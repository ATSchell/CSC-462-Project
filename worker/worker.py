from __future__ import print_function

import base64
import io
import logging
import os
import time
import warnings

import grpc
import rasterio
from PIL import Image

import dist_processing_pb2
import dist_processing_pb2_grpc


def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = dist_processing_pb2_grpc.ImageTransferStub(channel)

    while True:
        # Request an image tile to process
        try:
            response = stub.RequestImage(dist_processing_pb2.ImageRequest(worker_name=str(os.getpid())))
        except grpc.RpcError as rpc_error:
            if rpc_error.code() == grpc.StatusCode.CANCELLED:
                continue
            elif rpc_error.code() == grpc.StatusCode.UNAVAILABLE:
                continue
            else:
                print("gRPC Error: "+ rpc_error.details())
                continue
        break

    # Receive an encoded response
    band_input_1 = base64.b64decode(response.image)
    img = Image.open(io.BytesIO(band_input_1))
    img.save('output1-' + str(os.getpid()) + '.jp2')

    band_input_2 = base64.b64decode(response.image2)
    img2 = Image.open(io.BytesIO(band_input_2))
    img2.save('output2-' + str(os.getpid()) + '.jp2')

    band3 = rasterio.open('output1-' + str(os.getpid()) + '.jp2')  # red
    band11 = rasterio.open('output2-' + str(os.getpid()) + '.jp2')  # nir

    band11.read(1)

    green = band3.read(1).astype('float32')
    swir = band11.read(1).astype('float32')

    warnings.filterwarnings('ignore')
    index = 0
    ndvi = green
    for i, j in zip(green, swir):
        try:
            ndvi[index] = (i - j) / (i + j)
        except ZeroDivisionError:
            ndvi[index] = 0
        index = index + 1
    warnings.resetwarnings()

    # export ndvi image
    ndvi_image = rasterio.open('./Intermediate/result-' + str(os.getpid()) + '.tiff', 'w', width=band11.width,
                              height=band11.height,count=1, crs='EPSG:3857', transform=band11.transform, dtype='float32')
    ndvi_image.write(ndvi, 1)
    ndvi_image.close()
    band = Image.open('./Intermediate/result-' + str(os.getpid()) + '.tiff')

    buffer = io.BytesIO()
    band.save(buffer, 'tiff')
    buffer.seek(0)
    image_bytes = buffer.read()
    buffer.close()
    output_image = base64.b64encode(image_bytes)

    response = stub.ReturnImage(dist_processing_pb2.ProcessedImage(worker_name=str(os.getpid()),
                                                                   image_name=response.image_name, image=output_image))


if __name__ == '__main__':
    #logging.basicConfig()
    # Since the workers on on external servers they wil run continuously requesting tasks
    while True:
        run()
