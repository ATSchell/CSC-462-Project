from __future__ import print_function

import base64
import io
import os
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
                print("gRPC Error: " + rpc_error.details())
                continue
        break

    warnings.filterwarnings('ignore')
    # Receive an encoded response
    input_buffer = base64.b64decode(response.image)
    band_input_1 = open("new.tiff", 'wb')
    band_input_1.write(input_buffer)
    band_input_1.close()

    band3 = rasterio.open("new.tiff")

    red = band3.read(3).astype('float32')
    nir = band3.read(4).astype('float32')

    index = 0
    ndvi = red
    for i, j in zip(nir, red):
        try:
            ndvi[index] = (i - j) / (i + j)
        except ZeroDivisionError:
            ndvi[index] = 0
        index = index + 1

    # export ndvi/ndsi image
    ndvi_image = rasterio.open('./Intermediate/result-' + str(os.getpid()) + '.tiff', 'w', width=band3.width,
                               height=band3.height, count=1, crs='EPSG:4326', transform=band3.transform,
                               dtype='float32')
    ndvi_image.write(ndvi, 1)
    ndvi_image.close()
    band = Image.open('./Intermediate/result-' + str(os.getpid()) + '.tiff')

    buffer = io.BytesIO()
    band.save(buffer, 'TIFF')
    buffer.seek(0)
    image_bytes = buffer.read()
    buffer.close()
    output_image = base64.b64encode(image_bytes)
    warnings.resetwarnings()

    response = stub.ReturnImage(dist_processing_pb2.ProcessedImage(worker_name=str(os.getpid()),
                                                                   image_name=response.image_name, image=output_image))


if __name__ == '__main__':
    # logging.basicConfig()
    # Since the workers on on external servers they wil run continuously requesting tasks
    counter = 0
    while True:
        print(counter)
        counter = counter + 1
        run()
