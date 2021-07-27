import pprint
import re
import sys
import threading
import time
import webbrowser
from concurrent import futures
import logging
import io
import os
from threading import Lock

import grpc
import rasterio
from PIL import Image
import base64
from itertools import product

from osgeo import gdal
from rasterio import windows
import rasterio as rio
from rasterio.io import MemoryFile

import dist_processing_pb2
import dist_processing_pb2_grpc

from config import SIGNED_URL
from config import OUT_PATH
from config import OUTPUT_SPACING_METRES
#from config import PROCESSED_PATH

from config import UL_LAT
from config import UL_LNG
from config import LR_LAT
from config import LR_LNG

mutex = Lock()
tasks = []
tasks_remaining = 0
worker_count = 2

curr_width = 0
curr_height = 0


def reset_dir(dir):
    for f in os.listdir(dir):
        os.remove(os.path.join(dir, f))


def download_images():
    # ul_lng, lr_lat, lr_lng, ul_lat
    ds = gdal.Warp(OUT_PATH, gdal.Open(f'/vsicurl/{SIGNED_URL}'),
                   outputBounds=[UL_LNG, LR_LAT, LR_LNG, UL_LAT],
                   outputBoundsSRS='EPSG:4326',
                   dstSRS='EPSG:32610',
                   xRes=OUTPUT_SPACING_METRES,
                   yRes=OUTPUT_SPACING_METRES,
                   resampleAlg='cubic')
    ds = None


# This tiling function is based on this
# https://gis.stackexchange.com/questions/285499/how-to-split-multiband-image-into-image-tiles-using-rasterio
def tiling(input_file_path, output_directory):
    reset_dir(output_directory)

    def get_tiles(ds, width=256, height=256):
        nols, nrows = ds.meta['width'], ds.meta['height']
        offsets = product(range(0, nols, width), range(0, nrows, height))
        big_window = windows.Window(col_off=0, row_off=0, width=nols, height=nrows)
        for col_off, row_off in offsets:
            window = windows.Window(col_off=col_off, row_off=row_off, width=width, height=height).intersection(
                big_window)
            transform = windows.transform(window, ds.transform)
            yield window, transform

    with rio.open(input_file_path) as inds:
        tile_width, tile_height = 256, 256

        global curr_width
        curr_width = inds.width
        global curr_height
        curr_height = inds.height

        meta = inds.meta.copy()

        for window, transform in get_tiles(inds):
            # print(window)
            meta['transform'] = transform
            meta['width'], meta['height'] = window.width, window.height

            out_file_name = input_file_path
            out_file_name.replace(".tiff", "")

            outpath = os.path.join(output_directory,
                                   out_file_name + '-{}-{}.tiff'.format(int(window.col_off), int(window.row_off)))
            with rio.open(outpath, 'w', **meta) as outds:
                outds.write(inds.read(window=window))


# This function assigns each set of image tile pairs from each layer as a task
def create_tasks(input_file1, directory1):
    tiling(input_file1, directory1)

    global tasks
    for filename1 in os.listdir(directory1):
        if filename1.endswith(".tiff"):
            new_dict = {
                "task_id": len(tasks),
                "worker_id": "unassigned",
                "filename1": filename1,
                "path1": os.path.join(directory1, filename1),
                "status": "waiting",
                "started:": "0"
            }
            tasks.append(new_dict)
            continue
        else:
            continue

    global tasks_remaining
    tasks_remaining = len(tasks)
    pprint.pprint(tasks)
    print(tasks_remaining)


# This function loads in images to memory
def read(band1):

    # Consider opening with rasterio or notebook demo
    band = open(band1, "rb")

    buffer = io.BytesIO(band.read())
    out_buf = buffer.read()
    output_image = base64.b64encode(out_buf)

    # Need to strip band1 name from path information and band number for unified output file
    return band1, output_image


def get_next_files():
    global tasks_remaining
    global tasks

    if tasks_remaining > 0:
        for job in tasks:
            if job["status"] == "waiting":
                if job["worker_id"] == "unassigned":
                    return job

            # elif (job["worker_id"] != "unassigned") and ((time.time() - float(job["started"])) > 45.0):
            # print("[WAITED FOR]")
            # return job  # Re-assign job that has timed out
            # print(time.time() - float(job["started"]))
        return None


def merge_tiles(tiles_path):
    global curr_width
    global curr_height

    curr_width = curr_width
    curr_height = curr_height
    background = Image.new('RGBA', (curr_width, curr_height), (255, 255, 255, 255))

    file_pos = ""

    width_offset = 0
    height_offset = 0
    for filename in os.listdir(tiles_path):
        if filename.endswith(".png"):

            try:
                img = Image.open(tiles_path + filename)

            except FileNotFoundError:
                print(filename + " not found")
                continue

            filename_prefix = filename[:filename.index('-')]
            if len(filename_prefix) < 1:
                filename_prefix = str(time.localtime()[0]) + '_' + \
                                  str(time.localtime()[1]) + '_' + str(time.localtime()[0]) + "_output"

            filename = filename[filename.index('-'):]
            file_pos = filename.replace(".png", "")
            file_pos = file_pos.split('-')

            print(file_pos)
            background.paste(img, (int(file_pos[1]), int(file_pos[2])))

            img.close()

    if len(file_pos) > 0:
        background.save('./output_merged/'+str(filename_prefix)+'-merged.png')
        f = open('./output_merged/'+str(filename_prefix)+'-coordinates.txt', "w+")
        f.write(str(UL_LAT)+" "+str(UL_LNG)+" "+str(LR_LAT)+" "+str(LR_LNG))


def findMinMax(tiles_path):
    min = 999
    max = -999
    for filename in os.listdir(tiles_path):
        if filename.endswith(".tiff"):
            gtif = gdal.Open("./processed/"+filename)
            srcband = gtif.GetRasterBand(1)

            # Get raster statistics
            stats = srcband.GetStatistics(True, True)

            # Maintain current min and max values for entire raster
            if stats[0] < min:
                min = stats[0]
            if stats[1] > max:
                max = stats[1]
            gtif = None

    return min, max


def convertToPNG(tiles_path, tiles_output):
    reset_dir(tiles_output)
    minval, maxval = findMinMax(tiles_path)
    for filename in os.listdir(tiles_path):
        if filename.endswith(".tiff"):
            output_filename = filename.replace(".tiff", "")

            options_list = [
                '-ot Byte',
                '-of PNG',
                '-b 1',
                '-a_nodata 0',
                '-scale',
                str(minval),
                str(maxval),
                '0 255'
            ]

            options_string = " ".join(options_list)

            gdal.Translate(
                tiles_output + output_filename + '.png',
                tiles_path + filename,
                options=options_string
            )


# gRPC Handler
class ImageTransfer(dist_processing_pb2_grpc.ImageTransferServicer):

    def __init__(self, stop_event):
        self.stop_event = stop_event

    def RequestImage(self, request, context):
        global tasks
        global tasks_remaining
        global worker_count

        if tasks_remaining <= 0:
            print("done")
            worker_count = worker_count - 1
            #if worker_count == 0: # Could remove this
            self.stop_event.set()

            sys.exit(0)

        input_files = get_next_files()
        if input_files is None:
            return

        mutex.acquire()
        input_files["worker_id"] = request.worker_name
        input_files["status"] = "running"
        input_files["started"] = str(time.time())
        tasks[input_files["task_id"]] = input_files  # Update task worker assignment and status
        mutex.release()

        input_data = read(input_files["path1"])

        return dist_processing_pb2.ImageReply(task_id=input_files["task_id"],
                                              image_name=input_files["filename1"], image=input_data[1])

    def ReturnImage(self, request, context):
        band64 = base64.b64decode(request.image)
        img = Image.open(io.BytesIO(band64))
        file_name = re.sub(r'\.tiff$', '', request.image_name)
        img.save('./processed/' + file_name + '.tiff')

        global tasks_remaining
        global tasks

        mutex.acquire()
        returned_task = tasks[request.task_id]
        returned_task["status"] = "completed"
        tasks_remaining = tasks_remaining - 1
        mutex.release()

        return dist_processing_pb2.ImageDone(done=True)


def check_parameters():
    if abs(UL_LAT - LR_LAT) > 0.1500000 or (abs(UL_LNG) - abs(LR_LNG)) > 0.35:
        print("Selected query area it too large")
        sys.exit(1)


def serve():
    stop_event = threading.Event()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dist_processing_pb2_grpc.add_ImageTransferServicer_to_server(ImageTransfer(stop_event), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    stop_event.wait()
    server.stop(1).wait()

if __name__ == '__main__':
    # logging.basicConfig()

    # The initial input test sites will be determined by an external request from the user interface
    # In the form of a request to the server and handled by another function listening for requests

    check_parameters()
    download_images()
    create_tasks(OUT_PATH, './tiles1/')
    print('setup complete')
    serve()

    convertToPNG("./processed/", "./output_png/")
    merge_tiles("./output_png/")
    #webbrowser.open('file:///Users/<add complete path here>/index.html')


