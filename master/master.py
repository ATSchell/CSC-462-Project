#import pprint
import re
import sys
from concurrent import futures
import logging
import io
import os
from threading import Lock

import grpc
from PIL import Image
import base64
from itertools import product
from rasterio import windows
import rasterio as rio

import dist_processing_pb2
import dist_processing_pb2_grpc

mutex = Lock()
tasks = []
tasks_remaining = 0


# This tiling function is based on this
# https://gis.stackexchange.com/questions/285499/how-to-split-multiband-image-into-image-tiles-using-rasterio
# JPEG 2000 Chosen as intermediary format since it is easier to preserve attributes and is smaller
def tiling(input_file_path, output_directory):
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

        meta = inds.meta.copy()

        for window, transform in get_tiles(inds):
            # print(window)
            meta['transform'] = transform
            meta['width'], meta['height'] = window.width, window.height
            outpath = os.path.join(output_directory, 'tile_{}-{}.jp2'.format(int(window.col_off), int(window.row_off)))
            with rio.open(outpath, 'w', **meta) as outds:
                outds.write(inds.read(window=window))

# This function assigns each set of image tile pairs from each layer as a task
def create_tasks(input_file1, input_file2, directory1, directory2):
    tiling(input_file1, directory1)
    tiling(input_file2, directory2)

    global tasks
    for filename1, filename2 in zip(os.listdir(directory1), os.listdir(directory2)):
        if filename1.endswith(".jp2"):
            new_dict = {
                "task_id": len(tasks),
                "worker_id": "unassigned",
                "filename1": filename1,
                "filename2": filename2,
                "path1": os.path.join(directory1, filename1),
                "path2": os.path.join(directory2, filename2),
                "status": "waiting"
            }
            tasks.append(new_dict)
            continue
        else:
            continue

    global tasks_remaining
    tasks_remaining = len(tasks)
    # pprint.pprint(tasks)
    # print(tasks_remaining)

# This function loads in JPEG 2000 images to memory
# Band 2 will be subtracted from band 1
def read(band1, band2):
    band = Image.open(band1)
    buf = io.BytesIO()
    band.save(buf, 'JPEG2000')
    buf.seek(0)
    image_bytes = buf.read()
    buf.close()
    band64 = base64.b64encode(image_bytes)

    band = Image.open(band2)
    buf = io.BytesIO()
    band.save(buf, 'JPEG2000')
    buf.seek(0)
    image_bytes = buf.read()
    buf.close()
    band65 = base64.b64encode(image_bytes)

    # Need to strip band1 name from path information and band number for unified output file
    return band1, band64, band65


def get_next_files():
    global tasks_remaining
    global tasks

    rem_tasks = tasks_remaining

    if tasks_remaining > 0 and rem_tasks > 0:
        for job in tasks:
            #temp_task = tasks[rem_tasks - 1]
            #if temp_task["status"] == "running" or temp_task["worker_id"] != "unassigned":
                #rem_tasks = rem_tasks - 1
                #continue
            #else:
                #return tasks[rem_tasks - 1]
            if job["status"] == "waiting":
                return job
    return

# gRPC Handler
class ImageTransfer(dist_processing_pb2_grpc.ImageTransferServicer):

    def RequestImage(self, request, context):
        global tasks
        global tasks_remaining

        if tasks_remaining <= 0:
            print("done")
            sys.exit(0)

        input_files = get_next_files()
        if input_files is None:
            return

        mutex.acquire()
        input_files["worker_id"] = request.worker_name
        input_files["status"] = "running"
        tasks[input_files["task_id"]] = input_files  # Update task worker assignment and status
        mutex.release()

        input_data = read(input_files["path1"], input_files["path2"])
        return dist_processing_pb2.ImageReply(task_id=input_files["task_id"], image_name=input_files["filename1"],
                                              image=input_data[1], image2=input_data[2])

    def ReturnImage(self, request, context):
        band64 = base64.b64decode(request.image)
        img = Image.open(io.BytesIO(band64))
        file_name = re.sub(r'\.jp2$', '', request.image_name)
        img.save('./Output/processed/' + file_name + '.tiff')

        global tasks_remaining
        global tasks

        mutex.acquire()
        returned_task = tasks[request.task_id]
        returned_task["status"] = "completed"
        mutex.release()

        tasks_remaining = tasks_remaining - 1


        # Need to decrement total tasks and change status to completed here
        return dist_processing_pb2.ImageDone(done=True)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dist_processing_pb2_grpc.add_ImageTransferServicer_to_server(ImageTransfer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    #logging.basicConfig()

    # The initial input test sites will be determined by an external request from the user interface
    # In the form of a request to the server and handled by another function listening for requests
    create_tasks('T09UYT_20190320T192111_B03_20m.jp2', 'T09UYT_20190320T192111_B11_20m.jp2',
                 './Output/tiles1/', './Output/tiles2/')
    print('setup complete')
    serve()
