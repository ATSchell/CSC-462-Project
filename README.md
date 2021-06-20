# CSC-462-Project
Group project for CSC 462, distributed computing course at UVic

### Built with Python, gRPC, rasterio

### Architecture
- Intial tiling of input files is sequential

- Processing of tiles NDVI/NDSI is done in parallel

- Since I didn't set up a distributed file system and shared managed drives that can have more than 2 simultaneous connections are large and expensive I used base64 encoding and decoding to store images inside gRPC messages

- Can run on Azure free Ubuntu VMs but takes over 35 minutes to complete with 1 master, 2 workers and 2 input Sentinel-2 files to calculate NDVI/NDSI

- Currently writing to files with process PID that worked on it appended before extension for debugging

- NotGeoreferencedWarning appears in worker process, this might be an issue with the current tiling function

#### Notes:
- Requires Python3 and modules grpcio, grpcio-tools, Pillow and rasterio to be installed
- Requires /Output/results directory on machine running worker.py and /ndsi_output/ and /Output/tiles1 and /Output/tiles2 directories on machine running master.py
- IP address can be changed from localhost to run on servers, I used internal 10. IP addresses and all my machines were on the same Azure vnet
- Protocol Buffers are compiled with

```python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. dist_processing.proto```

