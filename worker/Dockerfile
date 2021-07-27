# syntax=docker/dockerfile:1
# Right now this is just for running the master

FROM python:buster

WORKDIR /app
CMD ["mkdir", "/app/Intermediate/"]
CMD ["mkdir", "/Intermediate/"]
RUN mkdir -p /app/Intermediate/
RUN mkdir -p  /Intermediate/

COPY ./requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY ["dist_processing_pb2.py", "dist_processing_pb2_grpc.py", "worker.py", "/app/"]
CMD ["python3", "worker.py"]