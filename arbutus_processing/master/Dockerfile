# syntax=docker/dockerfile:1
# Right now this is just for running the master

FROM ubuntu:focal

WORKDIR /app

RUN mkdir -p /app/Output/
RUN mkdir -p /app/tiles1/
RUN mkdir -p /app/processed/
RUN mkdir -p /app/output_png/
RUN mkdir -p /app/output_merged/

RUN apt-get update
RUN apt-get install -y software-properties-common && apt-get update
RUN apt-get install -y python3
RUN apt-get install -y python3-pip
RUN add-apt-repository ppa:ubuntugis/ppa &&  apt-get update
RUN apt-get install -y gdal-bin libgdal-dev
ARG CPLUS_INCLUDE_PATH=/usr/include/gdal
ARG C_INCLUDE_PATH=/usr/include/gdal

COPY ./requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

EXPOSE 50051

COPY ["dist_processing_pb2.py", "dist_processing_pb2_grpc.py", "master.py", "config.py", "connection.py", "/app/"]
CMD ["python3", "master.py"]
