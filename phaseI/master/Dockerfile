# syntax=docker/dockerfile:1
# Right now this is just for running the master

FROM python:buster

WORKDIR /app
CMD ["mkdir", "/app/Output/"]
CMD ["mkdir", "/app/Output/tiles1/"]
CMD ["mkdir", "/app/Output/tiles2/"]
CMD ["mkdir", "/app/Output/processed/"]

COPY ./requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

EXPOSE 50051

COPY ["dist_processing_pb2.py", "dist_processing_pb2_grpc.py", "master.py", "T09UYT_20190320T192111_B03_20m.jp2", "T09UYT_20190320T192111_B11_20m.jp2", "/app/"]
CMD ["python3", "master.py"]