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

COPY . /app
CMD ["python3", "master.py"]