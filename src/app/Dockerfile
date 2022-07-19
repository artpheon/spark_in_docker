FROM ubuntu:latest

RUN apt update && apt install -y python3-pip

RUN pip install kafka-python

RUN mkdir -p /home

COPY ./src/* /home/

ENTRYPOINT ["/bin/bash"]