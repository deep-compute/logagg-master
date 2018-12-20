FROM ubuntu:16.04
RUN apt-get -y update
RUN apt-get -y upgrade

WORKDIR /logagg-master
ADD . /logagg-master
RUN apt-get -y install python-pip
RUN apt-get -y install python3-pip
RUN apt-get -y install git

RUN pip3 install .
RUN pip install nsq-api
