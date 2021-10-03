FROM python:3.7-buster

# To install or upgrade packages
RUN apt-get update && apt-get install -y telnet

COPY requirements.txt requirements.txt
RUN python -m venv venv
RUN venv/bin/pip install -r requirements.txt

COPY tt_datastore tt_datastore
COPY tests tests
