FROM python:3.7.4-slim-stretch

WORKDIR /usr/src/app

COPY . .
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT python syncopcuaclient.py