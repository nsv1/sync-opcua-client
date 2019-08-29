FROM python:3.7.4-slim-stretch

WORKDIR /usr/src/app

COPY . .
RUN pip install --no-cache-dir -r requirements.txt

# CMD ["bash"]
# CMD [ "python", "./syncopcuaclient.py" ]
ENTRYPOINT python syncopcuaclient.py