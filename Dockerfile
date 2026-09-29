# 
FROM python:3.14-trixie 
WORKDIR /opt/yaluk
# 
RUN apt-get update -y
RUN apt-get install -y librdkafka-dev

COPY requirements.txt /opt/yaluk/requirements.txt
RUN pip install --no-cache-dir -r /opt/yaluk/requirements.txt
# 
COPY ./src /opt/yaluk/bin
RUN python3 -m compileall -b /opt/yaluk/bin
RUN find . -name \*.py -type f -delete
# 
CMD ["python", "/opt/yaluk/bin/main.pyc"]
