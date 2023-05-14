FROM python:3.11-alpine

COPY . /exporter

WORKDIR /exporter

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "-u", "solarman.py"]