# Minimal image for the async copy job container.
# Contains only the copy_worker module and its storage dependencies.

FROM docker.io/library/python:3.13-alpine

RUN pip install --no-cache-dir python-swiftclient

COPY pfcon/__init__.py /app/pfcon/__init__.py
COPY pfcon/copy_worker.py /app/pfcon/copy_worker.py
COPY pfcon/upload_worker.py /app/pfcon/upload_worker.py
COPY pfcon/storage /app/pfcon/storage

WORKDIR /app
