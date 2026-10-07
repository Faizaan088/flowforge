FROM python:3.11-slim

WORKDIR /app

# copy requirements first so docker caches the layer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .