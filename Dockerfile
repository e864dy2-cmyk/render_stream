FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN sed -i 's/hydrogram==0.2.1/hydrogram==0.2.0/g' requirements.txt && pip install --no-cache-dir -r requirements.txt

COPY main.py .

CMD uvicorn main:app --host 0.0.0.0 --port $PORT
