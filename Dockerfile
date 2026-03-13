FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN playwright install --with-deps chromium

COPY . .

CMD uvicorn main:app --host 0.0.0.0 --port $PORT
