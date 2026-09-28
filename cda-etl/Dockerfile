FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src/

ENV PYTHONPATH=/app/src/cda_etl:/app/src

CMD ["python", "src/cda_etl/main.py"]
