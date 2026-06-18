FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

ARG GIT_COMMIT=dev
ENV GIT_COMMIT=${GIT_COMMIT}
ENV DB_PATH=/app/data/news.db
ENV CONFIG_PATH=/app/config.yaml

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
