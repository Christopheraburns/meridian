FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app/src PORT=8080
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN mkdir -p /app/data
EXPOSE 8080
CMD ["python", "app.py"]
