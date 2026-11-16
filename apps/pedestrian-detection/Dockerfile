FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py detector.py ./
# Export beforehand; never download weights at startup.
COPY models/yolo11n-320.onnx models/yolo11n-320.onnx
ENV ORT_THREADS=1 OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 HEALTH_PORT=5001
EXPOSE 5000 5001
# One model and one inference request per Pod; replicas supply parallelism.
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "1", "--threads", "1", "--timeout", "60", "app:create_app()"]
