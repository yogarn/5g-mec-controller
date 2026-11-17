FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py detector.py gunicorn.conf.py ./
# Export beforehand; never download weights at startup.
COPY models/yolo11n-320.onnx models/yolo11n-320.onnx
ENV ORT_THREADS=1 OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 HEALTH_PORT=5001 HTTP_THREADS=5
EXPOSE 5000 5001
# HTTP uploads overlap; inference remains serialized within one model process.
CMD ["gunicorn", "--config", "gunicorn.conf.py", "app:create_app()"]
