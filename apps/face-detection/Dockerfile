FROM python:3.11-slim

RUN pip install --no-cache-dir flask "opencv-python-headless<5" numpy

WORKDIR /app
COPY app.py .

EXPOSE 5000
CMD ["python", "app.py"]
