FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir PyYAML==6.0.2
COPY collector /app/collector
EXPOSE 8080
CMD ["python", "-m", "collector.server", "--config", "/etc/ue-telemetry/config.yaml"]
