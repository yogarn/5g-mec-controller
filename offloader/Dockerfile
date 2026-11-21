FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir pyyaml
COPY offloader /app/offloader
COPY apps/pedestrian-detection/images/bus.jpg /app/apps/pedestrian-detection/images/bus.jpg
ENTRYPOINT ["python", "-u", "-m", "offloader.server"]
CMD ["--config", "/etc/vertical-offloader/config.yaml"]
