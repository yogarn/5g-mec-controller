import cv2
import numpy as np
from flask import Flask, jsonify, request

app = Flask(__name__)

FACE_CASCADE = cv2.CascadeClassifier(
    cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
)

@app.get("/health")
def health():
    return jsonify(status="ok")


@app.post("/detect")
def detect():
    if "image" not in request.files:
        return jsonify(error="missing 'image' file field"), 400

    data = request.files["image"].read()
    image = cv2.imdecode(np.frombuffer(data, np.uint8), cv2.IMREAD_COLOR)
    if image is None:
        return jsonify(error="could not decode image"), 400

    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    faces = FACE_CASCADE.detectMultiScale(
        gray, scaleFactor=1.1, minNeighbors=5, minSize=(30, 30)
    )

    return jsonify(
        face_count=len(faces),
        faces=[
            {"x": int(x), "y": int(y), "w": int(w), "h": int(h)}
            for (x, y, w, h) in faces
        ],
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=True)
