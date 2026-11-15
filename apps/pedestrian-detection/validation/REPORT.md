# Local pedestrian preview validation

- Final suite: **217 passed, 68 subtests passed**.
- Real model: official Ultralytics YOLO11n checkpoint exported as fixed FP32 ONNX, 320x320, opset 17, no embedded NMS.
- ONNX/PyTorch raw-output comparison passed at rtol=0.001 and atol=0.001 on the same preprocessed image; maximum absolute difference 0.0009918212890625.
- Official upstream bus road-scene smoke image: four person detections, HTTP 200 on all 80 requests. This is a functionality check, not a labeled accuracy evaluation.
- Local in-process handler p90: 26.50 ms (20 requests, one client); 62.53 ms (60 requests, three clients). Both tiers observed 100% acceptance at 100 ms.
- These timings exclude 5G, HTTP networking, WSGI admission queues and Kubernetes CPU/memory limits; they do not establish UE SLO attainment or Pod capacity. No remote calibration was run.
- Gunicorn application configuration check passed. Container image build and cluster deployment were not performed.
- Offline planning succeeded; tests assert it does not invoke cluster commands, network probes or campaign locks.
- Shared source files in the original repository were byte-identical to the starting snapshot after implementation. No original repository, manuscript, Pod, service, HPA, UE session or running experiment was changed.
- Model hashes, smoke-image provenance, raw local request timings, parity output and source-isolation checks are saved alongside this report. The smoke image itself is not redistributed here.
- Runtime model is COCO person detection; tracking, walking classification and collision warning are outside this implementation.
