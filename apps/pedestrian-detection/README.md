# Pedestrian-detection preview

This is a separate, CPU-only workload for **COCO person detection** using YOLO11n
and ONNX Runtime. It returns person boxes; it does not classify walking, track
people, predict collisions, or implement a complete V2X warning service.

The preview has been imported from the isolated project snapshot into this repository. Do not sync, push an
image, apply manifests, run `run.sh check`, or start a campaign while the current
face-detection calibration is running. Even `--check` creates temporary cluster
resources and UE traffic. `--plan-only` is the offline exception.

## Interface and sizing

- `POST /detect`: multipart `image`, as used by the existing UE runner. JSON
  contains `person_count`, `persons` (`x,y,w,h,confidence` in source pixels),
  `processing_ms`, `model_sha256`, and `input_size`.
- `GET /health`: succeeds only after model loading and a warm-up inference.
- Fixed 320x320 FP32 model, RGB letterboxing, COCO class 0, confidence 0.25,
  NMS IoU 0.45. These are initial reproducible settings, not calibrated optima.
- Newly built images use one Gunicorn process with five HTTP threads by default
  (`HTTP_THREADS=5`, `gthread`). Five HTTP threads allow overlapping request I/O
  (uploads and decoding) rather than assigning one Pod per UE or matching 5 RPS
  one-for-one; the application lock still permits only one inference at a time,
  using one ORT inference thread (`ORT_THREADS=1`).
  The dedicated health listener remains independent of the request thread pool.
  `HTTP_THREADS=1` selects the previous synchronous single-request mode. Values
  must be positive integers. Calibrate HTTP concurrency separately from
  `ORT_THREADS`; five HTTP threads are an initial setting, not a measured optimum.
  Gunicorn's thread behavior is documented at
  https://gunicorn.org/reference/settings/#threads.
- Startup never downloads a model; missing/incompatible weights stop startup.
- Four-MiB request limit and 16-megapixel decoded-image limit.
- Resource requests are 500m CPU / 256Mi memory; limits are 1 CPU / 512Mi
  memory (twice the requests), matching the compatibility face manifest. These
  are experimental settings, not evidence of replica capacity or peak RAM needs.
- `processing_ms` covers handler-side decoding, preprocessing, inference wait,
  inference, and postprocessing. It excludes network and WSGI admission wait;
  **do not call client time minus processing_ms propagation delay**. That residual
  also contains transfer, queueing, and protocol overhead.

The checked-in digest-pinned manifests still select the existing image. Changing
this source does not update running Pods. Build and validate a new immutable image
before pinning it for a new experiment. Use the same image and `HTTP_THREADS` value
for baseline and proposed, record both in the run configuration, and keep earlier
single-thread results as a separate application configuration. Increasing HTTP
concurrency can increase decoding memory and CPU use; performance and memory
within the existing Pod limits still require measurement.

## Export locally

Keep export tooling separate from the runtime; PyTorch/Ultralytics are not in
the application image. Install CPU-only PyTorch and a compatible Ultralytics
export environment, then, from the project root:

```bash
python apps/pedestrian-detection/export_model.py --weights yolo11n.pt --size 320
```

An official model name downloads weights during this explicit export step.
For offline export, pass an existing checkpoint path. The script writes
`models/yolo11n-320.onnx` plus JSON with checkpoint/model hashes and tool versions.
The service intentionally accepts only fixed FP32, raw-output, 80-class COCO
exports (`nms=False`). Filtering to person happens after inference; it does not
make the backbone a single-class model. Keep Ultralytics/model licensing with
any later distributed artifact.

Reference: https://docs.ultralytics.com/modes/export/ and
https://onnxruntime.ai/docs/performance/tune-performance/threading.html.

## Images and local checks

No evaluation dataset is bundled. Choose licensed road scenes covering near/far
pedestrians, occlusion and negative examples; record labels for an accuracy check.
Prepare identical payloads for both orchestration methods:

```bash
python apps/pedestrian-detection/prepare_images.py \
  --source /path/to/licensed-road-scenes --output /tmp/pedestrian-images \
  --source-url DATASET_URL --license LICENSE_REFERENCE
python apps/pedestrian-detection/benchmark_local.py \
  --model apps/pedestrian-detection/models/yolo11n-320.onnx \
  --image /tmp/pedestrian-images/0000.jpg --output /tmp/pedestrian-local.json
```

Preparation caps the long side at 640 pixels and uses JPEG quality 85, recording
hashes, sizes, and provenance. Review small-person detection before selecting
these values. The local benchmark uses the in-process HTTP handler at one and
three closed-loop clients; it starts no server, UE, or Kubernetes resource.
It measures neither 5G latency nor quota-constrained Pod capacity.

## Offline campaign plan

The old default remains `face-detection`. The isolated runner adds
`--workload pedestrian-detection`; manifests, HPA target, readiness checks,
cleanup, and the resource sampler all follow that selection. Cleanup targets
only the selected workload. Use a separate campaign config and pedestrian image
directory; the pedestrian CLI rejects the face image path and requires an
explicit SLO.

Safe local planning example from the snapshot root:

```bash
python -m experiments.campaign --workload pedestrian-detection \
  --config experiments/campaign-pedestrian.example.yaml --slo-ms 100 \
  --max-replicas 10 --plan-only /tmp/pedestrian-offline-plan
```

This writes both method configurations without network access, cluster probes,
locks, traffic, or deployment. The preview manifest uses
`pedestrian-detection:preview` and `imagePullPolicy: Never`, so it cannot fetch
or replace the active face image. After the ongoing experiment finishes,
validate a new immutable image digest and remote dataset paths before any run.
The HPA CPU target inherited from the baseline is **provisional**, not a
pedestrian calibration result. Do not reuse face capacity tables.

The 100-ms threshold is a candidate experimental SLO. It is not a claim that
3GPP prescribes 100-ms HTTP image inference, or that this preview achieves it.
Future calibration must start with one UE/one replica, then three UEs (1:1:1),
and record request rate, resource use, readiness, detection correctness,
client response distributions, failures and SLO acceptance rate for both methods.

## Build and distribute

From the repository root, export the model as described above, then build:

```bash
docker build -t pedestrian-detection:preview apps/pedestrian-detection
```

Model binaries are present locally after this import but ignored by Git. Fresh
clones must export them first. With `imagePullPolicy: Never`, load the image into
the Kubernetes container runtime on every eligible node (`gnb1`, `gnb-2`,
`gnb-3`, `core-5g`). Building in Docker alone does not populate another node or
a separate containerd runtime. For containerd:

```bash
docker save pedestrian-detection:preview -o /tmp/pedestrian-detection.tar
# Transfer to each node, then run there:
sudo ctr -n k8s.io images import /tmp/pedestrian-detection.tar
```

Alternatively publish to the cluster-accessible registry, pin the verified digest
in the selected manifest and change its pull policy to `IfNotPresent`.
`pedestrian-workload.patch` is an archival copy; its changes are already imported.
