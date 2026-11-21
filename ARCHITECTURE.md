# Arsitektur Sistem 5G MEC Placement, Autoscaling & Vertical Offloading

Dokumen ini merangkum komponen aktif pada testbed 5G MEC (`core-5g`, `gnb1`, `gnb-2`, `gnb-3`) berdasarkan berkas kode dan konfigurasi aktual di repositori.

## Alur Data dan Bidang Kontrol

```text
UE (nr-ue / benchmark.client)
  │ HTTP POST /detect (per-site UPF) + laporan observasi latensi
  ▼
Envoy Ingress per-site (mec-envoy-gnb-{1,2,3}) ──(xDS RDS/EDS)──◄── Vertical Offloader (offloader/)
  │  LOCAL -> pod edge lokal                                              ▲
  │  SPREAD -> weighted edge + Core pool                                  │ HTTP /offload/snapshot (1 Hz)
  ▼                                                                       │
Aplikasi pedestrian-detection (Edge + Core)                   UE Telemetry Collector (collector/)
                                                                ▲  ▲  │ Prometheus scrape (/metrics)
                                         SMF/AMF session map ───┘  │  ▼
                                                              Prometheus + prometheus-adapter
                                                                │                │
                                             CPU & mobility ◄───┘                └──► External metric (ue_p90_latency_seconds)
                                                   │                                         │
                                                   ▼                                         ▼
                                    Go Placement Controller                      Kubernetes HPA (autoscaler/)
                                  (Scheduler + Descheduler)                      (CPU baseline vs Latency proposed)
```

## Tabel Komponen

| Komponen | Peran | Berkas Inti | Berkas Konfigurasi | Masukan | Keluaran | Titik Masuk |
|---|---|---|---|---|---|---|
| **Scheduler** | Menempatkan pod `Pending` pada node edge/core berdasarkan skor *resource headroom* (CPU & memori) + *traffic locality* (`pod_share_scoring`). | `placement/cmd/placement-controller/main.go`, `placement/internal/controller/controller.go`, `placement/internal/policy/policy.go`, `placement/internal/config/config.go`, `placement/internal/telemetry/prometheus.go` | `placement/deploy.yaml` (ConfigMap `placement-controller-config`), `config.yaml` | Watch pod/node Kubernetes; metrik Prometheus (`node_cpu_seconds_total`, `ue_request_upf_placement_window_attempts / 5`). | Kubernetes `pods/binding` (`schedulerName: mec-placement-scheduler`). | `placement/cmd/placement-controller/main.go` (Deployment `placement-controller` di namespace `placement`) |
| **Descheduler** | Mengevaluasi ulang penempatan pod berjalan secara berkala (30 s) dan mengevakuasi satu pod (*highest positive delta*) untuk direlokasi ke node berskor lebih tinggi. | `placement/internal/controller/controller.go`, `placement/internal/policy/policy.go` | `placement/deploy.yaml`, `config.yaml` (`deschedule_interval_seconds`, `locality_weight`, `resource_weights`, `pod_share_scoring`) | Snapshot pod `Running`, kapasitas node, utilitas CPU & laju kedatangan per-UPF dari Prometheus. | Kubernetes `policy/v1` `pods/eviction` (maks. 1 eviksi per siklus). | Loop descheduler di dalam biner `placement/cmd/placement-controller/main.go` |
| **Autoscaler** | Menyesuaikan jumlah replika workload: *baseline* berbasis utilitas CPU (target 80%) vs *proposed* berbasis latensi p90 eksternal (SLO 150 ms, lantai latensi 0,8×SLO, stabilisasi turun 60 s). | `autoscaler/config.py`, `experiments/manifests.py` (`_render_hpa`, `_floor_adapter_config`) | `autoscaler/autoscaler.yaml`, `autoscaler/hpa-baseline.yaml`, `autoscaler/hpa-proposed.yaml`, `telemetry/05-prometheus-adapter.yaml` | Metrik CPU `metrics-server` (baseline) atau metrik eksternal `ue_p90_latency_seconds` dari `prometheus-adapter` (proposed). | Manifest `autoscaling/v2` `HorizontalPodAutoscaler` & ConfigMap `prometheus-adapter` yang mengubah `spec.replicas` Deployment. | Kubernetes HPA controller (manifest dirender oleh `python3 -m experiments.campaign`) |
| **Collector** | Mengumpulkan observasi latensi/error per permintaan UE di memori, memetakan IP UE ke node gNB/UPF via Open5GS SMF/AMF, serta mengekspos metrik Prometheus dan snapshot offloading. | `collector/server.py` | `collector/config.yaml`, `collector/manifest.yaml` (ConfigMap `ue-telemetry-config`), `collector/prometheus-scrape.yaml` | HTTP `POST /observations` dari klien beban (ber-token Bearer); HTTP `GET` periodik ke SMF (`/pdu-info`) & AMF (`/ue-info`). | Endpoint Prometheus `/metrics` (p90 jendela 15 s, placement window 5 s); JSON `/offload/snapshot` per-site; `/health`. | `python3 collector/server.py` (Deployment `ue-telemetry` di namespace `placement`, NodePort `31669`) |
| **Offloader** | Kontroler *vertical offloading* sisi-Core: memantau p90 & *error rate* per-site (1 Hz), memicu transisi `LOCAL -> SPREAD` saat melanggar SLO, dan melakukan *gradual retreat* (membagi dua porsi Core tiap langkah hingga < 1%) lewat Envoy xDS. | `offloader/server.py`, `offloader/policy.py`, `offloader/state.py`, `offloader/spread.py`, `offloader/entry.py`, `offloader/retreat.py`, `offloader/xds.py`, `offloader/k8s.py`, `offloader/probe.py`, `offloader/budget.py` | `offloader/config.yaml`, `offloader/manifest.yaml`, `offloader/envoy-ingress.yaml`, `offloader/core-backend.yaml` | Snapshot collector (`/offload/snapshot`); endpoint pod ready dari Kubernetes API; ACK/NACK gRPC xDS dari Envoy. | Konfigurasi xDS RDS (rute `LOCAL`/`SPREAD`) & EDS (bobot pod lokal) ke Envoy; API admin (`:8086`); log keputusan JSONL. | `python3 -m offloader.server` (Deployment di namespace `placement`, port xDS `8085`, admin `8086`) |
| **Aplikasi** | Layanan deteksi pejalan kaki berbasis ONNX (YOLO11n 320×320) di atas Gunicorn/Flask pada CPU-only; berjalan sebagai pod edge (`mec-placement-pedestrian-detection`) dan pool Core (`...-core`). | `apps/pedestrian-detection/app.py`, `apps/pedestrian-detection/detector.py`, `apps/pedestrian-detection/gunicorn.conf.py` | `apps/pedestrian-detection/manifest.yaml`, `offloader/core-backend.yaml` | Permintaan HTTP `POST /detect` berisi citra JPEG/PNG (`bus.jpg` dsb.). | Respons JSON daftar deteksi + header `X-MEC-Pod` & `X-MEC-Node` untuk pelacakan rute lokal vs Core. | Gunicorn WSGI di kontainer `apps/pedestrian-detection/Dockerfile` (port `5000`) |
| **Klien Beban** | Generator beban HTTP *closed-loop* per-UE melalui antarmuka TUN UERANSIM (`nr-ue`), mengeksekusi jadwal fase mobilitas/beban dan mengirim telemetri percobaan ke collector. | `benchmark/client.py`, `benchmark/telemetry.py`, `utils/experiment.py`, `utils/ue_lifecycle.py`, `utils/continuous_traffic.py` | `experiments/campaign-vertical.yaml`, `experiments/campaign-mobility-core.yaml`, inventaris UE (`~/.config/5g-mec/ue-inventory.yaml`) | Jadwal fase (`N:g1:g2:g3`), laju RPS/UE, batas *in-flight*, daftar URL site, citra uji, token telemetri. | Log mentah JSONL (`phase_start`, `request`, `phase_end`) dan pengiriman observasi HTTP ke collector. | `python3 -m benchmark.client` / `python3 -m utils.experiment` (dipanggil jarak jauh via SSH oleh `experiments/campaign.py`) |
| **Orkestrator Kampanye** | Menjalankan eksperimen terkontrol multi-arm (*baseline* vs *proposed*, *local-only* vs *vertical*), preflight kluster, `tc` delay, transfer log, ringkasan, dan plotting. | `experiments/campaign.py`, `experiments/profiles.py`, `experiments/manifests.py`, `experiments/cli.py`, `experiments/vertical.py`, `utils/traffic_control.py`, `utils/vertical_summary.py`, `utils/plot_vertical.py`, `utils/plot_autoscaler.py` | `experiments/campaign-vertical.yaml`, `experiments/campaign-mobility-core.yaml`, `autoscaler/autoscaler.yaml`, `config.yaml` | Nama profil (`vertical-joint`, `vertical-smoke`, `vertical-mobility`, `scaling-staircase`) dan opsi CLI. | Artefak kampanye di `results/<name>/` (plan YAML, manifest, JSONL permintaan, log offloader, ringkasan CSV, grafik PNG). | `run-vertical-{full,smoke,mobility,suite}.sh` -> `experiments/vertical_campaign.sh` -> `python3 -m experiments.campaign --profile <p>` |

## Parameter Bersama Lintas Komponen

Konsistensi nilai bersama diverifikasi secara otomatis oleh `experiments/test_config_consistency.py`:

| Parameter | Nilai | Komponen yang Berbagi |
|---|---|---|
| Target SLO p90 | `150 ms` (`0.15 s`) | `collector/config.yaml`, `collector/manifest.yaml`, `offloader/config.yaml`, `config.yaml`, `autoscaler/autoscaler.yaml`, `autoscaler/hpa-proposed.yaml`, `utils/defaults.py` |
| Jendela p90 & offloading | `15 s` | `collector/config.yaml` (`window_seconds`, `offload_window_seconds`), `offloader/config.yaml` (`min_dwell_seconds`) |
| Batas *stale* telemetri | `5 s` | `collector/config.yaml` (`offload_stale_seconds`), `offloader/config.yaml` (`stale_after_seconds`) |
| Jendela *traffic-share* placement | `5 s` | `collector/config.yaml` (`placement_window_seconds`), pembagi `/ 5` di `mobility_query` pada `config.yaml` & `placement/deploy.yaml` |
| Interval descheduler | `30 s` | `config.yaml`, `placement/deploy.yaml`, `experiments/profiles.py` (`DEFAULT_DESCHEDULE_INTERVAL_S`) |
| Target CPU HPA baseline | `80%` | `autoscaler/autoscaler.yaml`, `autoscaler/hpa-baseline.yaml` |
| Pemetaan site -> node | `gnb-1: gnb1`, `gnb-2: gnb-2`, `gnb-3: gnb-3`, `core: core-5g` | `collector/config.yaml`, `collector/manifest.yaml`, `offloader/config.yaml`, `experiments/profiles.py` |
