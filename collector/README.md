# UE Observation Collector (`collector/`)

## Peran

`collector/` adalah layanan telemetri in-memory satu replika yang menerima observasi permintaan UE dan probe diagnostik (`POST /observations`), menyimpannya dalam jendela waktu geser (*sliding window*), menggabungkannya dengan pemetaan sesi aktif UE-ke-node dari Open5GS SMF dan AMF, lalu menyediakan snapshot per situs untuk `offloader` (`/offload/snapshot`) serta metrik Prometheus untuk autoscaler dan controller penempatan (`/metrics`, `/metrics/placement`, `/sessions`).

## Alur Data

```text
  benchmark/client.py (user)  ----> POST /observations (Bearer token)
                                           |
                                           v
  Open5GS SMF/AMF --(1s poll)--> Collector (sliding-window deque + mapping)
                                     |           |                |
                                     v           v                v
                            /offload/snapshot  /metrics    /metrics/placement
                             (offloader 1 Hz) (Prometheus)  (Go placement ctrl)
```

## Peta Berkas dan Fungsi Publik

| Berkas | Peran (1 kalimat) | Fungsi / Kelas Utama | Dipanggil oleh |
| --- | --- | --- | --- |
| `collector/server.py` | Penyimpan jendela waktu in-memory, HTTP handler, dan titik masuk CLI `python -m collector.server`. | `Collector` (`ingest`, `refresh`, `evict_expired`, `offload_snapshot`, `sessions_snapshot`, `placement_metrics`, `metrics`), `make_handler`, `main` | `Dockerfile`, `collector/test_server.py`, `offloader/test_offloader.py`, `offloader/test_equivalence.py` |
| `collector/config.py` | Validasi skema dan pemuatan konfigurasi YAML collector. | `validate_settings`, `load_settings`, `is_valid_node_name`, `DEFAULT_SETTINGS`, `VALID_SITES`, `VALID_ROUTES` | `collector/server.py`, `collector/buckets.py`, `collector/metrics.py` |
| `collector/sessions.py` | Polling endpoint Open5GS SMF/AMF dan pemetaan IP UE ke node gNB serta node UPF. | `fetch`, `parse_upf_host`, `map_upf_sessions`, `join_sessions` | `collector/server.py` |
| `collector/buckets.py` | Agregasi bucket per situs/rute (`p90_ms`, `attempts`, `errors`, `timeouts`, kesegaran) untuk `/offload/snapshot` dan `/sessions`. | `summarize_bucket`, `build_offload_snapshot`, `build_sessions_snapshot` | `collector/server.py` |
| `collector/metrics.py` | Formatter teks Prometheus untuk `/metrics` dan `/metrics/placement`. | `render_metrics`, `render_placement_metrics` | `collector/server.py` |

## Tabel Endpoint HTTP (port 8080)

| Path | Metode | Masukan | Keluaran | Konsumen Aktif |
| --- | --- | --- | --- | --- |
| `/observations` | `POST` | Header `Authorization: Bearer <token>`, JSON observasi (`ue_ip`, `duration_ms`, `http_status`, `success`, opsional `site`, `route`, `server_node`, `timed_out`, `traffic_class`) | `202` diterima, `400` tidak valid, `401` token salah | `benchmark/client.py` |
| `/offload/snapshot` | `GET` | — | JSON snapshot per situs (`user.{local,core,unknown,overall}`, `probe.local`, `generated_at`, `window_seconds`) | `offloader/server.py` |
| `/metrics` | `GET` | — | Teks Prometheus (`ue_request_duration_p90_seconds`, `ue_site_route_*`, `ue_active_sessions`, counter ingest) | `telemetry/03-prometheus.yaml`, `autoscaler/` (HPA) |
| `/metrics/placement` | `GET` | — | Teks Prometheus ringan (`ue_request_node_placement_window_attempts`, `ue_request_upf_placement_window_*`) | `telemetry/03-prometheus.yaml`, `placement/` (Go controller) |
| `/sessions` | `GET` | — | JSON jumlah sesi aktif per node gNB, usia refresh, dan status `stale` | `utils/preflight.py`, `utils/ue_lifecycle.py` |
| `/health` | `GET` | — | `200 ok` | Kubernetes liveness/readiness probe |

## Kunci Konfigurasi (`collector/config.yaml`)

| Kunci | Nilai Default / Contoh | Arti |
| --- | --- | --- |
| `window_seconds` | `15` | Jendela waktu geser utama (detik) untuk metrik p90 dan rate keseluruhan. |
| `placement_window_seconds` | `5` | Jendela waktu geser (detik) untuk `/metrics/placement` (<= `window_seconds`). |
| `offload_window_seconds` | `15` | Jendela waktu geser (detik) untuk `/offload/snapshot` (<= `window_seconds`). |
| `offload_stale_seconds` | `5.0` | Batas usia sampel terakhir sebelum bucket ditandai `stale`. |
| `max_observations` | `100000` | Batas atas jumlah observasi yang disimpan di memori (FIFO eviction). |
| `mapping_refresh_seconds` | `1` | Interval polling SMF/AMF (detik). |
| `mapping_max_age_seconds` | `5` | Batas usia cache pemetaan SMF/AMF sebelum dianggap basi. |
| `slo_seconds` | `0.15` | Ambang SLO latensi (detik) untuk `ue_request_window_success_within_slo`. |
| `smf_url`, `amf_url` | URL HTTP Open5GS | Endpoint JSON sesi PDU SMF dan attachment UE AMF. |
| `core_node`, `site_nodes`, `gnb_nodes`, `upf_nodes` | Pemetaan node K8s | Pemetaan nama node Core, situs logis `gnb-1..3`, ID gNB AMF, dan IP UPF N3. |
