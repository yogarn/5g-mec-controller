#!/usr/bin/env bash
# ==============================================================================
# CLEANUP-DELETE-PLAN.sh — RENCANA PENGHAPUSAN BERKAS & USULAN PEMBERSIHAN YAML
# ==============================================================================
# PERINGATAN:
#   - Skrip ini HANYA RENCANA. JANGAN jalankan sebelum pemilik meninjau setiap
#     kelompok dan mengonfirmasi bahwa tidak ada eksperimen/reproduksi yang
#     masih bergantung pada berkas tersebut.
#   - Jalankan hanya setelah seluruh suite eksperimen di `core` selesai.
#   - Rujukan aktif: 3 tahap pada `run-vertical-suite.sh` (joint, scaling,
#     mobility) -> `run-vertical-full.sh` & `experiments/run.sh` ->
#     `experiments/campaign.py`.
# ==============================================================================
set -euo pipefail

# ------------------------------------------------------------------------------
# KELOMPOK 1: Workload lama face-detection
# Tingkat keyakinan: PASTI (untuk jalur suite; simpan hanya jika butuh arsip)
# Bukti tidak dipakai suite:
#   - `rg -n 'face-detection' run-vertical-suite.sh run-vertical-full.sh experiments/campaign-vertical.yaml experiments/campaign-mobility-core.yaml` -> 0 hasil.
#   - `experiments/campaign.py` kini hanya mendukung WORKLOADS=('pedestrian-detection',).
#   - Catatan: beberapa utilitas/tes warisan masih merujuk nama default
#     `apps/face-detection` (`utils/defaults.py:15`, `utils/fixed_replica_harness.py:52`,
#     `utils/rebuild_pedestrian.py:19`, `experiments/test_campaign.py:49`); perbarui
#     referensi tersebut sebelum menjalankan `git rm`.
#   - Catatan: `apps/face-detection/envoy-gnb2.yaml` sudah tidak ada di working
#     tree (terhapus pada perubahan belum di-commit sebelumnya).
# ------------------------------------------------------------------------------
git rm -r apps/face-detection

# ------------------------------------------------------------------------------
# KELOMPOK 2: Exporter GPU (DCGM)
# Tingkat keyakinan: PERLU KONFIRMASI PEMILIK
# Bukti tidak dipakai suite:
#   - `rg -n '02-dcgm-exporter|DCGM_FI_DEV' run-vertical-suite.sh run-vertical-full.sh experiments/ placement/` -> 0 hasil aktif (bobot gpu=0, vram=0; query GPU/VRAM dihapus dari Go controller & config.yaml).
#   - Alasan perlu konfirmasi: dashboard Grafana (`telemetry/04-grafana.yaml:340-629`)
#     dan recording rules Prometheus (`telemetry/03-prometheus.yaml:97-145`) masih
#     membaca metrik DCGM, dan kluster bisa dipakai eksperimen GPU lain.
# ------------------------------------------------------------------------------
git rm telemetry/02-dcgm-exporter.yaml

# ------------------------------------------------------------------------------
# KELOMPOK 3: Standalone vertical scaler (`offloader/scaler.py`)
# Tingkat keyakinan: PERLU KONFIRMASI PEMILIK
# Bukti tidak dipakai suite:
#   - `rg -n 'offloader\.scaler|VerticalDynamicScaler|account_edge_pods' experiments/ run-*.sh` -> 0 hasil.
#   - Satu-satunya referensi di luar `offloader/scaler.py` adalah unit test
#     `offloader/test_offloader.py:19,755-798`.
#   - Alasan perlu konfirmasi: menghapus `offloader/scaler.py` mengharuskan
#     menghapus tes terkait di `offloader/test_offloader.py` dan menghilangkan
#     CLI standalone scaler (bila tidak lagi dibutuhkan untuk operasi manual).
# ------------------------------------------------------------------------------
# (Hapus impor & test scaler di offloader/test_offloader.py terlebih dahulu)
git rm offloader/scaler.py

# ------------------------------------------------------------------------------
# KELOMPOK 4: Skrip wrapper `run-*.sh` yang tidak dipanggil `run-vertical-suite.sh`
# Tingkat keyakinan: PASTI (untuk `run-vertical-suite.sh`: hanya memanggil
#   `run-vertical-full.sh` dan `experiments/run.sh`) / PERLU KONFIRMASI PEMILIK
#   (bila masih dipakai untuk reproduksi/smoke manual).
# Bukti tidak dipakai suite:
#   - `run-vertical-suite.sh:29-39` hanya memanggil `run-vertical-full.sh` dan
#     `experiments/run.sh`.
#   - Seluruh skrip di bawah tidak dirujuk oleh `run-vertical-suite.sh` maupun
#     `run-vertical-full.sh`.
# ------------------------------------------------------------------------------
git rm \
  run-experiments-repeat.sh \
  run-experiments-steep.sh \
  run-final-mobility.sh \
  run-long-suite.sh \
  run-mobility-cluster-wide.sh \
  run-mobility-default.sh \
  run-mobility-hysteresis-smoke.sh \
  run-smoke-suite.sh \
  run-steep-fixed-move.sh \
  run-steep-session-probe.sh \
  run-vertical-smoke.sh

# ------------------------------------------------------------------------------
# KELOMPOK 5: Smoke campaign helper (`utils/smoke_campaign.py` & tes terkait)
# Tingkat keyakinan: PERLU KONFIRMASI PEMILIK
# Bukti tidak dipakai suite:
#   - `rg -n 'smoke_campaign' run-vertical-suite.sh run-vertical-full.sh experiments/campaign.py` -> 0 hasil.
#   - Hanya dipanggil oleh subcommand `experiments/run.sh:39-42` (`smoke`),
#     diuji di `utils/test_smoke_campaign.py:12` dan `utils/test_rebuild_pedestrian.py:84`.
#   - Alasan perlu konfirmasi: berguna untuk smoke test cepat sebelum kampanye;
#     jika dihapus, hapus juga branch `smoke` di `experiments/run.sh:39-42` dan
#     kasus uji di `utils/test_rebuild_pedestrian.py:84`.
# ------------------------------------------------------------------------------
git rm utils/smoke_campaign.py utils/test_smoke_campaign.py

# ------------------------------------------------------------------------------
# KELOMPOK 6: Profil `--debug` di `experiments/campaign.py`
# Tingkat keyakinan: TIDAK ADA BERKAS TERPISAH (flag inline di campaign.py)
# Bukti:
#   - `run-vertical-suite.sh` dan `run-vertical-full.sh` tidak meneruskan `--debug`.
#   - Tidak ada file konfigurasi terpisah untuk profil debug; seluruhnya berupa
#     flag `--debug` di `experiments/campaign.py` (diuji di `experiments/test_campaign.py`).
#   - Tidak ada perintah `git rm` untuk kelompok ini.
# ------------------------------------------------------------------------------

# ==============================================================================
# USULAN PERUBAHAN YAML (BUKAN PENGHAPUSAN BERKAS): telemetry/03-prometheus.yaml
# ==============================================================================
# Tingkat keyakinan: PERLU KONFIRMASI PEMILIK (berbagi Prometheus kluster;
#   terapkan hanya setelah suite selesai dan konfirmasi tidak ada beban GPU/vLLM).
#
# 1. Hapus recording rules GPU/VRAM (telemetry/03-prometheus.yaml:97-145):
#      - node:gpu_utilization:percent, node:gpu_utilization:percent:avg15s
#      - node:cuda_cores:total, node:cuda_cores:used, node:cuda_cores:used:avg15s
#      - node:vram_bytes:total/used, node:vram_gb:total/used/avg15s,
#        node:vram_utilization:percent, node:vram_utilization:percent:avg15s
#    (Catatan: panel GPU/VRAM di telemetry/04-grafana.yaml:340-629 akan kosong.)
#
# 2. Hapus recording rules vLLM TTFT (telemetry/03-prometheus.yaml:146-182):
#      - upf:ttft_ms:p90, upf:ttft_ms:p50, upf:ttft_ms:mean
#    (Tidak dipakai oleh suite pedestrian maupun telemetry/04-grafana.yaml.)
#
# 3. Hapus scrape job `dcgm-exporter` (telemetry/03-prometheus.yaml:215-233)
#    dan scrape job `vllm-edge` (telemetry/03-prometheus.yaml:235-254).
#    Catatan: job `upf-envoy` sudah tidak ada di telemetry/03-prometheus.yaml.
#
# 4. Kecualikan node `ueransim` yang timeout pada job `node-exporter`
#    (telemetry/03-prometheus.yaml:194-213) dengan menambahkan relabel rule:
#      - source_labels: [__meta_kubernetes_pod_node_name]
#        regex: ueransim
#        action: drop
# ==============================================================================
