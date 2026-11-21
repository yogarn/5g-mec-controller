# Cleanup Proposal (P0 Inventory)

Audit keterjangkauan (reachability) statis dari seluruh titik masuk aktif repositori:
- Skrip eksperimen aktif: `run-vertical-full.sh`, `run-vertical-smoke.sh`, `run-vertical-mobility.sh`, `run-vertical-suite.sh`, `experiments/vertical_campaign.sh`, `python3 -m experiments.campaign --profile {vertical-joint,vertical-smoke,vertical-mobility,scaling-staircase}`
- Layanan runtime aktif: `offloader/server.py`, `collector/server.py`, `placement/cmd/placement-controller/main.go`, `apps/pedestrian-detection/app.py`
- Manifest YAML aktif: `config.yaml`, `placement/deploy.yaml`, `offloader/{config,manifest,envoy-ingress,core-backend}.yaml`, `collector/{config,manifest,prometheus-scrape}.yaml`, `autoscaler/{hpa-baseline,hpa-proposed}.yaml`, `apps/pedestrian-detection/manifest.yaml`, `telemetry/{00-namespace,01-node-exporter,03-prometheus,04-grafana,05-prometheus-adapter}.yaml`, `open5gs/user-plane/upf-gnb-{2,3}.yaml`

> **Catatan**: Dokumen ini hanya berisi inventarisasi dan rekomendasi. **Tidak ada berkas yang dihapus.**

---

## 1. Berkas Tidak Terjangkau / Kandidat Pembersihan

### 1.1 Skrip Root Non-Vertical & Otomasi Lama

| Jalur | Status Terjangkau | Bukti | Rekomendasi |
|---|---|---|---|
| `CLEANUP-DELETE-PLAN.sh` | Tidak terjangkau | Tidak dirujuk skrip aktif; daftar rencana hapus lama | Hapus (setelah konsolidasi dengan dokumen ini) |
| `WATCHDOG.yml` | Tidak terjangkau | Tidak dirujuk; menyebut `openai-codex/gpt-6.1-terra:low` | Hapus |
| `run-experiments-repeat.sh` | Tidak terjangkau | Hanya dirujuk `CLEANUP-DELETE-PLAN.sh` | Arsip / Hapus |
| `run-experiments-steep.sh` | Tidak terjangkau | Hanya dirujuk `CLEANUP-DELETE-PLAN.sh` | Arsip / Hapus |
| `run-final-mobility.sh` | Tidak terjangkau | Hanya dirujuk `CLEANUP-DELETE-PLAN.sh` | Arsip / Hapus |
| `run-long-suite.sh` | Tidak terjangkau | Hanya dirujuk `CLEANUP-DELETE-PLAN.sh` | Arsip / Hapus |
| `run-mobility-cluster-wide.sh` | Tidak terjangkau | Hanya dirujuk `run-mobility-default.sh` & `CLEANUP-DELETE-PLAN.sh` | Arsip / Hapus |
| `run-mobility-default.sh` | Tidak terjangkau | Hanya dirujuk `CLEANUP-DELETE-PLAN.sh` | Arsip / Hapus |
| `run-mobility-hysteresis-smoke.sh` | Tidak terjangkau | Hanya dirujuk `CLEANUP-DELETE-PLAN.sh`; memanggil `utils.redeploy_placement` | Arsip / Hapus |
| `run-smoke-suite.sh` | Tidak terjangkau | Hanya dirujuk `CLEANUP-DELETE-PLAN.sh` | Arsip / Hapus |
| `run-steep-fixed-move.sh` | Tidak terjangkau | Hanya dirujuk `run-steep-session-probe.sh` & `CLEANUP-DELETE-PLAN.sh` | Arsip / Hapus |
| `run-steep-session-probe.sh` | Tidak terjangkau | Hanya dirujuk `CLEANUP-DELETE-PLAN.sh`; memanggil `collector/probe_sessions.py` | Arsip / Hapus |
| `experiments/matrix-commands.sh` | Tidak terjangkau | Tidak dirujuk; daftar perintah lama ber-komentar | Arsip / Hapus |

### 1.2 Dokumen Handoff, Runbook, Patch, dan Catatan Operasional Lama

| Jalur | Status Terjangkau | Bukti | Rekomendasi |
|---|---|---|---|
| `CODEX-HANDOFF.md` | Tidak terjangkau | Catatan handoff antar agen | Arsip / Hapus |
| `AGY-EXPERIMENT-RUNBOOK.md` | Tidak terjangkau | Runbook sesi agen lama | Arsip / Hapus |
| `DELEGATION-WORKPLAN.md` | Tidak terjangkau | Rencana delegasi sesi lama | Arsip / Hapus |
| `CONFIG-SIMPLIFICATION.md` | Tidak terjangkau | Catatan desain penyederhanaan konfigurasi | Arsip / Hapus |
| `GNB1-TC-FAILURE-DIAGNOSIS.md` | Tidak terjangkau | Catatan diagnosis insiden `tc` gnb1 | Arsip (berguna sebagai catatan lab) |
| `PEDESTRIAN-PREVIEW.md` | Tidak terjangkau | Catatan preview migrasi pedestrian-detection | Arsip / Hapus |
| `pedestrian-workload.patch` | Tidak terjangkau | Berkas patch 168 KB yang sudah diterapkan ke pohon kerja | Hapus |
| `architecture.mmd` | Tidak terjangkau | Diagram Mermaid manual lama | Pertahankan atau gantikan dengan `ARCHITECTURE.md` |

### 1.3 Kode Python & YAML Tidak Terjangkau atau Hanya Diuji

| Jalur | Status Terjangkau | Bukti | Rekomendasi |
|---|---|---|---|
| `offloader/scaler.py` | Hanya diuji (`TEST_ONLY`) | 353 baris; hanya diimpor `offloader/test_offloader.py` (tidak dipakai `offloader/server.py`, `policy.py`, maupun `experiments/campaign.py`) | Hapus (bersama unit test khusus `scaler.py` di `offloader/test_offloader.py`) |
| `offloader/budget.py` | **Terjangkau (AKTIF)** | Diimpor `experiments/campaign.py:29` (`from offloader.budget import VerticalBudget`) dan dipakai pada profil `vertical-*` | **Pertahankan** |
| `telemetry/02-dcgm-exporter.yaml` | Tidak terjangkau | Sisa GPU/NVIDIA DCGM; penelitian memakai `pedestrian-detection` CPU-only | Hapus |
| `apps/face-detection/` (`app.py`, `Dockerfile`, `manifest.yaml`, `images/*.png`) | `app.py` tidak terjangkau; `manifest.yaml` `TEST_ONLY` | Workload lama sebelum migrasi ke `pedestrian-detection`; `manifest.yaml` masih dirujuk `experiments/test_campaign.py` dan `utils/rebuild_pedestrian.py`; `images/` dirujuk `utils/defaults.py` | Arsip / Hapus setelah referensi default di `utils/` & tes dialihkan ke `pedestrian-detection` |
| `collector/probe_sessions.py` | Tidak terjangkau | Hanya dirujuk `run-steep-session-probe.sh` | Hapus |
| `experiments/campaign-19-phase.yaml` | Tidak terjangkau | Tidak dirujuk skrip aktif maupun tes | Arsip / Hapus |
| `experiments/campaign-g3.yaml` | Tidak terjangkau | Tidak dirujuk; masih menunjuk `apps/face-detection/images` | Hapus |
| `experiments/campaign-mobility-local.yaml` | Tidak terjangkau | Tidak dirujuk skrip aktif maupun tes | Arsip / Hapus |
| `experiments/campaign-pedestrian.example.yaml` | `TEST_ONLY` | Dirujuk `experiments/test_pedestrian_workload.py` dan `utils/fixed_replica_harness.py` | Pertahankan selama tes ada, atau arsip |
| `experiments/campaign-steep.yaml` | `TEST_ONLY` | Dirujuk skrip lama `run-*-steep*.sh` dan `experiments/test_campaign.py` | Pertahankan selama `test_campaign.py` memakainya |
| `experiments/campaign_ue.py` | `TEST_ONLY` | Hanya diimpor `utils/test_experiment.py` (tidak dipakai `experiments/campaign.py`) | Arsip / Hapus bersama tesnya |
| `utils/fixed_replica_harness.py` (+ `test_fixed_replica_harness.py`) | `TEST_ONLY` | Harness kalibrasi lama; tidak dipakai titik masuk aktif | Arsip / Hapus |
| `utils/calibrate_mobility.py` (+ `test_calibrate_mobility.py`) | `TEST_ONLY` | Utilitas kalibrasi satu-kali; tidak dipakai titik masuk aktif | Arsip / Hapus |
| `utils/smoke_campaign.py` (+ `test_smoke_campaign.py`) | `TEST_ONLY` | Digantikan oleh `--profile vertical-smoke` | Hapus |
| `utils/redeploy_placement.py` (+ `test_redeploy_placement.py`) | `TEST_ONLY` | Hanya dirujuk `run-mobility-hysteresis-smoke.sh` dan `utils/rebuild_pedestrian.py` | Arsip (berguna jika suatu saat rebuild `placement-controller` di `core`) |
| `utils/rebuild_pedestrian.py` (+ `test_rebuild_pedestrian.py`) | `TEST_ONLY` | Helper rebuild image `pedestrian-detection` di `core` | Arsip / Pertahankan sebagai helper lab |
| `utils/provision_ues.py` (+ `test_provision_ues.py`) | `TEST_ONLY` | Utilitas provisi subscriber Open5GS satu-kali | Pertahankan (helper infrastruktur lab) |
| `utils/transient_analysis.py` (+ `test_transient_analysis.py`) | `TEST_ONLY` (analisis offline) | Diimpor `utils/node_windows.py`, `utils/phase_windows.py`, `utils/plot_autoscaler.py` | **Pertahankan** (bagian pipeline analisis & plotting Bab 4) |
| `utils/plot_autoscaler.py`, `utils/plot_vertical.py`, `utils/plot_slides.py`, `utils/plot_overhead_slides.py`, `utils/plot_paths.py`, `utils/vertical_summary.py`, `utils/summarize.py`, `utils/node_windows.py`, `utils/phase_windows.py`, `utils/resource_windows.py`, `utils/control_overhead.py`, `utils/monitor_resources.py`, `utils/cadvisor_resources.py`, `utils/verify_vertical_path.py` | Analisis/plotting/post-processing offline | Dipakai untuk meringkas dan memplot hasil eksperimen (`results/`) untuk Bab 4 dan slide | **Pertahankan** |
| `utils/telemetry_history.py` | Tidak terjangkau | Tidak diimpor modul atau tes mana pun | Hapus |
| `utils/connectivity.py` (+ `test_connectivity.py`) | `TEST_ONLY` | Hanya diimpor `experiments/campaign_ue.py` dan tesnya | Arsip / Hapus bersama `campaign_ue.py` |
| `apps/pedestrian-detection/{benchmark_local,detector,export_model,gunicorn.conf,prepare_images}.py` | Build/runtime kontainer `pedestrian-detection` | Dipakai saat build Docker (`gunicorn.conf.py`, `detector.py`) atau persiapan model/gambar offline | **Pertahankan** |
| `open5gs/patches/*.yaml` | Dokumentasi/patch manual cluster | Referensi konfigurasi Open5GS di `core` | **Pertahankan** |
| `placement/placement-controller` | Binary Go terkompilasi lokal | Artefak build lokal (tidak masuk titik masuk aktif; cluster memakai image registry) | Hapus dari working tree (tambahkan ke `.gitignore` jika perlu) |

---

## 2. Sisa LLM / GPU / VRAM / DCGM & Workload Lama

| Berkas | Temuan | Status & Rekomendasi |
|---|---|---|
| `telemetry/02-dcgm-exporter.yaml` | DaemonSet NVIDIA DCGM Exporter untuk metrik GPU/VRAM | Tidak dipakai (`pedestrian-detection` CPU-only). Rekomendasi: **Hapus**. |
| `telemetry/03-prometheus.yaml` | Recording rules VRAM (L121–150) dan `vllm:time_to_first_token_seconds_bucket` (L152) | Sisa eksperimen vLLM/GPU. Rekomendasi: **Bersihkan** saat kluster Prometheus diperbarui. |
| `telemetry/04-grafana.yaml` | Panel dashboard `VRAM Resource Telemetry (Accelerated Nodes)` (L482–560) | Sisa eksperimen GPU. Rekomendasi: **Bersihkan** saat dashboard Grafana diperbarui. |
| `config.yaml`, `placement/deploy.yaml`, `experiments/campaign.py`, `utils/config.py` | Kunci `gpu_nodes`, `resource_weights` 4-dimensi (`gpu: 0.0, vram: 0.0`), dan `prefer_close` | **Pertahankan sementara**: binary `placement-controller` yang sedang ter-deploy di kluster `core` masih memvalidasi keberadaan `gpu_nodes`, `prefer_close`, dan 4 kunci `resource_weights`. Kode Go terbaru di `placement/internal/config/config.go` sudah menerima config 2D (`cpu`, `memory`) dan `pod_share_scoring`, serta mengabaikan `gpu_nodes`/`gpu`/`vram`. Setelah image `placement-controller` di-rebuild di `core`, kunci-kunci ini dapat dihapus total. |
| `autoscaler/hpa-baseline.yaml` | `scaleTargetRef.name: mec-placement-face-detection` | Manifest statis referensi lama (campaign merender HPA secara dinamis). Rekomendasi: perbarui nama workload ke `mec-placement-pedestrian-detection` atau tandai sebagai contoh. |
| `utils/defaults.py`, `utils/monitor_resources.py`, `utils/cadvisor_resources.py`, `utils/resource_windows.py`, `utils/experiment.py`, `utils/traffic_control.py` | Default fallback string `mec-placement-face-detection` / `apps/face-detection/images` | Masih dirujuk oleh beberapa unit test lama (`test_cadvisor_resources.py`, `test_min_replicas.py`, `test_campaign.py`). Jangan ubah diam-diam tanpa memperbarui tes dan memastikan tidak memengaruhi parsing hasil lama. |

---

## 3. Jalur Controller Python Lama

| Simbol / Berkas | Bukti Penggunaan | Rekomendasi |
|---|---|---|
| `PLACEMENT_ENGINES = ('python', 'go')` di `experiments/campaign.py:136` | Tidak ada profil aktif di `PROFILES` yang memakai `'python'`. Namun masih diuji oleh `experiments/test_campaign.py` (L1012, L1350) dan skrip validasi CLI. | **Pertahankan sementara** (jangan hapus di Tahap 2 karena masih diuji `test_campaign.py` untuk memastikan error handling); jadwalkan penghapusan setelah semua tes lama dibersihkan. |
| `AUTOSCALERS = ('controller', 'hpa')` di `experiments/campaign.py:135` | Tidak ada profil aktif yang memakai `'controller'`. Masih diuji oleh `experiments/test_campaign.py`, `experiments/test_rt_target_factor.py`, `experiments/test_latency_floor.py`. | **Pertahankan sementara** karena masih diuji sebagai jalur penolakan / validasi error. |
| `utils/redeploy_placement.py` | Hanya dirujuk `run-mobility-hysteresis-smoke.sh` dan `utils/rebuild_pedestrian.py`. | **Arsip** (bukan bagian loop eksperimen aktif, tapi berguna untuk rebuild image Go). |

---

## 4. Komentar / Docstring Usang

Daftar kemunculan kata kunci `codex`, `agy`, `gemini`, `claude`, `llm`, `legacy`, `old controller`, `TODO`, `XXX`, `HACK` di berkas kode/YAML:

| Berkas | Baris | Temuan | Rekomendasi |
|---|---|---|---|
| `utils/test_experiment.py` | L693 | `# Reproduce Codex's delay scenario: ...` | Bersihkan di Tahap 2 (ganti dengan deskripsi skenario teknis tanpa nama agen). |
| `WATCHDOG.yml` | L5 | `model: openai-codex/gpt-6.1-terra:low` | Berkas kandidat hapus (lihat §1.1). |
| `config.yaml` | L11, L25 | Menyebut `Legacy Python controller`, `Legacy gpu/vram keys` | Rapikan komentar agar ringkas menjelaskan arti kunci (Tahap 3). |
| `placement/deploy.yaml` | L60, L63 | Menyebut `Legacy spelling`, `legacy binary` | Biarkan singkat sebagai alasan kompatibilitas binary cluster yang sedang ter-deploy. |
| `experiments/campaign.py` | L829, L2025, L2029, L2033 | Menyebut `legacy spelling`, `legacy steps`, `legacy full reattach`, `legacy balanced` | Rapikan teks bantuan/komentar agar deskriptif tanpa jargon "legacy" yang tidak perlu (tanpa mengubah perilaku CLI). |
| `placement/internal/config/config.go` | L44, L92–93 | Komentar kompatibilitas `prefer_close` dan 4D weights | Pertahankan (hasil kerja Codex di working tree). |
| `experiments/test_profiles.py`, `collector/test_server.py`, `utils/plot_autoscaler.py` | Variabel/docstring `legacy` | Nama variabel lokal/tes untuk format lama yang masih didukung | Pertahankan (bukan komentar usang; mencerminkan kompatibilitas baca data). |
