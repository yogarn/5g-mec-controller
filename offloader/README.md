# Vertical Offloader (`offloader/`)

## Peran

`offloader/` adalah control plane vertical offloading di sisi Core yang berjalan pada frekuensi 1 Hz (di luar jalur data permintaan UE). Setiap detik, proses `offloader.server` menemukan Pod inferensi siap-pakai di tiap gNB dan Core melalui Kubernetes API, mengambil snapshot latensi/error per situs dari `collector` (`/offload/snapshot`), mengevaluasi mesin status per situs (`LOCAL` vs `SPREAD` berbobot dengan retreat bertahap), lalu menyajikan konfigurasi rute (RDS) dan bobot endpoint (EDS) ke proxy Envoy di tiap situs melalui REST-JSON xDS v3.

## Alur Keputusan Per Situs

```text
                      (1) Masuk SPREAD (n=0)
           p90 > 150 ms (samples >= 15) ATAU error_rate >= 0.5 (attempts >= 5)
           bertahan >= 5 dtk, >= 2 update segar, dwell >= 15 dtk, core_ready > 0
  +-------+ -----------------------------------------------------------------> +--------------+
  | LOCAL |                                                                    | SPREAD (n=0) |
  +-------+ <----------------------------------------------------------------- +--------------+
     ^           (4) Failback darurat: core_ready == 0 & local_ready > 0         |        ^
     |                                                                           |        |
     | (3) Retreat selesai (spread_retreat_complete)                             |        |
     |     s_{n+1} < SPREAD_RETREAT_FLOOR (0.01)                                 |        |
     |                                                                           v        |
     |                                                    (2a) Langkah retreat (n -> n+1) |
     |                                                    post-ACK selesai, p90 <= 150 ms,|
     |                                                    samples >= 15, >= 2 update segar|
     |                                                    tidak dalam hold, dwell >= 15dtk|
     |                                                                           |        |
     |                                                                           v        |
     +--------------------------------------------------------------------- +--------------+
                                                                            | SPREAD (n>0) |
                                                                            +--------------+
                                                                                  |
                                            (2b) Pulihkan bobot (restore) --------+
                                            p90 > 150 ms atau error_rate >= 0.5,
                                            >= 2 update segar -> target_level =
                                            good_level (jika n > good_level) atau
                                            good_level - 1; set hold_until = now + min_dwell
```

Serialisasi global: paling banyak satu perubahan rute/bobot dieksekusi per interval (`route_switch_interval_seconds = 5s`) dengan prioritas: **(1) enter SPREAD** > **(2) restore level** > **(3) step/complete retreat**.

## Peta Berkas dan Fungsi Publik

| Berkas | Peran (1 kalimat) | Fungsi / Kelas Utama | Dipanggil oleh |
| --- | --- | --- | --- |
| `offloader/server.py` | Titik masuk CLI dan loop kontrol 1 Hz yang menghubungkan K8s, collector, policy, dan xDS. | `VerticalOffloadController`, `fetch_collector_snapshot`, `load_offloader_settings`, `main` | `Dockerfile` (`python -m offloader.server`), `test_offloader.py`, `test_envoy_real.py` |
| `offloader/policy.py` | Koordinator mesin status `LOCAL`/`SPREAD`, serialisasi perubahan global, dan evaluasi 5 langkah per tick. | `VerticalOffloadPolicy.evaluate`, `set_manual_override`, `set_mode`, `reset`, `record_envoy_ack`, `record_envoy_eds_ack`, `record_envoy_nack`, `spread_count` | `server.py`, `test_offloader.py`, `test_equivalence.py` |
| `offloader/state.py` | Definisi konfigurasi tervalidasi, state per situs, dan struktur event audit. | `OffloadConfig`, `SiteState` (`reset`, `make_event`, `to_dict`), `DecisionEvent`, `RouteSwitchBusy`, `SITES`, `ROUTES`, `MODES`, `DEFAULT_SITE_NODES` | `policy.py`, `entry.py`, `retreat.py`, `transitions.py`, `envoy_ack.py`, `xds.py`, `xds_http.py` |
| `offloader/spread.py` | Rumus rasio/bobot Core-Local untuk `SPREAD` dan helper kesegaran jendela snapshot collector. | `SPREAD_RETREAT_FACTOR`, `SPREAD_RETREAT_FLOOR`, `spread_core_ratio`, `spread_local_weight`, `bucket_stale`, `post_ack_window`, `overall_user_bucket` | `policy.py`, `entry.py`, `retreat.py`, `xds.py` |
| `offloader/entry.py` | Deteksi overload situs `LOCAL` (p90, error berlabel, dan timeout tak berlabel pasca-ACK) untuk kandidat masuk `SPREAD`. | `evaluate_local_entry` | `policy.py` |
| `offloader/retreat.py` | Evaluasi situs `SPREAD` pasca-ACK untuk menurunkan porsi Core bertahap atau memulihkan bobot saat melanggar SLO. | `evaluate_spread_retreat` | `policy.py` |
| `offloader/transitions.py` | Eksekusi terurut paling banyak satu transisi rute/bobot per interval (enter, restore, retreat, failback, local-only). | `execute_transitions`, `apply_local_only`, `apply_core_failback` | `policy.py` |
| `offloader/envoy_ack.py` | Pencatatan ACK/NACK dan timeout ACK dari Envoy RDS/EDS ke `SiteState`. | `record_rds_ack`, `record_eds_ack`, `record_nack`, `check_ack_timeouts` | `policy.py` |
| `offloader/xds.py` | Penyimpanan snapshot xDS ber-epoch, perhitungan peta endpoint berbobot, dan handler protokol RDS/EDS. | `XdsState` (`set_site_route`, `set_site_retreat_level`, `update_endpoints`, `handle_rds`, `handle_eds`, `reset_routes`, `status_snapshot`) | `server.py`, `test_offloader.py`, `test_equivalence.py` |
| `offloader/xds_resources.py` | Pembangun dokumen JSON Envoy v3 `RouteConfiguration` (RDS) dan `ClusterLoadAssignment` (EDS). | `build_route_configuration`, `build_invalid_route_configuration`, `build_cluster_load_assignment`, `route_config_name`, `local_eds_service_name`, `spread_eds_service_name` | `xds.py`, `server.py`, `test_offloader.py`, `test_equivalence.py` |
| `offloader/xds_http.py` | Server HTTP terpisah untuk discovery Envoy (`/v3/discovery:*`) dan API admin ber-token (`/status`, `/events`, `/override`, `/mode`, `/reset`). | `make_xds_discovery_handler`, `make_admin_handler`, `start_xds_server`, `start_admin_server` | `xds.py`, `server.py`, `test_offloader.py`, `test_envoy_real.py` |
| `offloader/k8s.py` | Penemuan Pod inferensi yang Running & Ready per node gNB dan Core via in-cluster K8s API atau `kubectl`. | `is_pod_ready`, `filter_pinned_endpoints`, `KubernetesEndpointDiscoverer` | `server.py`, `test_offloader.py` |
| `offloader/budget.py` | Sumber kebenaran anggaran replika terpadu (edge anchor + Core fallback + replika dinamis). | `VerticalBudget`, `DEFAULT_MIN_REPLICAS`, `DEFAULT_MAX_REPLICAS`, `DEFAULT_EDGE_ANCHORS`, `DEFAULT_CORE_FALLBACK_REPLICAS` | `experiments/campaign.py`, `experiments/vertical.py`, `test_offloader.py` |

## Konstanta dan Kunci Konfigurasi (`offloader/config.yaml`)

| Nama | Nilai Default | Arti |
| --- | --- | --- |
| `SPREAD_RETREAT_FACTOR` | `2` | Pembagi porsi Core per langkah retreat: $s_n = s_0 / 2^n$ dengan $s_0 = c / (c + k \cdot x_i)$. |
| `SPREAD_RETREAT_FLOOR` | `0.01` | Ambang porsi Core minimum; jika $s_{n+1} < 0.01$, retreat selesai dan situs kembali ke `LOCAL`. |
| `mode` | `'vertical'` | Mode operasi: `'vertical'` (otomatis `LOCAL`/`SPREAD`) atau `'local-only'` (terkunci di `LOCAL`). |
| `poll_interval_seconds` | `1.0` | Periode loop kontrol (detik). |
| `collector_timeout_seconds` | `1.0` | Timeout HTTP saat mengambil `/offload/snapshot` dari collector. |
| `offload_p90_threshold_ms` | `150.0` | Ambang SLO p90 (ms) untuk masuk `SPREAD` (`>`) dan evaluasi retreat (`<=`). |
| `min_samples` | `15` | Jumlah sampel sukses minimum sebelum p90 dievaluasi. |
| `min_error_samples` | `5` | Jumlah percobaan minimum sebelum laju error memicu overload/restore. |
| `offload_error_rate_threshold` | `0.5` | Ambang rasio error (`errors / attempts`) untuk memicu `SPREAD` atau restore. |
| `breach_duration_seconds` | `5.0` | Durasi pelanggaran berkelanjutan sebelum situs `LOCAL` boleh masuk `SPREAD`. |
| `min_fresh_updates` | `2` | Jumlah kedatangan sampel baru (`newest_sample_seq` meningkat) minimum sebelum transisi. |
| `min_dwell_seconds` | `15.0` | Waktu tahan minimum setelah transisi rute atau restore sebelum langkah retreat berikutnya. |
| `route_switch_interval_seconds` | `5.0` | Jarak waktu minimum global antar perubahan rute/bobot lintas seluruh situs. |
| `stale_after_seconds` | `5.0` | Batas usia sampel terakhir sebelum bucket telemetri dianggap basi (`stale`). |
| `envoy_ack_timeout_seconds` | `5.0` | Batas tunggu ACK RDS/EDS dari Envoy sebelum ditandai `envoy_update_failed`. |
| `envoy_route_timeout` | `'10s'` | Timeout rute per permintaan pada Envoy `RouteConfiguration`. |
| `listen_host` / `listen_port` | `'0.0.0.0'` / `8085` | Alamat dan port listener xDS discovery Envoy. |
| `admin_host` / `admin_port` | `'0.0.0.0'` / `8086` | Alamat dan port listener API admin ber-token Bearer. |
| `namespace`, `edge_app_label`, `core_app_label`, `container_port`, `core_node`, `site_nodes` | `services`, `mec-placement-pedestrian-detection`, `...-core`, `5000`, `core-5g`, `{gnb-1: gnb1, ...}` | Parameter penemuan Pod Kubernetes terpin per node. |
| `collector_snapshot_url` | URL layanan collector | Endpoint pembacaan snapshot `/offload/snapshot`. |
