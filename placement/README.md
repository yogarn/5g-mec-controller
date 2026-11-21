# Go Placement Controller (`placement/`)

## Peran

`placement/` adalah controller penempatan in-cluster (Go) untuk workload `mec-placement-pedestrian-detection` yang terdiri dari **Scheduler** (mengikat Pod Pending ke node terbaik melalui Kubernetes Pod Binding API) dan **Descheduler** (mengeviksi paling banyak satu Pod terikat melalui `policy/v1` Eviction API saat perpindahan menghasilkan kenaikan skor positif). Skala jumlah replika tetap menjadi tanggung jawab Kubernetes HPA.

## Peta Berkas (`placement/internal/`)

| Berkas | Peran (1 kalimat) | Fungsi Utama |
| --- | --- | --- |
| `controller/controller.go` | Struct `Controller`, Pod Informer event-driven, dan loop periodik scheduler/descheduler. | `NewController`, `startPodInformer`, `Run`, `logDecisionTiming` |
| `controller/snapshot.go` | Pengumpulan snapshot node & Pod dari Kubernetes API dan metrik CPU/mobilitas dari Prometheus. | `workloadPods`, `nodesSnapshot`, `queryNodeTelemetry`, `collectAllocatedResources`, `buildPolicyNodes`, `allowedNodesForSpec` |
| `controller/scheduler.go` | Alur penjadwalan Pod Pending berurutan dan pengikatan ke node terpilih. | `ReconcileScheduler`, `pendingPods`, `computePodShares`, `scheduleOnePod`, `bindPod` |
| `controller/descheduler.go` | Alur evaluasi migrasi satu Pod setelah Deployment tenang dan eksekusi eviksi `policy/v1`. | `ReconcileDescheduler`, `reconcileDescheduler`, `settledActivePods`, `groupPodsByNode`, `evictVictim` |
| `policy/policy.go` | Algoritma skoring kapasitas CPU/RAM, saturasi, locality demand, dan seleksi kandidat deschedule. | `ScoreCandidateNode`, `SelectDescheduleCandidate`, `AddNodeAllocation`, `PodRequests`, `PodFitsNodeConstraints` |
| `config/config.go` | Pembacaan `config.yaml` dan identifikasi `(namespace, name)` Deployment dari manifest. | `LoadSettings`, `ValidateResourceWeights`, `WorkloadIdentity` |
| `telemetry/prometheus.go` | Klien HTTP query instant vector Prometheus per node. | `PerNode` |

## Alur `ReconcileScheduler` (Langkah demi Langkah)

1. **Ambil Deployment & Pod aktif** (`AppsV1.Deployments.Get` → `workloadPods`): sumber data = Kubernetes API; membersihkan `inFlightBindings` untuk Pod yang sudah terikat atau hilang.
2. **Saring & urutkan Pod Pending** (`pendingPods`): pilih Pod tanpa `NodeName`, `SchedulerName == mec-placement-scheduler`, bukan terminating, dan tidak sedang in-flight; urutkan deterministik berdasarkan `(CreationTimestamp, Name, UID)`.
3. **Bangun snapshot node** (`nodesSnapshot`):
   * `queryNodeTelemetry`: ambil utilisasi CPU dan laju kedatangan UE per node dari **Prometheus** (`ZeroLocalityNodes` diisi `0.0`).
   * `Nodes.List` + `collectAllocatedResources`: ambil kapasitas `Allocatable` node dan total request CPU/RAM semua Pod aktif + `inFlightBindings` dari **Kubernetes API**.
4. **Hitung porsi Pod per node** (`computePodShares`): jika `PodShareScoring` aktif, hitung fraksi Pod saat ini relatif terhadap `max(active, spec.replicas)` agar penjadwalan batch tidak bias.
5. **Skor & ikat tiap Pod** (`scheduleOnePod` → `policy.ScoreCandidateNode` → `bindPod`): filter constraint node (`allowedNodesForSpec`), pilih node skor tertinggi, panggil Kubernetes Pod Binding API, catat di `inFlightBindings`, lalu perbarui alokasi & `podShares` in-memory sebelum Pod berikutnya.

## Alur `reconcileDescheduler` (Langkah demi Langkah)

1. **Cek interval & ambil Deployment/Pod** (`descheduleIntervalDue` → `Deployments.Get` → `workloadPods`): sumber data = jam lokal + Kubernetes API; abaikan bila Deployment tidak memakai `mec-placement-scheduler`.
2. **Pastikan Deployment sudah tenang** (`settledActivePods`): sumber data = status Pod Kubernetes API + `inFlightBindings`; tunda bila ada Pod belum Ready, belum terikat, in-flight, atau jumlah aktif ≠ `spec.replicas`.
3. **Catat waktu cek & hitung request template** (`deploymentRequests`): ambil request CPU/RAM dari `dep.Spec.Template.Spec`.
4. **Bangun snapshot node & pilih kandidat** (`nodesSnapshot` → `allowedNodesForSpec` → `policy.SelectDescheduleCandidate`): sumber data = Prometheus + Kubernetes API; cari pasangan `(Source, Destination)` dengan kenaikan skor melebihi ambang minimum.
5. **Eviksi satu korban deterministik** (`evictVictim`): urutkan Pod di node `Source` berdasarkan nama, ambil yang pertama, lalu panggil `Pods.EvictV1` (menangani penolakan PDB `429` dan konflik `404`/`409` secara aman).
