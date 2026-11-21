"""Tests for utils.plot_final_phases using a small synthetic fixture."""
import csv
import json
import math

from utils import plot_final_phases as pfp


def _write_csv(path, fieldnames, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_arm_aux_csvs(arm_dir, phases, pending_phase=None):
    pod_rows = []
    res_rows = []
    for idx, phase in enumerate(phases, start=1):
        for node in pfp.NODES:
            pod_rows.append({
                "phase": phase,
                "node": node,
                "pod_count": 1 if node != "core-5g" else 0,
                "ready_count": 1 if node != "core-5g" else 0,
                "pod_names": "",
                "sample_time": str(1000 + idx * 120 - 2),
                "phase_end": str(1000 + idx * 120),
                "sample_age_seconds": "2.0",
            })
        pod_rows.append({
            "phase": phase,
            "node": "<pending>",
            "pod_count": 2 if phase == pending_phase else 0,
            "ready_count": 0,
            "pod_names": "",
            "sample_time": str(1000 + idx * 120 - 2),
            "phase_end": str(1000 + idx * 120),
            "sample_age_seconds": "2.0",
        })
        res_rows.append({
            "phase": phase,
            "rate_window_seconds": "45",
            "cpu_millicores": "" if (idx == 2 and pending_phase) else str(200 + idx * 10),
            "memory_mib": str(300 + idx * 15),
            "cpu_completeness_status": "partial" if idx == 1 else "complete",
            "memory_completeness_status": "complete",
        })
    _write_csv(
        arm_dir / "pods-by-ramp.csv",
        ["phase", "node", "pod_count", "ready_count", "pod_names", "sample_time", "phase_end", "sample_age_seconds"],
        pod_rows,
    )
    _write_csv(
        arm_dir / "resource-by-ramp.csv",
        ["phase", "rate_window_seconds", "cpu_millicores", "memory_mib",
         "cpu_completeness_status", "memory_completeness_status"],
        res_rows,
    )


def _build_fixture(tmp_path):
    final_dir = tmp_path / "final"

    # Joint: 4 arms x 4 phases
    joint_phases = [f"phase_{i:02d}" for i in range(1, 5)]
    joint_dir = final_dir / pfp.SCENARIOS["joint"]["dir"]
    joint_rows = []
    for arm in pfp.FOUR_ARMS:
        for idx, phase in enumerate(joint_phases, start=1):
            joint_rows.append({
                "arm": arm,
                "phase_index": idx,
                "phase": phase,
                "workload_ue": idx * 3,
                "gnb1_ues": idx,
                "gnb2_ues": idx,
                "gnb3_ues": idx,
                "p90_ms": 80.0 + idx * 10,
                "slo_violation_rate": 0.05 * idx,
            })
        _write_arm_aux_csvs(joint_dir / arm, joint_phases, pending_phase="phase_03" if arm == "baseline-local" else None)
    _write_csv(
        joint_dir / "vertical-4arm-phases.csv",
        ["arm", "phase_index", "phase", "workload_ue", "gnb1_ues", "gnb2_ues", "gnb3_ues",
         "p90_ms", "slo_violation_rate"],
        joint_rows,
    )

    # Mobility: 4 arms x 3 phases
    mob_phases = [f"phase_{i:02d}" for i in range(1, 4)]
    mob_dir = final_dir / pfp.SCENARIOS["mobility"]["dir"]
    mob_rows = []
    for arm in pfp.FOUR_ARMS:
        for idx, phase in enumerate(mob_phases, start=1):
            mob_rows.append({
                "arm": arm,
                "phase_index": idx,
                "phase": phase,
                "workload_ue": 24,
                "gnb1_ues": 10,
                "gnb2_ues": 7,
                "gnb3_ues": 7,
                "p90_ms": 95.0 + idx * 5,
                "slo_violation_rate": 0.02 * idx,
            })
        _write_arm_aux_csvs(mob_dir / arm, mob_phases)
    _write_csv(
        mob_dir / "vertical-4arm-phases.csv",
        ["arm", "phase_index", "phase", "workload_ue", "gnb1_ues", "gnb2_ues", "gnb3_ues",
         "p90_ms", "slo_violation_rate"],
        mob_rows,
    )

    # Scaling: 2 arms x 13 phases
    scaling_phases = [f"phase_{i:02d}" for i in range(1, 14)]
    scaling_dir = final_dir / pfp.SCENARIOS["scaling"]["dir"]
    for arm in pfp.TWO_ARMS:
        rows = []
        for idx, phase in enumerate(scaling_phases, start=1):
            per_site = min(idx, 14 - idx)
            rows.append({
                "phase": phase,
                "workload_ue": 3 * per_site,
                "gnb1_ues": per_site,
                "gnb2_ues": per_site,
                "gnb3_ues": per_site,
                "latency_p90_ms": 110.0 + idx * 4,
                "slo_attainment_all_attempts": 0.95 - 0.01 * idx,
            })
        _write_csv(
            scaling_dir / arm / "summary.csv",
            ["phase", "workload_ue", "gnb1_ues", "gnb2_ues", "gnb3_ues",
             "latency_p90_ms", "slo_attainment_all_attempts"],
            rows,
        )
        _write_arm_aux_csvs(scaling_dir / arm, scaling_phases)
        with (scaling_dir / arm / "requests.jsonl").open("w", encoding="utf-8") as handle:
            for phase in scaling_phases:
                for ms in range(10, 110, 10):
                    handle.write(json.dumps({"event": "request", "phase": phase,
                                             "success": True, "duration_ms": ms}) + "\n")
                handle.write(json.dumps({"event": "request", "phase": phase,
                                         "success": False, "duration_ms": 9999}) + "\n")

    return final_dir, len(joint_phases), len(mob_phases), len(scaling_phases)


def test_loader_row_counts_and_edge_cases(tmp_path):
    final_dir, n_joint, n_mob, n_scaling = _build_fixture(tmp_path)

    for scenario, expected_arms, expected_phases in (
        ("joint", 4, n_joint),
        ("mobility", 4, n_mob),
        ("scaling", 2, n_scaling),
    ):
        cfg = pfp.SCENARIOS[scenario]
        run_dir = final_dir / cfg["dir"]
        metrics, phases, phase_info, _ = pfp.load_phase_metrics(run_dir, cfg)
        resources, r_missing, _ = pfp.load_resources(run_dir, cfg["arms"], phases)
        pods, p_missing, pending = pfp.load_pods(run_dir, cfg["arms"], phases)

        assert len(phases) == expected_phases
        assert len(metrics) == expected_arms * expected_phases
        assert len(resources) == expected_arms * expected_phases
        assert len(pods) == expected_arms * expected_phases
        assert p_missing == []
        assert ":" in pfp.phase_tick_label(phases[0], phase_info[phases[0]])

        if scenario == "joint":
            assert ("baseline-local", "phase_02", "cpu_millicores") in r_missing
            gap = [r for r in resources if r["arm"] == "baseline-local" and r["phase"] == "phase_02"][0]
            assert math.isnan(gap["cpu_millicores"])
            assert pending == [("baseline-local", "phase_03", 2)]


def test_generate_all_plots_writes_png_and_csv(tmp_path):
    final_dir, n_joint, n_mob, n_scaling = _build_fixture(tmp_path)
    out_dir = final_dir / "plots"

    report = pfp.generate_all_plots(final_dir=final_dir, output_dir=out_dir)
    assert len(report["files"]) == 3 * 4 * 2  # 3 scenarios x 4 plots x (png, csv)

    expected_rows = {"joint": 4 * n_joint, "mobility": 4 * n_mob, "scaling": 2 * n_scaling}
    for scenario, count in expected_rows.items():
        for suffix in ("p90_response_time", "slo_violation_rate", "resource_usage", "pod_placement"):
            for ext in (".png", ".csv"):
                path = out_dir / f"{scenario}_{suffix}{ext}"
                assert path.is_file() and path.stat().st_size > 0
            assert not (out_dir / f"{scenario}_{suffix}.svg").exists()
            with (out_dir / f"{scenario}_{suffix}.csv").open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            assert len(rows) == count


def test_nearest_rank_p90_uses_ceil_0_9_n_of_successes(tmp_path):
    path = tmp_path / "requests.jsonl"
    with path.open("w", encoding="utf-8") as handle:
        for ms in range(10, 110, 10):  # 10 successes: ceil(0.9 * 10) = 9th value = 90
            handle.write(json.dumps({"event": "request", "phase": "phase_01",
                                     "success": True, "duration_ms": ms}) + "\n")
        handle.write(json.dumps({"event": "request", "phase": "phase_01",
                                 "success": False, "duration_ms": 9999}) + "\n")
        handle.write(json.dumps({"event": "phase_end", "phase": "phase_01"}) + "\n")
    assert pfp.nearest_rank_p90_by_phase(path) == {"phase_01": 90.0}
