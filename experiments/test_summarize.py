import csv
import json
import io
from contextlib import redirect_stdout
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from experiments import summarize


def records():
    phase = {'phase': 'phase_01', 'workload_ue': 2, 'gnb2_ues': 1, 'gnb3_ues': 1}
    ues = [{'group': 'gnb-2', 'source_address': '10.0.0.1'},
           {'group': 'gnb-3', 'source_address': '10.0.0.2'}]
    start = {'event': 'phase_start', **phase, 'start_time': 100.0, 'duration_s': 10.0, 'ues': ues}

    def request(ue, attempt, latency, status, error=None):
        return {'event': 'request', 'phase': phase['phase'], **ue, 'url': 'http://example/',
                'method': 'GET', 'attempt': attempt, 'start_time': 101.0, 'end_time': 102.0,
                'duration_ms': latency, 'http_status': status, 'error': error,
                'success': error is None and status is not None and 200 <= status < 300}

    events = [start, request(ues[0], 1, 10, 200), request(ues[1], 1, 20, 200),
              request(ues[0], 2, 2, 503), request(ues[1], 2, 30, None, 'timeout')]
    events.append({'event': 'phase_end', **phase, 'start_time': 100.0, 'end_time': 112.0,
                   'generation_duration_s': 10.0, 'attempts': 4, 'successes': 2,
                   'per_ue': [{**ue, 'attempts': 2, 'successes': 1} for ue in ues]})
    return events


class SummaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.source = self.directory / 'input.jsonl'
        self.prefix = self.directory / 'results'

    def load(self, events):
        self.source.write_text(''.join(json.dumps(event) + '\n' for event in events), encoding='utf-8')
        return summarize.summarize(self.source, 15)

    def test_metrics_and_outputs(self):
        phases = self.load(records())
        result = phases[0]
        self.assertEqual((result['workload_ue'], result['gnb2_ues'], result['gnb3_ues']), (2, 1, 1))
        self.assertEqual((result['attempts'], result['successes'], result['failures']), (4, 2, 2))
        self.assertEqual((result['start_time'], result['end_time'], result['generation_duration_s']), (100, 112, 10))
        self.assertEqual(result['throughput_denominator_s'], 12)
        self.assertAlmostEqual(result['attempt_throughput_per_s'], 4 / 12)
        self.assertAlmostEqual(result['success_throughput_per_s'], 2 / 12)
        self.assertEqual(result['latency_p50_ms'], 15)
        self.assertEqual(result['latency_p95_ms'], 19.5)
        self.assertEqual(result['latency_p99_ms'], 19.9)
        self.assertEqual(result['slo_met_attempts'], 1)
        self.assertEqual(result['slo_attainment_all_attempts'], .25)
        summarize.write_results(phases, self.prefix)
        self.assertEqual(json.loads(self.prefix.with_suffix('.json').read_text()), phases)
        with self.prefix.with_suffix('.csv').open(newline='') as file:
            rows = list(csv.DictReader(file))
        self.assertEqual(rows[0]['phase'], 'phase_01')
        self.assertEqual(float(rows[0]['slo_attainment_all_attempts']), .25)

    def test_per_ue_outputs_and_collision(self):
        self.load(records())
        ue_rows = []
        phases = summarize.summarize(self.source, 15, ue_rows)
        self.assertEqual(len(ue_rows), 2)
        first, second = ue_rows
        self.assertEqual((first['phase'], first['group'], first['source_address']),
                         ('phase_01', 'gnb-2', '10.0.0.1'))
        self.assertEqual((first['attempts'], first['successes'], first['failures']), (2, 1, 1))
        self.assertEqual((first['latency_p50_ms'], first['latency_p95_ms'], first['latency_p99_ms']), (10, 10, 10))
        self.assertEqual(first['slo_attainment_all_attempts'], .5)
        self.assertEqual(second['slo_attainment_all_attempts'], 0)
        ue_csv = self.directory / 'results-ues.csv'
        ue_json = self.directory / 'results-ues.json'
        ue_csv.write_text('preserve')
        with self.assertRaises(FileExistsError):
            summarize.write_results(phases, self.prefix, ue_rows)
        self.assertEqual(ue_csv.read_text(), 'preserve')
        self.assertFalse(ue_json.exists())
        self.assertFalse(self.prefix.with_suffix('.json').exists())
        self.assertFalse(self.prefix.with_suffix('.csv').exists())
        ue_csv.unlink()
        summarize.write_results(phases, self.prefix, ue_rows)
        self.assertEqual(json.loads(ue_json.read_text()), ue_rows)
        with ue_csv.open(newline='') as file:
            rows = list(csv.DictReader(file))
        self.assertEqual(rows[0]['source_address'], '10.0.0.1')
        self.assertEqual(float(rows[0]['slo_attainment_all_attempts']), .5)

    def test_per_ue_zero_attempts_and_incomplete_input(self):
        events = records()
        events = [events[0], dict(events[-1], attempts=0, successes=0,
                                per_ue=[{**ue, 'attempts': 0, 'successes': 0} for ue in events[0]['ues']])]
        self.load(events)
        ue_rows = []
        summarize.summarize(self.source, 15, ue_rows)
        self.assertIsNone(ue_rows[0]['latency_p50_ms'])
        self.assertIsNone(ue_rows[0]['slo_attainment_all_attempts'])
        self.source.write_text(json.dumps(events[0]) + '\n')
        with self.assertRaises(ValueError):
            summarize.summarize(self.source, 15, ue_rows)
        self.assertEqual(len(ue_rows), 2)

    def test_empty_phase_and_multiple_phases(self):
        events = records()
        empty_start = dict(events[0], phase='phase_02', start_time=200)
        empty_end = dict(events[-1], phase='phase_02', start_time=200, end_time=210,
                         attempts=0, successes=0,
                         per_ue=[{**ue, 'attempts': 0, 'successes': 0} for ue in empty_start['ues']])
        result = self.load(events + [empty_start, empty_end])[1]
        self.assertEqual(result['attempts'], 0)
        self.assertIsNone(result['slo_attainment_all_attempts'])
        self.assertIsNone(result['latency_p99_ms'])

    def test_all_failures_and_inclusive_slo(self):
        events = records()
        self.assertEqual(self.load(events)[0]['slo_met_attempts'], 1)
        self.assertEqual(summarize.summarize(self.source, 10)[0]['slo_met_attempts'], 1)
        for event in events[1:-1]:
            event.update(http_status=500, error=None, success=False)
        events[-1]['successes'] = 0
        for ue in events[-1]['per_ue']:
            ue['successes'] = 0
        result = self.load(events)[0]
        self.assertIsNone(result['latency_p50_ms'])
        self.assertEqual(result['slo_attainment_all_attempts'], 0)

    def test_incomplete_and_inconsistent_inputs_rejected(self):
        events = records()
        cases = [events[:-1], events[1:], events + [events[-1]],
                 events[:2] + events[3:],
                 [events[0], dict(events[1], attempt=2)] + events[2:],
                 events[:-1] + [dict(events[-1], attempts=99)],
                 events[:-1] + [dict(events[-1], end_time=100)],
                 [dict(events[0], workload_ue=3)] + events[1:],
                 [events[0], dict(events[1], success=False)] + events[2:]]
        for case in cases:
            with self.subTest(case=case), self.assertRaises(ValueError):
                self.load(case)
        self.source.write_text('{"event":"phase_start","event":"phase_end"}\n')
        with self.assertRaises(ValueError):
            summarize.summarize(self.source, 15)
        self.source.write_text('{bad json\n')
        with self.assertRaises(ValueError):
            summarize.summarize(self.source, 15)
        with self.assertRaises(ValueError):
            summarize.summarize(self.source, float('nan'))

    def test_never_overwrites_and_rolls_back_partial_output(self):
        phases = self.load(records())
        csv_path = self.prefix.with_suffix('.csv')
        json_path = self.prefix.with_suffix('.json')
        csv_path.write_text('existing csv')
        with self.assertRaises(FileExistsError):
            summarize.write_results(phases, self.prefix)
        self.assertEqual(csv_path.read_text(), 'existing csv')
        self.assertFalse(json_path.exists())
        csv_path.unlink()
        json_path.write_text('existing json')
        with self.assertRaises(FileExistsError):
            summarize.write_results(phases, self.prefix)
        self.assertEqual(json_path.read_text(), 'existing json')
        self.assertFalse(csv_path.exists())

    def test_cli_reports_counts_and_all_output_paths(self):
        self.source.write_text(''.join(json.dumps(event) + '\n' for event in records()), encoding='utf-8')
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            summarize.main([str(self.source), '--output-prefix', str(self.prefix)])
        output = stdout.getvalue()
        self.assertIn('1 phase(s) and 2 per-UE result(s)', output)
        for suffix in ('.json', '.csv', '-ues.json', '-ues.csv'):
            self.assertIn(f'{self.prefix}{suffix}', output)
            self.assertTrue(Path(f'{self.prefix}{suffix}').is_file())
        stdout = io.StringIO()
        with redirect_stdout(stdout), self.assertRaises(SystemExit) as exit_status:
            summarize.main([str(self.source), '--output-prefix', str(self.prefix)])
        self.assertEqual(exit_status.exception.code, 2)
        self.assertEqual(stdout.getvalue(), '')

    def test_cli_reports_bad_input_without_creating_outputs(self):
        self.source.write_text('not json\n')
        with self.assertRaises(SystemExit) as exit_status:
            summarize.main([str(self.source), '--output-prefix', str(self.prefix), '--slo-ms', '10'])
        self.assertEqual(exit_status.exception.code, 2)
        self.assertFalse(self.prefix.with_suffix('.json').exists())
        self.assertFalse(self.prefix.with_suffix('.csv').exists())

    def test_default_summary_selects_only_unambiguous_run(self):
        run = self.directory / 'run-1'
        run.mkdir()
        (run / 'requests.jsonl').write_text(''.join(json.dumps(r) + '\n' for r in records()))
        with patch.object(summarize.defaults, 'RESULTS', self.directory):
            summarize.main([])
            self.assertTrue((run / 'summary.csv').is_file())
            second = self.directory / 'run-2'
            second.mkdir()
            (second / 'requests.jsonl').write_text((run / 'requests.jsonl').read_text())
            with self.assertRaises(SystemExit):
                summarize.main([])
            self.assertFalse((second / 'summary.csv').exists())


if __name__ == '__main__':
    unittest.main()
