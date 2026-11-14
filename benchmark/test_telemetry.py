import http.client
import http.server
import json
import threading
from urllib.parse import urlsplit

from benchmark.client import attempt
from benchmark.telemetry import TelemetryPublisher, request_with_telemetry
from collector.server import Collector, load_settings, make_handler


def test_source_bound_delivery_and_failures(capsys):
    seen = []
    class App(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            seen.append(self.client_address[0])
            self.send_response(200 if self.client_address[0] == '127.0.0.1' else 503)
            self.end_headers()
        def log_message(self, *args):
            pass
    collector = Collector(load_settings('collector/config.yaml'))
    observations = []
    ingest = collector.ingest
    def capture(observation):
        observations.append(observation)
        return ingest(observation)
    collector.ingest = capture
    class Handler(make_handler(collector, 'secret')):
        def do_POST(self):
            seen.append(self.client_address[0])
            super().do_POST()
        def log_message(self, *args):
            pass
    servers = [http.server.ThreadingHTTPServer(('127.0.0.1', 0), handler) for handler in (App, Handler)]
    threads = [threading.Thread(target=s.serve_forever, daemon=True) for s in servers]
    for thread in threads:
        thread.start()
    try:
        app = urlsplit(f'http://127.0.0.1:{servers[0].server_port}/')
        telemetry = urlsplit(f'http://127.0.0.1:{servers[1].server_port}/observations')
        publisher = TelemetryPublisher((telemetry, 'secret'), ['127.0.0.1', '127.0.0.2'])
        first = request_with_telemetry(attempt, publisher, app, 'GET', None, {}, '127.0.0.1', 1)
        second = request_with_telemetry(attempt, publisher, app, 'GET', None, {}, '127.0.0.2', 1)
        publisher.close()
        assert first['success'] and not second['success']
        assert seen.count('127.0.0.1') == seen.count('127.0.0.2') == 2
        assert {item['http_status'] for item in observations} == {200, 503}
        assert all(item['duration_ms'] >= 0 and 'duration_seconds' not in item for item in observations)
        assert 'ue_request_window_attempts 2\n' in collector.metrics()
        assert 'ue_request_window_successes 1\n' in collector.metrics()
        bad = TelemetryPublisher((telemetry, 'bad'), ['127.0.0.1'])
        assert request_with_telemetry(attempt, bad, app, 'GET', None, {}, '127.0.0.1', 1)['success']
        bad.close()
        assert bad.dropped == 1 and bad.delivered == 0
        assert 'HTTP 401' in capsys.readouterr().err
        connection = http.client.HTTPConnection('127.0.0.1', servers[1].server_port)
        connection.request('POST', '/observations', '{bad', {'Authorization': 'Bearer secret'})
        assert connection.getresponse().status == 400
        connection.close()
    finally:
        for server in servers:
            server.shutdown()
            server.server_close()
        for thread in threads:
            thread.join()



def test_saturation_and_drain_are_accounted(capsys):
    entered = threading.Event()
    release = threading.Event()
    class Slow(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            entered.set()
            release.wait(2)
            self.send_response(202)
            self.end_headers()
        def log_message(self, *args):
            pass
    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Slow)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    publisher = TelemetryPublisher((urlsplit(f'http://127.0.0.1:{server.server_port}/observations'), 'x'),
                                   ['127.0.0.1'], capacity=1)
    result = {'duration_ms': 1.0, 'http_status': 200, 'success': True}
    try:
        publisher.submit('127.0.0.1', result)
        assert entered.wait(1)
        publisher.submit('127.0.0.1', result)
        publisher.submit('127.0.0.1', result)
        assert publisher.dropped == 1
        release.set()
        publisher.close()
        assert publisher.delivered == 2
        assert all(not thread.is_alive() for thread in publisher.threads)
        assert 'queue full' in capsys.readouterr().err
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        thread.join()