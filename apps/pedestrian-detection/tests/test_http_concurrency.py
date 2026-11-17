"""Exercise the actual Gunicorn configuration without model downloads."""
from concurrent.futures import ThreadPoolExecutor
import io
import json
import os
from pathlib import Path
import runpy
import socket
import subprocess
import sys
import time
from urllib.request import Request, urlopen

from PIL import Image
import pytest

APP_DIR = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('value,threads,worker', [
    (None, 5, 'gthread'), ('1', 1, 'sync'), ('2', 2, 'gthread'),
])
def test_http_threads_configuration(monkeypatch, value, threads, worker):
    monkeypatch.delenv('HTTP_THREADS', raising=False)
    if value is not None:
        monkeypatch.setenv('HTTP_THREADS', value)
    config = runpy.run_path(str(APP_DIR / 'gunicorn.conf.py'))
    assert (config['workers'], config['threads'], config['worker_class']) == (1, threads, worker)


@pytest.mark.parametrize('value', ['0', '-1', 'abc', '1.5'])
def test_invalid_http_threads_rejected(monkeypatch, value):
    monkeypatch.setenv('HTTP_THREADS', value)
    with pytest.raises(ValueError):
        runpy.run_path(str(APP_DIR / 'gunicorn.conf.py'))


def test_gunicorn_serves_requests_during_slow_upload_and_serializes_inference(tmp_path):
    pytest.importorskip('gunicorn')
    # Log detector entry/exit to verify the application lock under real HTTP load.
    wrapper = tmp_path / 'concurrency_app.py'
    wrapper.write_text('''
import threading
import time
from app import create_app

class Detector:
    model_sha256 = 'test-model'
    size = 320
    active = 0
    maximum = 0
    guard = threading.Lock()

    def predict(self, image):
        with self.guard:
            self.active += 1
            self.maximum = max(self.maximum, self.active)
        try:
            time.sleep(0.1)
            return []
        finally:
            with self.guard:
                self.active -= 1

detector = Detector()
app = create_app(detector, health_port=0)

@app.get('/concurrency')
def concurrency():
    return {'maximum': detector.maximum}
''')
    image = io.BytesIO()
    Image.new('RGB', (32, 32), 'white').save(image, format='JPEG')
    boundary = 'concurrency-test'
    body = (f'--{boundary}\r\nContent-Disposition: form-data; name="image"; '
            'filename="test.jpg"\r\nContent-Type: image/jpeg\r\n\r\n').encode()
    body += image.getvalue() + f'\r\n--{boundary}--\r\n'.encode()
    env = {**os.environ, 'PYTHONPATH': os.pathsep.join([str(APP_DIR), str(tmp_path)]),
           'HTTP_THREADS': '5', 'GUNICORN_CMD_ARGS': ''}
    with socket.socket() as listener, (tmp_path / 'gunicorn.log').open('w+') as log:
        listener.bind(('127.0.0.1', 0))
        listener.listen()
        port = listener.getsockname()[1]
        base = f'http://127.0.0.1:{port}'
        process = subprocess.Popen([
            sys.executable, '-m', 'gunicorn', '--config', str(APP_DIR / 'gunicorn.conf.py'),
            '--bind', f'fd://{listener.fileno()}', 'concurrency_app:app',
        ], env=env, pass_fds=(listener.fileno(),), stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 10
            while True:
                try:
                    with urlopen(base + '/ready', timeout=0.5) as response:
                        assert response.status == 200
                    break
                except OSError:
                    if process.poll() is not None or time.monotonic() > deadline:
                        log.seek(0)
                        pytest.fail('Gunicorn did not become ready: ' + log.read())
                    time.sleep(0.05)

            with socket.create_connection(('127.0.0.1', port), timeout=3) as slow:
                # This request enters the handler, then blocks reading its body.
                slow.sendall((f'POST /detect HTTP/1.1\r\nHost: localhost\r\n'
                              f'Content-Type: multipart/form-data; boundary={boundary}\r\n'
                              f'Content-Length: {len(body)}\r\n'
                              'Expect: 100-continue\r\nConnection: close\r\n\r\n').encode())
                assert b'100 Continue' in slow.recv(4096)

                def detect():
                    req = Request(base + '/detect', data=body, headers={
                        'Content-Type': f'multipart/form-data; boundary={boundary}',
                    })
                    with urlopen(req, timeout=3) as response:
                        return json.load(response)

                with ThreadPoolExecutor(max_workers=2) as pool:
                    futures = [pool.submit(detect) for _ in range(2)]
                    results = [future.result(timeout=4) for future in futures]
                assert all(r['person_count'] == 0 for r in results)
                assert max(r['lock_wait_ms'] for r in results) > 0
                with urlopen(base + '/concurrency', timeout=1) as response:
                    assert json.load(response)['maximum'] == 1
                # Release the slow upload before terminating the server.
                slow.sendall(body)
                assert b'200 OK' in slow.recv(4096)
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
