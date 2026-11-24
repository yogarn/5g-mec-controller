"""Hold the campaign's inter-node path delay open until Ctrl-C.

TrafficDelay keeps the names of the IFB devices it creates in memory, so the delay has to be
started and removed by the same process. Run from the project root on core, in its own terminal:

    export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
    .venv/bin/python deploy/final/tools/hold_tc_delay.py 10

It installs the same filters the campaign installs for --tc-ms 10 (TCP port 5000 to/from the
Calico pool mec-campaign-reserved), re-checks them every 10 s like the campaign does during traffic,
and removes them on Ctrl-C or SIGTERM.
"""
import signal
import sys
import threading

sys.path.insert(0, '.')
from utils.traffic_control import TrafficDelay  # noqa: E402


def main():
    delay_ms = float(sys.argv[1]) if len(sys.argv) > 1 else 10.0
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    delay = TrafficDelay(delay_ms)
    delay.start()
    print(f'tc delay {delay_ms:g} ms active. Ctrl-C removes it.', flush=True)
    try:
        while not stop.wait(10):
            delay.check()
    finally:
        delay.close()
        print('tc delay removed.', flush=True)


if __name__ == '__main__':
    main()
