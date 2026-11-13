"""Strict Prometheus instant-vector collection; no synthesized observations."""
import math
import time
import requests


class Prometheus:
    def __init__(self, url: str, max_age: float):
        self.url, self.max_age = url, max_age
        self.session = requests.Session()

    def vector(self, query: str) -> list[tuple[dict, float]]:
        response = self.session.get(self.url + '/api/v1/query', params={'query': query}, timeout=10)
        response.raise_for_status()
        payload = response.json()
        if payload.get('status') != 'success' or payload.get('data', {}).get('resultType') != 'vector':
            raise ValueError('Prometheus query did not return a successful instant vector')
        result = []
        for series in payload['data']['result']:
            stamp, raw = series['value']
            value, stamp = float(raw), float(stamp)
            age = time.time() - stamp
            if not math.isfinite(stamp) or age > self.max_age or age < -10:
                raise ValueError('invalid or stale Prometheus sample')
            if math.isnan(value):
                raise ValueError('Prometheus sample is NaN')
            if not math.isfinite(value):
                raise ValueError('invalid or stale Prometheus sample')
            result.append((series['metric'], value))
        if not result:
            raise ValueError('Prometheus query returned no samples')
        return result

    def scalar(self, query: str) -> float:
        values = self.vector(query)
        if len(values) != 1:
            raise ValueError('latency query must return exactly one series')
        return values[0][1]

    def per_node(self, query: str, nodes: tuple[str, ...]) -> dict[str, float]:
        values = {}
        for labels, value in self.vector(query):
            name = labels.get('node')
            if name not in nodes or name in values or value < 0:
                raise ValueError('node query must return one nonnegative series per eligible node, labelled node')
            values[name] = value
        if set(values) != set(nodes):
            raise ValueError('node query is missing eligible nodes')
        return values
