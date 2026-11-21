"""Fixed-rate traffic across phase boundaries, pausing only changed subscribers."""
from concurrent.futures import ThreadPoolExecutor
import threading
import time

from utils.ue_lifecycle import plan_assignments


def run_continuous_phases(phases, durations, lifecycle, images, assignments, urls,
                          rps, max_in_flight, dispatch, emit, publisher_factory=None,
                          initial_probe=None):
    plans, offsets = [], [0.0]
    previous = {}
    for phase, duration in zip(phases, durations):
        previous = plan_assignments(previous, [phase[f'gnb{s}_ues'] for s in (1, 2, 3)])
        plans.append(previous)
        offsets.append(offsets[-1] + duration)
    count = max(len(p) for p in plans)
    condition = threading.Condition()
    stop = threading.Event()
    states = [{'session': None, 'publisher': None, 'pending': 0} for _ in range(count)]
    stats = [{i: dict(scheduled=0, started=0, completed=0, successes=0, missed=0,
                      transition_missed=0) for i in plan} for plan in plans]
    phase_pending = [0] * len(phases)
    scheduler_done = [0] * len(phases)
    announced = [False] * len(phases)
    errors = []
    active_phase = [0]

    def before_change(index):
        with condition:
            state = states[index]
            old = state['session']
            state['session'] = None
            emit({'event': 'ue_transition_start', 'ue_index': index,
                  'phase': phases[active_phase[0]]['phase'],
                  'start_time': time.time(), 'previous_site': old['site'] if old else None})
            while state['pending'] and not stop.is_set():
                condition.wait(0.05)
            if stop.is_set():
                raise RuntimeError('traffic stopped during UE transition')
            publisher, state['publisher'] = state['publisher'], None
        if publisher is not None:
            publisher.close()

    def after_change(index, session):
        publisher = publisher_factory(session['ip']) if publisher_factory and session else None
        with condition:
            states[index]['session'] = session
            states[index]['publisher'] = publisher
            emit({'event': 'ue_transition_end', 'ue_index': index, 'end_time': time.time(),
                  'phase': phases[active_phase[0]]['phase'],
                  'site': session['site'] if session else None,
                  'source_address': session['ip'] if session else None,
                  'interface': session.get('iface') if session else None})
            condition.notify_all()

    def fail(exc):
        with condition:
            errors.append(exc)
            stop.set()
            condition.notify_all()

    def check():
        if errors:
            raise RuntimeError('continuous traffic worker failed') from errors[0]

    def phase_start(pi):
        plan, phase = plans[pi], phases[pi]
        # Keep the same order for UE phase offsets and exact slot reconstruction.
        order = sorted(plan, key=lambda i: (plan[i], i))
        ues = [{'ue_index': i, 'group': f'gnb-{plan[i] + 1}',
                'source_address': lifecycle.subscribers[i][plan[i]]['ip']}
               for i in order]
        emit({'event': 'phase_start', **phase, 'start_time': wall_start + offsets[pi],
              'duration_s': durations[pi], 'rps_per_ue': rps,
              'max_in_flight_per_ue': max_in_flight, 'ues': ues,
              'phase_transition': 'continuous',
              'switching': 'continuous-unaffected; moving UEs stop-and-reattach'})
        with condition:
            announced[pi] = True
            condition.notify_all()
        print(f"[RUN] {phase['phase']}: {pi + 1}/{len(phases)}; continuous traffic; "
              f"{len(plan)} UEs for {durations[pi]:g}s at {rps:g} RPS/UE", flush=True)

    def request(pi, index, slot, slot_time, session, publisher):
        stat = stats[pi][index]
        try:
            actual = time.monotonic()
            with condition:
                if stop.is_set() or actual >= mono_start + offsets[pi + 1] or actual > slot_time + period * 0.25:
                    stat['missed'] += 1
                    return
                stat['started'] += 1
            image_index = assignments[index]
            image = images[image_index]
            group = f"gnb-{session['site'] + 1}"
            try:
                result = dispatch(urls[group], image, session['ip'], publisher, site=group)
            except TypeError:
                result = dispatch(urls[group], image, session['ip'], publisher)
            with condition:
                stat['completed'] += 1
                stat['successes'] += int(result['success'])
                emit({'event': 'request', 'phase': phases[pi]['phase'], 'group': group,
                      'source_address': session['ip'], 'ue_index': index,
                      'url': urls[group].geturl() if hasattr(urls[group], 'geturl') else urls[group],
                      'method': 'POST', 'image_index': image_index,
                      'image_filename': image['filename'], 'slot': slot,
                      'scheduled_offset_s': slot_time - mono_start - offsets[pi],
                      'dispatch_offset_s': actual - mono_start - offsets[pi],
                      'attempt': stat['completed'], **result})
        except Exception as exc:
            fail(exc)
        finally:
            with condition:
                states[index]['pending'] -= 1
                phase_pending[pi] -= 1
                condition.notify_all()

    def scheduler(index):
        try:
            with ThreadPoolExecutor(max_workers=max_in_flight) as pool:
                futures = []
                for pi, plan in enumerate(plans):
                    if index not in plan:
                        continue
                    order = sorted(plan, key=lambda i: (plan[i], i))
                    offset = order.index(index) / len(plan) * period
                    slot = 0
                    while offset + slot * period < durations[pi]:
                        due = mono_start + offsets[pi] + offset + slot * period
                        if stop.wait(max(0, due - time.monotonic())):
                            return
                        with condition:
                            while not announced[pi] and not stop.is_set():
                                condition.wait(0.01)
                            if stop.is_set():
                                return
                            stat = stats[pi][index]
                            stat['scheduled'] += 1
                            state = states[index]
                            session = state['session']
                            unavailable = session is None or session['site'] != plan[index]
                            if unavailable or time.monotonic() > due + period * 0.25 or state['pending'] >= max_in_flight:
                                stat['missed'] += 1
                                stat['transition_missed'] += int(unavailable)
                            else:
                                state['pending'] += 1
                                phase_pending[pi] += 1
                                futures.append(pool.submit(request, pi, index, slot, due,
                                                           dict(session), state['publisher']))
                        slot += 1
                    with condition:
                        scheduler_done[pi] += 1
                        condition.notify_all()
                    # Exceptions are observed promptly without waiting for all requests.
                    futures = [future for future in futures if not future.done()]
                for future in futures:
                    future.result()
        except Exception as exc:
            fail(exc)

    def phase_end(pi):
        if stop.wait(max(0, mono_start + offsets[pi + 1] - time.monotonic())):
            return
        with condition:
            while (scheduler_done[pi] < len(plans[pi]) or phase_pending[pi]) and not stop.is_set():
                condition.wait(0.05)
            if stop.is_set():
                return
            per_ue = []
            for i, counters in stats[pi].items():
                s = dict(counters)
                s.update(ue_index=i, group=f'gnb-{plans[pi][i] + 1}',
                         source_address=lifecycle.subscribers[i][plans[pi][i]]['ip'],
                         attempts=s['completed'], failures=s['completed'] - s['successes'],
                         actual_start_rate=s['started'] / durations[pi],
                         request_coverage=s['completed'] / s['scheduled'] if s['scheduled'] else None)
                per_ue.append(s)
            totals = {k: sum(s[k] for s in per_ue)
                      for k in ('scheduled', 'started', 'completed', 'successes', 'failures', 'missed', 'transition_missed')}
            emit({'event': 'phase_end', **phases[pi], **totals,
                  'start_time': wall_start + offsets[pi], 'end_time': wall_start + offsets[pi + 1],
                  'drain_completed_time': time.time(),
                  'generation_duration_s': durations[pi], 'rps_per_ue': rps,
                  'max_in_flight_per_ue': max_in_flight, 'attempts': totals['completed'],
                  'actual_start_rate': totals['started'] / durations[pi],
                  'request_coverage': totals['completed'] / totals['scheduled'] if totals['scheduled'] else None,
                  'per_ue': per_ue})
            print(f"[--] {phases[pi]['phase']}: {totals['completed']} completed, "
                  f"{totals['failures']} failed, {totals['missed']} missed "
                  f"({totals['transition_missed']} during attachment)", flush=True)

    period = 1.0 / rps
    threads = []
    try:
        lifecycle.reconcile(plans[0], before_change, after_change)
        if initial_probe:
            initial_probe(lifecycle.sessions)
        wall_start, mono_start = time.time(), time.monotonic()
        phase_start(0)
        def guarded(fn, *args):
            try:
                fn(*args)
            except Exception as exc:
                fail(exc)
        threads = [threading.Thread(target=guarded, args=(scheduler, i), name=f'ue-pacer-{i}') for i in range(count)]
        threads += [threading.Thread(target=guarded, args=(phase_end, pi), name=f'phase-end-{pi}') for pi in range(len(phases))]
        for thread in threads:
            thread.start()
        for pi in range(1, len(phases)):
            stop.wait(max(0, mono_start + offsets[pi] - time.monotonic()))
            check()
            active_phase[0] = pi
            phase_start(pi)
            lifecycle.reconcile(plans[pi], before_change, after_change)
            if time.monotonic() >= mono_start + offsets[pi + 1]:
                raise RuntimeError('UE transition exceeded its traffic phase duration')
        while any(thread.is_alive() for thread in threads):
            check()
            for thread in threads:
                thread.join(timeout=0.05)
        check()
    finally:
        stop.set()
        with condition:
            condition.notify_all()
        for thread in threads:
            if thread.ident is not None:
                thread.join()
        for state in states:
            if state['publisher'] is not None:
                state['publisher'].close()
