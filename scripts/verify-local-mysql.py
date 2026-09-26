#!/usr/bin/env python3
"""Real Compose + executable JAR acceptance tests. Requires free ports 8080/8081.
Run after ./gradlew build and docker compose up -d --wait mysql kafka.
Stops only application processes it starts; Compose services remain for inspection.
"""
import datetime
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
RUN = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
OUT = ROOT / 'docs' / ('local-mysql-evidence-' + RUN)
OUT.mkdir(parents=True)
TOPIC = 'outbox.tc.' + RUN.lower()
results = []
processes = []
logs = []
kafka_stopped = False

def command(*args, timeout=90):
    p = subprocess.run(args, text=True, capture_output=True, timeout=timeout)
    if p.returncode:
        raise RuntimeError(f'{args}: {p.stdout}\n{p.stderr}')
    return p.stdout.strip()

def compose(*args):
    return command('docker', 'compose', *args)

def sql(query):
    return compose('exec', '-T', '-e', 'MYSQL_PWD=outbox', 'mysql', 'mysql',
                   '--default-character-set=utf8mb4', '-uoutbox', '-D', 'outbox', '-N', '-B', '-e', query)

def http(path, body=None, port=8080):
    data = None if body is None else json.dumps(body, ensure_ascii=False).encode()
    req = urllib.request.Request(f'http://localhost:{port}{path}', data=data,
                                 headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())

def wait(description, predicate, timeout=75):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except Exception as e:
            last = str(e)
        time.sleep(0.5)
    raise AssertionError(f'Timeout: {description}; last error={last}')

def record(case, expected, actual):
    results.append({'id': case, 'status': 'PASS', 'expected': expected, 'actual': actual})
    print(json.dumps(results[-1], ensure_ascii=False), flush=True)
    save()

def save():
    (OUT / 'results.json').write_text(json.dumps({'run': RUN, 'topic': TOPIC,
        'results': results}, ensure_ascii=False, indent=2) + '\n')

def start(app, owner):
    log = open(OUT / (owner + '.log'), 'w')
    logs.append(log)
    p = subprocess.Popen(['java', '-Duser.timezone=UTC', '-jar',
        f'app/{app}/build/libs/{app}-0.0.1-SNAPSHOT.jar',
        f'--outbox.lock-owner-id={owner}', f'--orders.topic={TOPIC}'],
        stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    processes.append(p)
    port = 8080 if app == 'api' else 8081
    wait(app + ' health', lambda: p.poll() is None and http('/actuator/health', port=port)[1].get('status') == 'UP')
    return p

def post(name):
    code, body = http('/api/orders', {'productName': RUN + '-' + name, 'quantity': 2})
    assert code == 201, (code, body)
    return body

def row(event):
    return sql("select o.id, o.`key`, if(o.processed is null,'PENDING','PROCESSED'), "
               "b.product_name, b.quantity from outbox_kafka o join orders b on o.`key`=b.id "
               f"where b.id='{event['orderId']}'")

def processed(event):
    return 'PROCESSED' in row(event)

def owner():
    return sql('select owner_id from outbox_kafka_lock')

api_owner = 'tc-api-' + RUN
worker_owner = 'tc-worker-' + RUN
try:
    for port in (8080, 8081):
        with socket.socket() as s:
            assert s.connect_ex(('127.0.0.1', port)) != 0, f'Port {port} is already in use'
    compose('exec', '-T', 'kafka', '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server',
            'localhost:19092', '--create', '--topic', TOPIC, '--partitions', '1', '--replication-factor', '1')
    api = start('api', api_owner)
    wait('API owns lock', lambda: owner() == api_owner)
    worker = start('worker', worker_owner)
    assert owner() == api_owner
    record('TC-L01', 'Both apps healthy, migrations applied, API owns lock', {
        'apiHealth': http('/actuator/health'), 'workerHealth': http('/actuator/health', port=8081),
        'mysql': sql('select version(), @@session.time_zone'), 'owner': owner(),
        'migrations': sql('select version, description, success from flyway_schema_history order by installed_rank')})
    first = post('정상-주문')
    wait('first event processed', lambda: processed(first))
    record('TC-L02', 'HTTP 201, order + event persisted, processed set', {'response': first, 'row': row(first)})
    before = sql('select (select count(*) from orders), (select count(*) from outbox_kafka)')
    code, body = http('/api/orders', {'productName': '', 'quantity': 0})
    after = sql('select (select count(*) from orders), (select count(*) from outbox_kafka)')
    assert code == 400 and before == after, (code, before, after)
    record('TC-L03', 'Invalid input returns 400 and saves no records', {'http': code, 'before': before, 'after': after})
    compose('stop', 'kafka')
    kafka_stopped = True
    pending = post('장애-복구')
    samples = []
    deadline = time.monotonic() + 12
    while time.monotonic() < deadline:
        value = row(pending)
        assert 'PENDING' in value, value
        samples.append(value)
        time.sleep(1)
    record('TC-L04', 'Kafka unavailable: HTTP 201 and pending retained beyond 10s delivery timeout',
           {'response': pending, 'samples': len(samples), 'row': samples[-1]})
    os.killpg(api.pid, signal.SIGKILL)
    api.wait(timeout=10)
    started = time.monotonic()
    wait('worker takes expired lock', lambda: owner() == worker_owner)
    record('TC-L05', 'After API SIGKILL worker takes lock and pending data survives',
           {'owner': owner(), 'takeoverSeconds': round(time.monotonic()-started, 2), 'row': row(pending)})
    compose('start', 'kafka')
    kafka_stopped = False
    wait('worker processes pending event', lambda: processed(pending), timeout=90)
    record('TC-L06', 'Kafka recovery causes worker to publish pending event', {'owner': owner(), 'row': row(pending)})
    api = start('api', api_owner + '-restart')
    assert owner() == worker_owner
    third = post('재시작-주문')
    wait('restarted API event processed', lambda: processed(third))
    record('TC-L07', 'Restarted API saves orders; current worker retains lock and publishes',
           {'owner': owner(), 'response': third, 'row': row(third)})
    messages = compose('exec', '-T', 'kafka', '/opt/kafka/bin/kafka-console-consumer.sh',
        '--bootstrap-server', 'localhost:19092', '--topic', TOPIC, '--from-beginning',
        '--max-messages', '3', '--timeout-ms', '20000', '--property', 'print.key=true',
        '--property', 'print.headers=true')
    (OUT / 'kafka-messages.txt').write_text(messages + '\n')
    for event in (first, pending, third):
        assert event['orderId'] in messages, (event, messages)
    assert messages.count('x-source:outbox-pattern') == 3, messages
    assert messages.count('x-sequence:') == 3, messages
    assert messages.count('event-type:OrderCreated') == 3, messages
    record('TC-L08', 'Real Kafka consumer receives all 3 order IDs and source/sequence/type headers',
           {'orders': [e['orderId'] for e in (first, pending, third)], 'messages': messages})
    (OUT / 'compose-status.txt').write_text(compose('ps', '-a') + '\n')
except Exception as e:
    results.append({'id': 'EXECUTION', 'status': 'FAIL', 'actual': repr(e)})
    save()
    raise
finally:
    if kafka_stopped:
        compose('start', 'kafka')
    for p in processes:
        if p.poll() is None:
            os.killpg(p.pid, signal.SIGTERM)
            try:
                p.wait(timeout=25)
            except subprocess.TimeoutExpired:
                os.killpg(p.pid, signal.SIGKILL)
                p.wait(timeout=10)
    for log in logs:
        log.close()
    save()
    print('Evidence: ' + str(OUT), flush=True)
