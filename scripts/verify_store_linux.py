"""Real Linux Redis/HTTP/ExUnit, isolated namespace, no production configuration."""
import json
from pathlib import Path
import subprocess
import uuid

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / '.local-evidence'
IMAGE = 'sha256:a5ae0c9dfd46a2a1389dd4c1637b1ce7094b7e23dbf655f42d1d6076dc333d6c'
REDIS = 'sha256:ab3bbb60f1b644f11c80b990d900cd4f56509d3a1761cc538eaba614320da469'


def docker(*args, check=True):
    result = subprocess.run(['docker', '--context', 'desktop-linux', *args], capture_output=True, text=True, timeout=120)
    if check and result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result


OUT.mkdir(exist_ok=True)
name = 'lifecycle-redis-' + uuid.uuid4().hex[:10]
worker = 'lifecycle-tests-' + uuid.uuid4().hex[:10]
try:
    docker('image', 'inspect', REDIS)
    docker('run', '-d', '--name', name, '--network', 'none', '--read-only', '--tmpfs', '/data:rw,size=64m',
        '--memory', '128m', '--cpus', '1', '--pids-limit', '64', '--entrypoint', 'redis-server', REDIS,
        '--bind', '127.0.0.1', '--save', '', '--appendonly', 'no')
    result = docker('run', '--name', worker, '--network', 'container:'+name, '--read-only',
        '--tmpfs', '/tmp:rw,nosuid,size=256m', '--memory', '1g', '--cpus', '2', '--pids-limit', '256',
        '--mount', f'type=bind,src={ROOT},dst=/src,readonly', '-e', 'APP_ROLE=none',
        '-e', 'DISPATCH_DISPOSABLE_REDIS_URL=redis://127.0.0.1:6379/0', '--entrypoint', 'elixir', IMAGE,
        '/src/scripts/lifecycle_store_linux.exs', check=False)
    (OUT/'linux-exunit.txt').write_text(result.stdout + result.stderr)
    (OUT/'linux-store-runtime.json').write_text(json.dumps({'redis_image':REDIS, 'worker_image':IMAGE,
        'exit_code':result.returncode, 'network':'none; shared namespace only',
        'state':json.loads(docker('inspect', '--format', '{{json .State}}',worker).stdout)},indent=2))
    print(result.stdout + result.stderr)
    raise SystemExit(result.returncode)
finally:
    for container in (worker, name):
        docker('rm','-f',container,check=False)
