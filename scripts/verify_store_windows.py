"""Supplementary real Redis tests on local Windows; not Linux lifecycle evidence."""
from pathlib import Path
import os
import socket
import subprocess
import time

root = Path(__file__).resolve().parents[1]
out = root / '.local-evidence'
out.mkdir(exist_ok=True)
with socket.socket() as sock:
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
config = out / 'redis.conf'
config.write_text(f'bind 127.0.0.1\nport {port}\nsave ""\nappendonly no\nprotected-mode yes\n')
redis = Path.home() / 'scoop/apps/redis/8.6.2/redis-server.exe'
env = {key: os.environ[key] for key in (
    'PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'USERPROFILE', 'HOMEDRIVE', 'HOMEPATH', 'PATHEXT'
) if key in os.environ}
env.update(APP_ROLE='none', MIX_ENV='test', TEMP=str(root/'.local-tools'), TMP=str(root/'.local-tools'),
    DISPATCH_DISPOSABLE_REDIS_URL=f'redis://127.0.0.1:{port}/0')
env['PATH'] = str(root/'.local-tools') + os.pathsep + env['PATH']
with (out/'redis.log').open('w') as log:
    server = subprocess.Popen([str(redis), 'redis.conf'], cwd=out, stdout=log, stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NO_WINDOW)
    try:
        for _ in range(40):
            if server.poll() is not None:
                raise RuntimeError('Disposable Redis failed to start; inspect redis.log')
            with socket.socket() as probe:
                probe.settimeout(.2)
                if probe.connect_ex(('127.0.0.1', port)) == 0:
                    break
            time.sleep(.1)
        with (out/'exunit-store.txt').open('w') as result:
            completed = subprocess.run(['cmd', '/c', 'mix', 'test'], cwd=root/'elixir_app', env=env,
                stdout=result, stderr=subprocess.STDOUT, timeout=90)
        print((out/'exunit-store.txt').read_text())
        raise SystemExit(completed.returncode)
    finally:
        server.terminate()
        server.wait(timeout=10)
