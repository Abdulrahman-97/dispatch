"""Supplementary actual OTP prep_stop tests; HTTP simulated, no Linux/SIGTERM claim."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import urllib.request

root = Path(__file__).resolve().parents[1]
out = root / '.local-evidence'
out.mkdir(exist_ok=True)
results = []
for mode in ('graceful', 'deadline', 'lost_ack', 'heartbeat_recovery', 'scheduler_crash'):
    # Refuse an occupied fixture port rather than touching another service.
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 4999))
    def request(path, data=None):
        body = json.dumps(data).encode() if data is not None else None
        with urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:4999'+path, data=body), timeout=2) as response:
            raw = response.read()
        return json.loads(raw) if raw else None
    flag = out / f'{mode}.stop'
    if flag.exists():
        flag.unlink()
    script = out / f'{mode}.exs'
    script.write_text('Code.prepend_paths(Path.wildcard('+json.dumps((root/'elixir_app/_build/test/lib/*/ebin').as_posix())+'))\n'
        'Application.ensure_all_started(:elixir_app, :permanent)\n'
        'Stream.repeatedly(fn -> receive do after 50 -> :ok end end) |> Enum.find(fn _ -> File.exists?('+json.dumps(flag.as_posix())+') end)\n'
        + ('Process.exit(Process.whereis(Dispatch.Worker.Scheduler), :kill)\n' if mode=='scheduler_crash' else 'System.stop(0)\n')
        + 'Process.sleep(:infinity)\n')
    env = {key: os.environ[key] for key in (
        'PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'USERPROFILE', 'HOMEDRIVE', 'HOMEPATH', 'PATHEXT'
    ) if key in os.environ}
    env['ERL_CRASH_DUMP_SECONDS'] = '0'
    env.update(APP_ROLE='worker', COORDINATOR_URL='http://127.0.0.1:4999', WORKER_CONCURRENCY='2',
        WORKER_POLL_INTERVAL_MS='100', DISPATCH_SHUTDOWN_GRACE_MS='500' if mode=='deadline' else '15000')
    elixir_lib = Path.home()/'scoop/apps/elixir/current/lib'
    server = subprocess.Popen([sys.executable, str(root/'scripts/lifecycle_fixture.py')], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    worker = None
    try:
        for _ in range(30):
            try:
                request('/setup', {'python': sys.executable, 'remote_seconds': 4, 'lost_ack': mode=='lost_ack', 'heartbeat_outage': mode=='heartbeat_recovery'})
                break
            except OSError:
                time.sleep(.1)
        with (out/f'{mode}.log').open('w') as log:
            worker = subprocess.Popen([str(root/'.local-tools/erl.exe'), '-noshell', '-elixir_root', str(elixir_lib),
                '-pa', str(elixir_lib/'elixir/ebin'), '-s', 'elixir', 'start_cli', '-extra', str(script)],
                env=env, stdout=log, stderr=subprocess.STDOUT)
            for _ in range(100):
                if any(e['path']=='/remote' for e in request('/events')):
                    break
                if worker.poll() is not None:
                    raise RuntimeError('Worker exited early; see log')
                time.sleep(.1)
            else:
                raise RuntimeError('Remote not started')
            stop_at = time.time()
            flag.write_text('stop')
            code = worker.wait(timeout=25)
            duration = time.time()-stop_at
        for _ in range(60):
            events = request('/events')
            if any(e['path']=='remote_complete' for e in events):
                break
            time.sleep(.1)
        log = (out/f'{mode}.log').read_text()
        assert (code != 0 if mode=='scheduler_crash' else code == 0), log
        if mode!='scheduler_crash':
            assert 'worker_drain_acknowledged' in log
        assert sum(e['path']=='/remote' for e in events)==1
        assert not any(e['path']=='/internal/poll' and e['at']>stop_at+.1 for e in events)
        if mode=='scheduler_crash':
            assert not any(e['path']=='/internal/result' for e in events)
        elif mode=='deadline':
            assert 'worker_drain_deadline' in log
            assert any(e['path']=='/internal/interrupted' for e in events)
        else:
            assert any(e['path']=='/internal/result' for e in events)
            assert not any(e['path']=='/internal/interrupted' for e in events)
        if mode=='lost_ack':
            assert sum(e['path']=='/internal/result' for e in events)>=2
        results.append({'mode': mode, 'exit_code': code, 'stop_seconds': duration, 'events': events,
            'platform': 'Windows OTP 28; actual application stop; SIMULATED HTTP'})
    finally:
        if worker and worker.poll() is None:
            worker.terminate()
            worker.wait(timeout=10)
        server.terminate()
        server.wait(timeout=10)
        (out/'native-prep-stop.json').write_text(json.dumps(results, indent=2))
print(f'{len(results)} native prep_stop cases passed (not Linux SIGTERM verification)')
