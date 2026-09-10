"""Local Docker only; immutable preinstalled test image, no build/pull or production config.

Fixture container has --network none. Worker joins only that network namespace.
Both root filesystems are read-only, with bounded tmpfs; no credential mounts.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

IMAGE = "sha256:a5ae0c9dfd46a2a1389dd4c1637b1ce7094b7e23dbf655f42d1d6076dc333d6c"
ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", default="desktop-linux")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.context != "desktop-linux":
        parser.error("this harness is restricted to the local desktop-linux context")
    def docker(*command):
        result = subprocess.run(["docker", "--context", args.context, *command], capture_output=True, text=True, timeout=60)
        if result.returncode:
            raise RuntimeError(result.stderr)
        return result.stdout.strip()
    global IMAGE
    if os.environ.get("GITHUB_ACTIONS") == "true":
        IMAGE = os.environ["DISPATCH_TEST_IMAGE_ID"]
        assert IMAGE.startswith("sha256:")
    docker("image", "inspect", IMAGE)
    results = []
    for mode in ("sigterm", "docker_stop", "deadline", "kill", "docker_kill_deadline", "lost_ack", "heartbeat_recovery"):
        suffix = uuid.uuid4().hex[:10]
        fixture, worker = f"lifecycle-fixture-{suffix}", f"lifecycle-worker-{suffix}"
        common = ["--read-only", "--tmpfs", "/tmp:rw,nosuid,size=128m", "--memory", "1g", "--cpus", "2", "--pids-limit", "256",
                  "--mount", f"type=bind,src={ROOT},dst=/src,readonly"]
        def request(path, payload=None):
            code = "import urllib.request,json; r=urllib.request.Request('http://127.0.0.1:4999' + " + repr(path)
            if payload is not None:
                code += ",data=" + repr(json.dumps(payload).encode())
            code += "); print(urllib.request.urlopen(r,timeout=3).read().decode())"
            value = docker("exec", fixture, "python3", "-c", code)
            return json.loads(value) if value else None
        try:
            docker("run", "-d", "--name", fixture, "--network", "none", *common, "--entrypoint", "python3", IMAGE, "/src/scripts/lifecycle_fixture.py")
            for _ in range(30):
                try:
                    request("/setup", {"remote_seconds": 5, "lost_ack": mode == "lost_ack", "heartbeat_outage": mode == "heartbeat_recovery"})
                    break
                except RuntimeError:
                    time.sleep(.1)
            docker("run", "-d", "--name", worker, "--network", f"container:{fixture}", *common,
                "-e", "APP_ROLE=worker", "-e", "COORDINATOR_URL=http://127.0.0.1:4999",
                "-e", "WORKER_CONCURRENCY=2", "-e", "WORKER_POLL_INTERVAL_MS=100",
                "-e", f"DISPATCH_SHUTDOWN_GRACE_MS={500 if mode == 'deadline' else 15000}",
                "--entrypoint", "elixir", IMAGE, "/src/scripts/lifecycle_worker.exs")
            for _ in range(100):
                events = request("/events")
                if any(e["path"] == "/remote" for e in events):
                    break
                time.sleep(.1)
            else:
                raise AssertionError("remote never started: " + docker("logs", worker))
            started = time.time()
            if mode == "kill":
                docker("kill", "--signal", "KILL", worker)
            elif mode == "sigterm":
                docker("kill", "--signal", "TERM", worker)
                docker("wait", worker)
            elif mode == "docker_kill_deadline":
                docker("stop", "--time", "1", worker)
            else:
                docker("stop", "--time", "22", worker)
            duration = time.time() - started
            state = json.loads(docker("inspect", "--format", "{{json .State}}", worker))
            log = docker("logs", worker)
            for _ in range(70):
                events = request("/events")
                if any(e["path"] == "remote_complete" for e in events):
                    break
                time.sleep(.1)
            assert any(e["path"] == "remote_complete" for e in events)
            assert sum(e["path"] == "/remote" for e in events) == 1
            # Spare capacity remains. No continued polling after local shutdown
            # has been delivered (allow the already in-flight loopback request).
            assert not any(e["path"] == "/internal/poll" and e["at"] > started + .25 for e in events)
            if mode in {"kill", "docker_kill_deadline"}:
                assert state["ExitCode"] == 137
            elif mode == "deadline":
                assert state["ExitCode"] == 0 and duration < 8
                assert "worker_drain_deadline" in log
                assert any(e["path"] == "/internal/interrupted" for e in events)
            else:
                assert state["ExitCode"] == 0 and "worker_drain_acknowledged" in log
                assert any(e["path"] == "/internal/result" for e in events)
                assert not any(e["path"] == "/internal/interrupted" for e in events)
                assert any(e["path"] == "/internal/heartbeat" and e["at"] > started for e in events)
            if mode == "lost_ack":
                assert sum(e["path"] == "/internal/result" for e in events) >= 2
            results.append({"mode": mode, "duration": duration, "docker_state": state, "events": events,
                "log": log, "remote_evidence": "SIMULATED HTTP; actual Docker lifecycle"})
        finally:
            for name in (worker, fixture):
                subprocess.run(["docker", "--context", args.context, "rm", "-f", name], capture_output=True)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(results, indent=2))
    print(f"{len(results)} lifecycle scenarios passed")


if __name__ == "__main__":
    main()
