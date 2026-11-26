import os
import re
import sys
import json
import time
import queue
import base64
import shutil
import signal
import hashlib
import threading
import subprocess
import importlib.metadata
from urllib.parse import urlparse, parse_qs
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", 3000))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.join(BASE_DIR, "workspace")
START_FILE = os.path.join(BASE_DIR, ".startcmd")
REQ_HASH_FILE = os.path.join(BASE_DIR, ".req_hash")

os.makedirs(WORKSPACE, exist_ok=True)

if os.path.exists(START_FILE):
    with open(START_FILE, "r", encoding="utf-8") as f:
        start_cmd = f.read().strip() or "python3 main.py"
else:
    start_cmd = "python3 main.py"

server_proc = None
is_starting = False
stop_requested = False
proc_lock = threading.Lock()
sse_clients = set()
sse_lock = threading.Lock()


def broadcast(msg_type, text):
    payload = {"type": msg_type, "text": text}
    with sse_lock:
        for q in list(sse_clients):
            try:
                q.put_nowait(payload)
            except Exception:
                pass


def safe_join(rel=""):
    rel = (rel or "").lstrip("/\\")
    full = os.path.abspath(os.path.join(WORKSPACE, rel))
    if full == WORKSPACE or full.startswith(WORKSPACE + os.sep):
        return full
    return WORKSPACE


def pipe_and_wait(proc):
    def reader(pipe, msg_type):
        try:
            for line in iter(pipe.readline, ""):
                broadcast(msg_type, line.rstrip("\r\n"))
        finally:
            pipe.close()

    t1 = threading.Thread(target=reader, args=(proc.stdout, "info"), daemon=True)
    t2 = threading.Thread(target=reader, args=(proc.stderr, "err"), daemon=True)
    t1.start()
    t2.start()
    code = proc.wait()
    t1.join(timeout=1)
    t2.join(timeout=1)
    return code


def needs_pip_install(req_path):
    if not os.path.isfile(req_path):
        return False
    with open(req_path, "r", encoding="utf-8", errors="replace") as f:
        content = f.read()
    lines = [ln.strip() for ln in content.splitlines() if ln.strip() and not ln.strip().startswith("#")]
    if not lines:
        return False

    current_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
    saved_hash = ""
    if os.path.isfile(REQ_HASH_FILE):
        with open(REQ_HASH_FILE, "r", encoding="utf-8") as f:
            saved_hash = f.read().strip()

    if current_hash != saved_hash:
        return True

    for ln in lines:
        if ln.startswith("-") or "://" in ln or "@" in ln:
            continue
        pkg = re.split(r"[<>=!~;\[\s#]", ln, 1)[0].strip()
        if not pkg:
            continue
        try:
            importlib.metadata.distribution(pkg)
        except importlib.metadata.PackageNotFoundError:
            return True

    return False


def save_req_hash(req_path):
    with open(req_path, "r", encoding="utf-8", errors="replace") as f:
        content = f.read()
    current_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
    with open(REQ_HASH_FILE, "w", encoding="utf-8") as f:
        f.write(current_hash)


def run_server_sequence():
    global server_proc, is_starting
    try:
        req_path = os.path.join(WORKSPACE, "requirements.txt")

        if needs_pip_install(req_path):
            broadcast("sys", "Installing requirements.txt...")
            pip_proc = subprocess.Popen(
                [sys.executable, "-m", "pip", "install", "-r", req_path],
                cwd=WORKSPACE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                errors="replace",
                start_new_session=True,
            )
            with proc_lock:
                server_proc = pip_proc

            code = pipe_and_wait(pip_proc)

            with proc_lock:
                if server_proc is pip_proc:
                    server_proc = None

            if stop_requested:
                broadcast("warn", "Start cancelled.")
                return

            if code != 0:
                broadcast("err", f"requirements.txt install failed (exit code: {code})")
                return

            save_req_hash(req_path)
            broadcast("ok", "requirements.txt installed.")

        with proc_lock:
            if stop_requested:
                return
            broadcast("sys", f"Starting: {start_cmd}")
            proc = subprocess.Popen(
                ["bash", "-c", start_cmd],
                cwd=WORKSPACE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                errors="replace",
                start_new_session=True,
            )
            server_proc = proc
            is_starting = False
            broadcast("ok", f"Server started (PID: {proc.pid})")

        code = pipe_and_wait(proc)

        with proc_lock:
            if server_proc is proc:
                server_proc = None
        broadcast("warn", f"Server stopped (exit code: {code})")
    except Exception as e:
        broadcast("err", f"Start error: {e}")
    finally:
        with proc_lock:
            is_starting = False


def start_server_proc():
    global stop_requested, is_starting
    with proc_lock:
        if is_starting or (server_proc is not None and server_proc.poll() is None):
            broadcast("warn", "Server is already running.")
            return
        stop_requested = False
        is_starting = True
    threading.Thread(target=run_server_sequence, daemon=True).start()


def stop_server_proc():
    global server_proc, stop_requested
    with proc_lock:
        stop_requested = True
        proc = server_proc
        starting = is_starting

    if not starting and (proc is None or proc.poll() is not None):
        broadcast("warn", "Server is not running.")
        return

    broadcast("warn", "Stopping server...")
    if proc is not None and proc.poll() is None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except Exception:
            try:
                proc.terminate()
            except Exception:
                pass

        for _ in range(20):
            if proc.poll() is not None:
                break
            time.sleep(0.1)

        if proc.poll() is None:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except Exception:
                pass

    for _ in range(20):
        with proc_lock:
            if not is_starting and (server_proc is None or server_proc.poll() is not None):
                break
        time.sleep(0.1)

    with proc_lock:
        if server_proc is proc:
            server_proc = None


class PanelHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_cors_headers()
        self.end_headers()

    def read_json(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b"{}"
        return json.loads(raw.decode("utf-8"))

    def send_json(self, code, obj):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        pathname = parsed.path
        qs = parse_qs(parsed.query)

        try:
            if pathname == "/":
                index_path = os.path.join(BASE_DIR, "index.html")
                if os.path.exists(index_path):
                    with open(index_path, "rb") as f:
                        data = f.read()
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(data)))
                    self.send_cors_headers()
                    self.end_headers()
                    self.wfile.write(data)
                else:
                    self.send_json(200, {"status": "online", "message": "Python Hosting Panel API is running!"})
                return

            if pathname == "/api/logs":
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "keep-alive")
                self.send_cors_headers()
                self.end_headers()

                q = queue.Queue()
                with sse_lock:
                    sse_clients.add(q)

                init_msg = json.dumps({"type": "sys", "text": f'Connected. Startup cmd: "{start_cmd}"'})
                self.wfile.write(f"data: {init_msg}\n\n".encode("utf-8"))
                self.wfile.flush()

                try:
                    while True:
                        try:
                            msg = q.get(timeout=15)
                            self.wfile.write(f"data: {json.dumps(msg)}\n\n".encode("utf-8"))
                            self.wfile.flush()
                        except queue.Empty:
                            self.wfile.write(b": keepalive\n\n")
                            self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass
                finally:
                    with sse_lock:
                        sse_clients.discard(q)
                return

            if pathname == "/api/files":
                rel = qs.get("path", [""])[0]
                dir_path = safe_join(rel)
                items = []
                with os.scandir(dir_path) as it:
                    for entry in it:
                        is_dir = entry.is_dir()
                        size = 0
                        count = 0
                        try:
                            if is_dir:
                                count = len(os.listdir(entry.path))
                            else:
                                size = entry.stat().st_size
                        except Exception:
                            pass
                        items.append({
                            "name": entry.name,
                            "type": "folder" if is_dir else "file",
                            "size": size,
                            "count": count,
                        })
                return self.send_json(200, {"items": items})

            if pathname == "/api/file":
                rel = qs.get("path", [""])[0]
                with open(safe_join(rel), "r", encoding="utf-8", errors="replace") as f:
                    content = f.read()
                return self.send_json(200, {"content": content})

            self.send_json(404, {"error": "Not found"})
        except Exception as e:
            self.send_json(500, {"error": str(e)})

    def do_POST(self):
        global start_cmd
        pathname = urlparse(self.path).path

        try:
            data = self.read_json()

            if pathname == "/api/file":
                target = safe_join(data.get("path", ""))
                with open(target, "w", encoding="utf-8") as f:
                    f.write(data.get("content", ""))
                return self.send_json(200, {"ok": True})

            if pathname == "/api/folder":
                os.makedirs(safe_join(data.get("path", "")), exist_ok=True)
                return self.send_json(200, {"ok": True})

            if pathname == "/api/upload":
                target = safe_join(data.get("path", ""))
                raw = base64.b64decode(data.get("base64", ""))
                with open(target, "wb") as f:
                    f.write(raw)
                return self.send_json(200, {"ok": True})

            if pathname == "/api/rename":
                os.rename(safe_join(data.get("oldPath", "")), safe_join(data.get("newPath", "")))
                return self.send_json(200, {"ok": True})

            if pathname == "/api/server":
                action = data.get("action")
                if action == "start":
                    start_server_proc()
                elif action == "stop":
                    stop_server_proc()
                elif action == "restart":
                    stop_server_proc()
                    start_server_proc()
                elif action == "status":
                    running = server_proc is not None and server_proc.poll() is None
                    broadcast("ok" if running else "warn", f"Server Status: {'RUNNING' if running else 'STOPPED'} | Startup: {start_cmd}")
                return self.send_json(200, {"ok": True})

            if pathname == "/api/exec":
                cmd = (data.get("cmd") or "").strip()
                rel_path = data.get("relPath", "")

                self.send_response(200)
                self.send_header("Content-Type", "application/x-ndjson")
                self.send_cors_headers()
                self.end_headers()

                def send_line(obj):
                    self.wfile.write((json.dumps(obj) + "\n").encode("utf-8"))
                    self.wfile.flush()

                if cmd.startswith("setstart "):
                    start_cmd = cmd[9:].strip()
                    with open(START_FILE, "w", encoding="utf-8") as f:
                        f.write(start_cmd)
                    send_line({"type": "ok", "text": f"Startup command updated: {start_cmd}"})
                    return

                if cmd == "cd" or cmd.startswith("cd "):
                    arg = cmd[2:].strip()
                    cur_dir = safe_join(rel_path)
                    next_dir = WORKSPACE if (not arg or arg in ("~", "/")) else os.path.abspath(os.path.join(cur_dir, arg))
                    if (next_dir == WORKSPACE or next_dir.startswith(WORKSPACE + os.sep)) and os.path.isdir(next_dir):
                        new_rel = os.path.relpath(next_dir, WORKSPACE)
                        send_line({"type": "cwd", "relPath": "" if new_rel == "." else new_rel})
                    else:
                        send_line({"type": "err", "text": f"cd: {arg}: No such directory"})
                    return

                proc = subprocess.Popen(
                    ["bash", "-c", cmd],
                    cwd=safe_join(rel_path),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=1,
                    errors="replace",
                )
                write_lock = threading.Lock()

                def pipe_reader(pipe, msg_type):
                    try:
                        for line in iter(pipe.readline, ""):
                            with write_lock:
                                send_line({"type": msg_type, "text": line.rstrip("\r\n")})
                    finally:
                        pipe.close()

                t1 = threading.Thread(target=pipe_reader, args=(proc.stdout, "info"), daemon=True)
                t2 = threading.Thread(target=pipe_reader, args=(proc.stderr, "err"), daemon=True)
                t1.start()
                t2.start()
                code = proc.wait()
                t1.join(timeout=1)
                t2.join(timeout=1)
                send_line({"type": "done", "code": code})
                return

            self.send_json(404, {"error": "Not found"})
        except Exception as e:
            self.send_json(500, {"error": str(e)})

    def do_DELETE(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/item":
            try:
                rel = parse_qs(parsed.query).get("path", [""])[0]
                target = safe_join(rel)
                if target != WORKSPACE:
                    if os.path.isdir(target):
                        shutil.rmtree(target)
                    elif os.path.exists(target):
                        os.remove(target)
                return self.send_json(200, {"ok": True})
            except Exception as e:
                return self.send_json(500, {"error": str(e)})
        self.send_json(404, {"error": "Not found"})


class PanelServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


if __name__ == "__main__":
    print(f"Python Hosting Panel running on port {PORT}")
    print(f"Workspace: {WORKSPACE}")
    PanelServer(("0.0.0.0", PORT), PanelHandler).serve_forever()
