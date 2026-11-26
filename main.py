import os
import re
import sys
import stat
import json
import time
import queue
import base64
import shutil
import signal
import hashlib
import zipfile
import tarfile
import tempfile
import threading
import subprocess
import importlib.metadata
from datetime import datetime
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


def unique_dest_path(dest_dir, name):
    candidate = os.path.join(dest_dir, name)
    if not os.path.exists(candidate):
        return candidate
    base, ext = os.path.splitext(name)
    if os.path.isdir(candidate):
        base, ext = name, ""
    i = 1
    while True:
        suffix = " (copy)" if i == 1 else f" (copy {i})"
        new_name = f"{base}{suffix}{ext}"
        candidate = os.path.join(dest_dir, new_name)
        if not os.path.exists(candidate):
            return candidate
        i += 1


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


def create_zip_archive(src_paths, zip_path):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for src in src_paths:
            if not os.path.exists(src):
                continue
            base = os.path.dirname(src)
            if os.path.isfile(src):
                if src != zip_path:
                    zf.write(src, os.path.relpath(src, base))
            elif os.path.isdir(src):
                added = False
                for root, dirs, files in os.walk(src):
                    for fn in files:
                        fp = os.path.join(root, fn)
                        if fp == zip_path:
                            continue
                        zf.write(fp, os.path.relpath(fp, base))
                        added = True
                    if not dirs and not files:
                        zf.writestr(os.path.relpath(root, base) + "/", "")
                        added = True
                if not added:
                    zf.writestr(os.path.relpath(src, base) + "/", "")


class PanelHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Expose-Headers", "Content-Disposition")

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

            if pathname == "/api/info":
                rel = qs.get("path", [""])[0]
                target = safe_join(rel)
                if not os.path.exists(target):
                    return self.send_json(404, {"error": "Item not found"})
                st = os.stat(target)
                is_dir = os.path.isdir(target)
                total_size = st.st_size
                if is_dir:
                    total_size = 0
                    for root, _, files in os.walk(target):
                        for fn in files:
                            try:
                                total_size += os.path.getsize(os.path.join(root, fn))
                            except Exception:
                                pass
                return self.send_json(200, {
                    "name": os.path.basename(target),
                    "path": "/" + os.path.relpath(target, WORKSPACE).replace("\\", "/"),
                    "type": "Folder" if is_dir else "File",
                    "size": total_size,
                    "permissions": oct(stat.S_IMODE(st.st_mode))[2:].zfill(3),
                    "modified": datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                })

            if pathname == "/api/download":
                rel = qs.get("path", [""])[0]
                target = safe_join(rel)
                if not os.path.exists(target):
                    return self.send_json(404, {"error": "File not found"})

                if os.path.isfile(target):
                    filename = os.path.basename(target)
                    size = os.path.getsize(target)
                    self.send_response(200)
                    self.send_header("Content-Type", "application/octet-stream")
                    self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
                    self.send_header("Content-Length", str(size))
                    self.send_cors_headers()
                    self.end_headers()
                    with open(target, "rb") as f:
                        shutil.copyfileobj(f, self.wfile)
                    return

                # Folder download as zip
                tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
                tmp.close()
                try:
                    create_zip_archive([target], tmp.name)
                    filename = os.path.basename(target) + ".zip"
                    size = os.path.getsize(tmp.name)
                    self.send_response(200)
                    self.send_header("Content-Type", "application/zip")
                    self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
                    self.send_header("Content-Length", str(size))
                    self.send_cors_headers()
                    self.end_headers()
                    with open(tmp.name, "rb") as f:
                        shutil.copyfileobj(f, self.wfile)
                finally:
                    if os.path.exists(tmp.name):
                        os.remove(tmp.name)
                return

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

            if pathname == "/api/copy":
                sources = data.get("sources", [])
                dest_dir = safe_join(data.get("destDir", ""))
                if not os.path.isdir(dest_dir):
                    return self.send_json(400, {"error": "Destination is not a folder"})
                for rel in sources:
                    src = safe_join(rel)
                    if not os.path.exists(src) or src == WORKSPACE:
                        continue
                    if os.path.isdir(src) and (dest_dir == src or dest_dir.startswith(src + os.sep)):
                        return self.send_json(400, {"error": "Cannot copy folder into itself"})
                    dst = unique_dest_path(dest_dir, os.path.basename(src))
                    if os.path.isdir(src):
                        shutil.copytree(src, dst)
                    else:
                        shutil.copy2(src, dst)
                return self.send_json(200, {"ok": True})

            if pathname == "/api/move":
                sources = data.get("sources", [])
                dest_dir = safe_join(data.get("destDir", ""))
                if not os.path.isdir(dest_dir):
                    return self.send_json(400, {"error": "Destination is not a folder"})
                for rel in sources:
                    src = safe_join(rel)
                    if not os.path.exists(src) or src == WORKSPACE:
                        continue
                    if os.path.isdir(src) and (dest_dir == src or dest_dir.startswith(src + os.sep)):
                        return self.send_json(400, {"error": "Cannot move folder into itself"})
                    if os.path.dirname(src) == dest_dir:
                        continue
                    dst = unique_dest_path(dest_dir, os.path.basename(src))
                    shutil.move(src, dst)
                return self.send_json(200, {"ok": True})

            if pathname == "/api/zip":
                sources = data.get("sources", [])
                dest_dir = safe_join(data.get("destDir", ""))
                zip_name = os.path.basename((data.get("zipName") or "archive.zip").strip())
                if not zip_name.lower().endswith(".zip"):
                    zip_name += ".zip"
                zip_path = unique_dest_path(dest_dir, zip_name)
                src_paths = [safe_join(r) for r in sources]
                create_zip_archive(src_paths, zip_path)
                return self.send_json(200, {"ok": True, "name": os.path.basename(zip_path)})

            if pathname == "/api/unzip":
                archive_path = safe_join(data.get("path", ""))
                dest_dir = safe_join(data.get("destDir", ""))
                if not os.path.isfile(archive_path):
                    return self.send_json(404, {"error": "Archive not found"})
                lower = archive_path.lower()
                if lower.endswith(".zip"):
                    with zipfile.ZipFile(archive_path, "r") as zf:
                        for member in zf.infolist():
                            mp = os.path.abspath(os.path.join(dest_dir, member.filename))
                            if not (mp == dest_dir or mp.startswith(dest_dir + os.sep)):
                                return self.send_json(400, {"error": "Unsafe path in zip"})
                        zf.extractall(dest_dir)
                elif lower.endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tar.xz")):
                    with tarfile.open(archive_path, "r:*") as tf:
                        tf.extractall(dest_dir, filter="data")
                else:
                    return self.send_json(400, {"error": "Unsupported archive format"})
                return self.send_json(200, {"ok": True})

            if pathname == "/api/chmod":
                target = safe_join(data.get("path", ""))
                mode_str = str(data.get("mode", "")).strip()
                if not re.fullmatch(r"[0-7]{3,4}", mode_str):
                    return self.send_json(400, {"error": "Invalid mode (use e.g. 755 or 644)"})
                os.chmod(target, int(mode_str, 8))
                return self.send_json(200, {"ok": True})

            if pathname == "/api/delete-batch":
                paths = data.get("paths", [])
                for rel in paths:
                    target = safe_join(rel)
                    if target != WORKSPACE:
                        if os.path.isdir(target):
                            shutil.rmtree(target)
                        elif os.path.exists(target):
                            os.remove(target)
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
