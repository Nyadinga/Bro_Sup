
import socket
import threading
import struct
import json
import uuid
import queue
import os
import hashlib
import time
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

STORAGE_DIR = "storage"
METADATA_FILE = os.path.join(STORAGE_DIR, "cloud_files.json")
REPLICATION_FACTOR = 2 
REPLICATION_WORKERS = 2
HEADER_SIZE = 4
LOG_LEVEL = logging.INFO


logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("ThreadedNetworkMerged")

def ensure_storage():
    if not os.path.exists(STORAGE_DIR):
        os.makedirs(STORAGE_DIR)

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

def recv_exact(sock: socket.socket, n: int) -> Optional[bytes]:
    data = b""
    while len(data) < n:
        try:
            chunk = sock.recv(n - len(data))
        except socket.timeout:
            return None
        except Exception:
            return None
        if not chunk:
            return None
        data += chunk
    return data

def send_json_with_header(sock: socket.socket, obj: dict):
    data = json.dumps(obj).encode()
    sock.sendall(struct.pack(">I", len(data)) + data)

def recv_json_with_header(sock: socket.socket, timeout: float = None) -> Optional[dict]:
    if timeout is not None:
        sock.settimeout(timeout)
    header = recv_exact(sock, HEADER_SIZE)
    if header is None:
        return None
    length = struct.unpack(">I", header)[0]
    payload = recv_exact(sock, length)
    if payload is None:
        return None
    try:
        return json.loads(payload.decode())
    except Exception:
        return None

class DistributedFileService:
    """Stores files on disk and keeps metadata; performs replication tasks via manager"""
    def __init__(self, manager):
        self.manager = manager
        ensure_storage()
        self.lock = threading.RLock()
        self._load_metadata()
        # replication queue (file_id)
        self.repl_queue = queue.Queue()
        self.repl_workers = []
        for i in range(REPLICATION_WORKERS):
            t = threading.Thread(target=self._replication_worker, daemon=True, name=f"repl-worker-{i}")
            t.start()
            self.repl_workers.append(t)

    def _load_metadata(self):
        if os.path.exists(METADATA_FILE):
            try:
                with open(METADATA_FILE, "r") as f:
                    self.cloud_files = json.load(f)
            except Exception:
                logger.exception("Failed to load metadata - starting fresh")
                self.cloud_files = {}
        else:
            self.cloud_files = {}

    def _save_metadata(self):
        try:
            with open(METADATA_FILE, "w") as f:
                json.dump(self.cloud_files, f, default=str, indent=2)
        except Exception:
            logger.exception("Failed to save metadata")

    def upload_file(self, uploader_node_id: str, file_id: str, filename: str, file_path: str, checksum: str) -> dict:
        """Register uploaded file and enqueue replication."""
        with self.lock:
            size = os.path.getsize(file_path)
            existing = self.cloud_files.get(file_id)
            if existing is None:
                self.cloud_files[file_id] = {
                    "file_id": file_id,
                    "filename": filename,
                    "file_size": size,
                    "checksum": checksum,
                    "uploader_node_id": uploader_node_id,
                    "replica_nodes": [uploader_node_id], 
                    "upload_timestamp": datetime.utcnow().isoformat(),
                    "download_count": 0
                }
            else:
                existing.update({
                    "filename": filename,
                    "file_size": size,
                    "checksum": checksum
                })
            self._save_metadata()

            self.repl_queue.put(file_id)
            logger.info(f"Enqueued replication for {filename} ({file_id[:8]}...)")
            return {"success": True, "file_id": file_id}

    def list_files(self) -> List[dict]:
        with self.lock:
            return list(self.cloud_files.values())

    def get_file_metadata(self, file_id: str) -> Optional[dict]:
        with self.lock:
            return self.cloud_files.get(file_id)

    def increment_download_count(self, file_id: str):
        with self.lock:
            if file_id in self.cloud_files:
                self.cloud_files[file_id]["download_count"] = self.cloud_files[file_id].get("download_count", 0) + 1
                self._save_metadata()


    def _replication_worker(self):
        while True:
            try:
                file_id = self.repl_queue.get()
                if file_id is None:
                    break
                try:
                    self._perform_replication(file_id)
                except Exception:
                    logger.exception("Replication task error")
                finally:
                    self.repl_queue.task_done()
            except Exception:
                logger.exception("Replication worker loop error")
                time.sleep(1)

    def _perform_replication(self, file_id: str):

        meta = self.get_file_metadata(file_id)
        if not meta:
            logger.warning("Replication: metadata not found for %s", file_id)
            return
        filename = meta["filename"]
        file_path = os.path.join(STORAGE_DIR, file_id)  # stored by file_id
        if not os.path.exists(file_path):
            logger.warning("Replication: file bytes missing for %s", file_id)
            return

        with self.lock:
            current_replicas = list(meta.get("replica_nodes", []))

        needed = max(0, REPLICATION_FACTOR - len(current_replicas))
        if needed == 0:
            logger.debug("Replication: replication factor already met for %s", file_id)
            return

        logger.info("Replication: need %d more replicas for %s", needed, filename)

        # Choose candidate nodes (connected and not already holding the file)
        with self.manager.node_connection_lock:
            candidates = [
                nid for nid, info in self.manager.connected_nodes.items()
                if nid not in current_replicas and info.get("status") == "connected"
            ]

        if not candidates:
            logger.info("Replication: no candidate nodes available right now for %s", file_id)
            return

        chosen = candidates[:needed]
        for node_id in chosen:
            success = self._replicate_to_node(node_id, file_id, filename, file_path, meta["checksum"], meta["uploader_node_id"])
            if success:
                with self.lock:
                    self.cloud_files[file_id]["replica_nodes"].append(node_id)
                    self._save_metadata()

    def _replicate_to_node(self, node_id: str, file_id: str, filename: str, file_path: str, checksum: str, source_node_id: str) -> bool:
        """Attempt to contact node and send a replicate_file control message, then stream bytes."""
        logger.info("Replicating %s to %s", filename, node_id)
        with self.manager.node_connection_lock:
            node_sock = self.manager.node_connections.get(node_id)
            if node_sock is None:
                logger.warning("Replication: node socket not found for %s", node_id)
                return False
            try:
                # Send control msg
                ctl = {
                    "type": "replicate_file",
                    "file_id": file_id,
                    "filename": filename,
                    "file_size": os.path.getsize(file_path),
                    "checksum": checksum,
                    "source_node_id": source_node_id
                }
                send_json_with_header(node_sock, ctl)
                # Wait for 'ready' (with a short timeout)
                node_sock.settimeout(10.0)
                resp = recv_json_with_header(node_sock, timeout=10.0)
                if not resp or resp.get("status") != "ready":
                    logger.warning("Replication: node %s not ready: %s", node_id, resp)
                    return False

                # Stream the bytes
                with open(file_path, "rb") as f:
                    while True:
                        chunk = f.read(8192)
                        if not chunk:
                            break
                        node_sock.sendall(chunk)

                node_sock.settimeout(10.0)
                final = recv_json_with_header(node_sock, timeout=10.0)
                if final and final.get("status") == "success":
                    logger.info("Replication succeeded to %s", node_id)
                    return True
                else:
                    logger.warning("Replication final not success: %s", final)
                    return False
            except Exception:
                logger.exception("Replication stream to node %s failed", node_id)
                return False
            finally:
                try:
                    node_sock.settimeout(None)
                except Exception:
                    pass


class ThreadedNetworkManager:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.server_socket: Optional[socket.socket] = None
        self.running = False


        self.node_connection_lock = threading.RLock()
        self.node_connections: Dict[str, socket.socket] = {}
        self.connected_nodes: Dict[str, dict] = {}


        self.file_service = DistributedFileService(self)

        self.heartbeat_interval = 10.0
        self.heartbeat_timeout = 30.0

    def start_network_server(self):
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(16)
            self.running = True
            logger.info("Network manager started on %s:%d", self.host, self.port)


            threading.Thread(target=self._connection_health_monitor, daemon=True).start()

            while self.running:
                try:
                    client_sock, address = self.server_socket.accept()
                    logger.info("New connection from %s:%d", *address)
                    t = threading.Thread(target=self._handle_node_connection, args=(client_sock, address), daemon=True)
                    t.start()
                except Exception:
                    logger.exception("Accept loop error")
        except Exception:
            logger.exception("Failed to start network server")
        finally:
            self.stop_network()

    def stop_network(self):
        logger.info("Shutting down network manager...")
        self.running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
        with self.node_connection_lock:
            for sock in self.node_connections.values():
                try:
                    sock.close()
                except:
                    pass

    def _handle_node_connection(self, client_sock: socket.socket, address):
        node_id = None
        try:

            client_sock.settimeout(None)
            while self.running:
                msg = recv_json_with_header(client_sock, timeout=None)
                if not msg:
                    logger.info("Connection closed or invalid from %s:%d", *address)
                    break
                mtype = msg.get("type")

                if mtype == "register_node":
                    node_id = msg.get("node_id") or str(uuid.uuid4())
                    node_info = msg.get("node_info", {})
                    with self.node_connection_lock:
                        self.connected_nodes[node_id] = {
                            "node_info": node_info,
                            "connected_at": datetime.utcnow().isoformat(),
                            "last_heartbeat": time.time(),
                            "status": "connected"
                        }
                        self.node_connections[node_id] = client_sock
                    logger.info("Registered node %s (%s)", node_id, node_info.get("ip", address[0]))
                    send_json_with_header(client_sock, {"status":"success", "message":"registered"})
                elif mtype == "heartbeat_response":
                    nid = msg.get("node_id")
                    with self.node_connection_lock:
                        if nid in self.connected_nodes:
                            self.connected_nodes[nid]["last_heartbeat"] = time.time()


                    res = self._handle_file_upload(msg, client_sock)
                    if res and isinstance(res, dict) and res.get("send_response", True):
                        send_json_with_header(client_sock, res)
                elif mtype == "download_file":
                    self._handle_file_download(msg, client_sock)
                elif mtype == "replicate_file":
                    self._handle_replicate_receive(msg, client_sock)
                elif mtype == "list_cloud_files":
                    files = self.file_service.list_files()
                    send_json_with_header(client_sock, {"status":"success", "files": files})
                elif mtype == "get_cloud_stats":
                    files = self.file_service.list_files()
                    total_size = sum(f["file_size"] for f in files)
                    total_downloads = sum(f.get("download_count", 0) for f in files)
                    send_json_with_header(client_sock, {"status":"success", "stats":{
                        "total_files": len(files),
                        "total_size_mb": total_size/(1024*1024),
                        "total_downloads": total_downloads,
                        "replication_factor": REPLICATION_FACTOR,
                        "connected_nodes": len(self.connected_nodes)
                    }})
                elif mtype == "disconnect_node":
                    nid = msg.get("node_id")
                    self._cleanup_disconnected_node(nid)
                    send_json_with_header(client_sock, {"status":"success", "message":"disconnected"})
                else:
                    logger.warning("Unknown message type: %s", mtype)
                    send_json_with_header(client_sock, {"status":"error", "message":"unknown type"})
        except Exception:
            logger.exception("Node handler exception")
        finally:
            if node_id:
                self._cleanup_disconnected_node(node_id)
            try:
                client_sock.close()
            except:
                pass


    def _handle_file_upload(self, msg: dict, sock: socket.socket) -> dict:
        """
        Rules for file upload:
        1) Node sends JSON with type='upload_file' and metadata (file_size, file_id, filename, checksum, node_id)
        2) Server responds with {"status": "ready"} if ready to receive
        3) Node streams file bytes (file_size bytes)
        4) Server verifies checksum, saves file, and responds with {"status": "success"} or {"status": "error"}
        """
