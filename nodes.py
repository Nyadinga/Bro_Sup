import threading
import time
import json
import socket
import uuid
import hashlib
import os
import sqlite3
import shutil
import random
import logging
import struct
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass
from enum import Enum
import queue # Added for replication worker simulation if used

# =====================================================================
# 🌐 GLOBAL UTILITIES (Required for ThreadedStorageNode)
# =====================================================================

HEADER_SIZE = 4
LOG_LEVEL = logging.INFO

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("ThreadedStorageNode")

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
    
# =====================================================================
# 🌐 TCP/IP PROTOCOL STACK SIMULATION CLASSES (Simplified for clarity)
# (Keep your full definitions from previous context here)
# =====================================================================

class ProtocolType(Enum):
    TCP = "TCP"
    IP = "IP"
    ETHERNET = "Ethernet"

@dataclass
class EthernetHeader:
    source_mac: str
    destination_mac: str
    ethertype: str = "0x0800" 
    frame_check_sequence: str = ""
    def __post_init__(self):
        data = f"{self.source_mac}{self.destination_mac}{self.ethertype}"
        self.frame_check_sequence = hashlib.md5(data.encode()).hexdigest()[:8]

@dataclass
class IPHeader:
    version: int = 4
    header_length: int = 20
    time_to_live: int = 64
    protocol: int = 6
    header_checksum: str = ""
    source_ip: str = ""
    destination_ip: str = ""
    def __post_init__(self):
        header_data = f"{self.version}{self.header_length}{self.source_ip}{self.destination_ip}"
        self.header_checksum = hashlib.md5(header_data.encode()).hexdigest()[:4]

@dataclass
class TCPHeader:
    source_port: int = 21
    destination_port: int = 21
    sequence_number: int = 0
    checksum: str = ""
    def __post_init__(self):
        tcp_data = f"{self.source_port}{self.destination_port}{self.sequence_number}"
        self.checksum = hashlib.md5(tcp_data.encode()).hexdigest()[:4]

@dataclass
class NetworkPacket:
    ethernet_header: EthernetHeader
    ip_header: IPHeader  
    tcp_header: TCPHeader
    chunk_id: int
    data_size: int
    data_checksum: str
    payload: bytes = b""
    
    def get_total_size(self) -> int:
        return 18 + 20 + 20 + self.data_size # Simplified size calc

class NetworkStack:
    def __init__(self, node_id: str, ip_address: str, mac_address: str):
        self.node_id = node_id
        self.ip_address = ip_address
        self.mac_address = mac_address
        self.sequence_counter = random.randint(1000, 9999)
        
    def encapsulate_data(self, chunk_id: int, data_size: int, data_checksum: str, 
                        dest_ip: str, dest_mac: str) -> NetworkPacket:
        # Simplified: just return a packet structure
        self.sequence_counter += data_size
        return NetworkPacket(
            ethernet_header=EthernetHeader(self.mac_address, dest_mac),
            ip_header=IPHeader(source_ip=self.ip_address, destination_ip=dest_ip),
            tcp_header=TCPHeader(sequence_number=self.sequence_counter),
            chunk_id=chunk_id, data_size=data_size, data_checksum=data_checksum
        )
    
    def decapsulate_packet(self, packet: NetworkPacket) -> bool:
        # Simplified: check destination addresses
        if packet.ethernet_header.destination_mac != self.mac_address:
            return False
        if packet.ip_header.destination_ip != self.ip_address:
            return False
        return True
    
    def simulate_network_transmission(self, packets: List[NetworkPacket]) -> List[NetworkPacket]:
        # Not used for Manager comms, but kept for simulation layer
        shuffled_packets = packets.copy()
        random.shuffle(shuffled_packets)
        return shuffled_packets
    
    def reassemble_packets(self, packets: List[NetworkPacket]) -> List[NetworkPacket]:
        # Not used for Manager comms
        return sorted(packets, key=lambda p: p.tcp_header.sequence_number)

# =====================================================================
# 💾 FILE MANAGER CLASS (Updated with save_replica_file)
# =====================================================================

class FileManager:
    def __init__(self, node_id: str, storage_path: str = None):
        self.node_id = node_id
        self.storage_path = Path(storage_path) if storage_path else Path(f"./storage_{node_id}")
        self.storage_path.mkdir(exist_ok=True, parents=True)
        
        self.local_files_path = self.storage_path / "local_files"
        self.replica_files_path = self.storage_path / "replicas"
        self.downloads_path = self.storage_path / "downloads"
        
        self.local_files_path.mkdir(exist_ok=True)
        self.replica_files_path.mkdir(exist_ok=True)
        self.downloads_path.mkdir(exist_ok=True)
        
        self.db_path = self.storage_path / "file_metadata.db"
        self._init_database()
        
        self.file_locks = {}
        self.lock_manager = threading.RLock()
        
    def _init_database(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS local_files (
                file_id TEXT PRIMARY KEY, filename TEXT NOT NULL, original_path TEXT NOT NULL,
                file_path TEXT NOT NULL, file_size INTEGER NOT NULL, checksum TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cloud_files (
                file_id TEXT PRIMARY KEY, filename TEXT NOT NULL, file_size INTEGER NOT NULL,
                checksum TEXT NOT NULL, upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                replica_nodes TEXT NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS replica_files (
                file_id TEXT PRIMARY KEY, filename TEXT NOT NULL, file_path TEXT NOT NULL,
                file_size INTEGER NOT NULL, checksum TEXT NOT NULL, source_node_id TEXT NOT NULL,
                replicated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS downloaded_files (
                file_id TEXT PRIMARY KEY, filename TEXT NOT NULL, file_path TEXT NOT NULL,
                file_size INTEGER NOT NULL, checksum TEXT NOT NULL, source_node_id TEXT NOT NULL,
                downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()

    def add_local_file(self, filename: str, source_file_path: str) -> dict:
        if not os.path.exists(source_file_path):
            raise FileNotFoundError(f"File not found: {source_file_path}")
            
        file_id = str(uuid.uuid4())
        file_size = os.path.getsize(source_file_path)
        checksum = sha256_file(source_file_path) # Use the utility function
        local_file_path = self.local_files_path / f"{file_id}_{filename}"
        shutil.copy2(source_file_path, local_file_path)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO local_files (file_id, filename, original_path, file_path, file_size, checksum)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (file_id, filename, source_file_path, str(local_file_path), file_size, checksum))
        conn.commit()
        conn.close()
        
        return {
            'file_id': file_id, 'filename': filename, 'file_size': file_size,
            'checksum': checksum, 'local_path': str(local_file_path)
        }
    
    def get_local_file_by_id(self, file_id: str) -> Optional[dict]:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM local_files WHERE file_id = ?', (file_id,))
        row = cursor.fetchone()
        conn.close()
        if row:
             return {
                'file_id': row[0], 'filename': row[1], 'original_path': row[2],
                'file_path': row[3], 'file_size': row[4], 'checksum': row[5],
                'created_at': row[6]
            }
        return None

    def get_storage_stats(self) -> dict:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        stats = {}
        for table, category in [('local_files', 'local'), ('replica_files', 'replicas'), 
                               ('downloaded_files', 'downloads')]:
            cursor.execute(f'SELECT COUNT(*), SUM(file_size) FROM {table}')
            count, size = cursor.fetchone()
            stats[category] = {
                'count': count or 0,
                'size_mb': (size or 0) / (1024 * 1024)
            }
        conn.close()
        return stats
        
    def save_replica_file(self, file_id: str, filename: str, file_path: str, file_size: int, 
                           checksum: str, source_node_id: str):
        """Records a successfully received replicated file."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO replica_files 
            (file_id, filename, file_path, file_size, checksum, source_node_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (file_id, filename, file_path, file_size, checksum, source_node_id))
        conn.commit()
        conn.close()
        
    # Placeholder for other required FileManager methods (get_local_files, register_cloud_file, etc.)
    # ... assuming they are defined as in previous context ...

# =====================================================================
# 💻 THREADED STORAGE NODE CLASS (INTEGRATED)
# =====================================================================

class ThreadedStorageNode:
    """Enhanced storage node with file upload/download capabilities"""
    
    def __init__(self, node_id: str, ip_address: str, mac_address: str,
                 storage_capacity_gb: int = 100, bandwidth_mbps: int = 1000,
                 network_host: str = 'localhost', network_port: int = 8888):
        
        self.node_id = node_id
        self.ip_address = ip_address
        self.mac_address = mac_address
        self.storage_capacity = storage_capacity_gb * 1024 * 1024 * 1024
        self.bandwidth = bandwidth_mbps
        
        self.network_host = network_host
        self.network_port = network_port
        
        # New network state and socket object
        self.network_socket: Optional[socket.socket] = None
        self.connected_to_manager = False 
        
        # NetworkStack remains for local simulation
        self.network_stack = NetworkStack(node_id, ip_address, mac_address)
        
        self.file_manager = FileManager(node_id, f"./storage/{node_id}")
        
        self.used_storage = 0
        self.files_uploaded = 0
        self.files_downloaded = 0
        
        self.running = False
        self.network_listener_thread = None
        
    # --- Network Connection & Listener (UPDATED) ---

    def connect_to_network(self) -> bool:
        """Connect this node to the network manager and register itself."""
        try:
            # 1. Establish TCP connection
            self.network_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.network_socket.connect((self.network_host, self.network_port))
            
            # 2. Register the Node
            reg_msg = {
                "type": "register_node",
                "node_id": self.node_id,
                "node_info": {
                    "ip": self.ip_address,
                    "mac": self.mac_address,
                    "capacity_gb": self.storage_capacity / (1024**3)
                }
            }
            send_json_with_header(self.network_socket, reg_msg)
            
            response = recv_json_with_header(self.network_socket, timeout=10.0)
            
            if response and response.get("status") == "success":
                self.connected_to_manager = True
                logger.info(f"Node registered successfully with Manager at {self.network_host}:{self.network_port}")
                
                # Start network listener thread for heartbeats and commands
                self.running = True
                self.network_listener_thread = threading.Thread(
                    target=self._network_message_listener, 
                    daemon=True
                )
                self.network_listener_thread.start()
                
                return True
            else:
                logger.error(f"Manager registration failed: {response}")
                self.network_socket.close()
                return False
                
        except Exception as e:
            logger.error(f"Network connection failed for {self.node_id}: {e}")
            return False

    def _network_message_listener(self):
        """Listens for commands and heartbeats from the Network Manager."""
        logger.info(f"Network message listener started for {self.node_id}")
        
        while self.running and self.connected_to_manager:
            try:
                # Use a short timeout so the thread can shut down gracefully
                msg = recv_json_with_header(self.network_socket, timeout=1.0) 
                
                if msg is None:
                    continue 
                if not msg:
                    logger.warning("Manager disconnected.")
                    break
                
                mtype = msg.get("type")

                if mtype == "heartbeat_request":
                    self._send_heartbeat_response()
                elif mtype == "replicate_file":
                    self._handle_replication_request(msg)
                # TODO: Add handlers for "download_request" etc.
                else:
                    logger.debug(f"Received unhandled message type: {mtype}")
                    
            except socket.timeout:
                continue
            except Exception:
                logger.exception("Network listener error, disconnecting.")
                break
                
        self.stop_node()

    def _send_heartbeat_response(self):
        """Sends a response back to the manager to prove node health."""
        try:
            send_json_with_header(self.network_socket, {
                "type": "heartbeat_response",
                "node_id": self.node_id,
                "used_storage_bytes": self.used_storage
            })
            logger.debug("Heartbeat response sent.")
        except Exception as e:
            logger.warning(f"Failed to send heartbeat: {e}")
            
    # --- File Transfer Methods (UPDATED) ---

    def _upload_file_to_cloud(self, file_id: str):
        """Initiates the two-phase file upload protocol to the Network Manager."""
        local_file_info = self.file_manager.get_local_file_by_id(file_id)
        if not local_file_info:
            logger.error(f"Local file {file_id[:8]}... not found for upload.")
            print(f" Error: Local file with ID {file_id[:8]}... not found."); return

        file_path = local_file_info['local_path']
        
        try:
            # 1. Send initial upload request with metadata
            upload_msg = {
                "type": "upload_file",
                "node_id": self.node_id,
                "file_id": file_id,
                "filename": local_file_info['filename'],
                "file_size": local_file_info['file_size'],
                "checksum": local_file_info['checksum']
            }
            send_json_with_header(self.network_socket, upload_msg)
            
            # 2. Wait for 'ready' signal from Manager
            logger.info(f"Awaiting 'ready' signal from Manager for {local_file_info['filename']}")
            
            self.network_socket.settimeout(30.0)
            response = recv_json_with_header(self.network_socket, timeout=30.0)
            
            if not response or response.get("status") != "ready":
                logger.error(f"Manager not ready for upload: {response}")
                print(f" ❌ Upload failed: Manager rejected upload or timed out.")
                return

            # 3. Stream the file bytes
            logger.info("Manager ready. Starting file stream...")
            
            with open(file_path, "rb") as f:
                while True:
                    chunk = f.read(8192)
                    if not chunk:
                        break
                    # The raw bytes are sent directly over the TCP connection
                    self.network_socket.sendall(chunk)
            
            # 4. Wait for final 'success' response
            final_response = recv_json_with_header(self.network_socket, timeout=60.0)
            
            if final_response and final_response.get("status") == "success":
                self.files_uploaded += 1
                logger.info(f"Upload SUCCESS for {file_id[:8]}...")
                print(f" ✅ Upload to cloud initiated and verified by Manager.")
            else:
                logger.error(f"Upload verification failed by Manager: {final_response}")
                print(f" ❌ Upload failed: Manager reported an error (checksum or size mismatch).")
                
        except Exception:
            logger.exception("Upload process failed")
            print(f" ❌ A critical error occurred during upload.")
        finally:
            self.network_socket.settimeout(None)

    def _handle_replication_request(self, msg: dict):
        """
        Handles an incoming replication stream request from the Manager (or another node).
        This method will receive file bytes and store them as a replica.
        """
        file_id = msg.get("file_id")
        filename = msg.get("filename")
        file_size = msg.get("file_size")
        checksum = msg.get("checksum")
        source_node_id = msg.get("source_node_id")

        if not all([file_id, file_size, checksum]):
            logger.error("Received incomplete replication request metadata.")
            send_json_with_header(self.network_socket, {"status": "error", "message": "Missing metadata"})
            return

        replica_path = self.file_manager.replica_files_path / f"{file_id}_{filename}"
        logger.info(f"Starting replication receive for {filename} ({file_id[:8]}...) from {source_node_id}")

        try:
            # 1. Send 'ready' signal
            send_json_with_header(self.network_socket, {"status": "ready"})
            self.network_socket.settimeout(60.0) # Timeout for receiving bytes

            # 2. Receive and write file bytes
            received_bytes = self._receive_file_bytes(self.network_socket, str(replica_path), file_size)

            if received_bytes is None or received_bytes != file_size:
                logger.error("Replication failed: Incomplete byte stream.")
                if replica_path.exists(): os.remove(replica_path)
                send_json_with_header(self.network_socket, {"status": "error", "message": "Incomplete byte stream"})
                return

            # 3. Verify checksum
            calculated_checksum = sha256_file(str(replica_path))
            
            if calculated_checksum != checksum:
                logger.error(f"Replication failed: Checksum mismatch. Calculated: {calculated_checksum[:8]}")
                os.remove(replica_path)
                send_json_with_header(self.network_socket, {"status": "error", "message": "Checksum failed"})
                return
            
            # 4. Save metadata and acknowledge success
            self.file_manager.save_replica_file(
                file_id=file_id, 
                filename=filename, 
                file_path=str(replica_path), 
                file_size=file_size, 
                checksum=checksum, 
                source_node_id=source_node_id
            )
            
            logger.info(f"Replication SUCCESS. File stored as replica.")
            send_json_with_header(self.network_socket, {"status": "success", "file_id": file_id})

        except Exception:
            logger.exception("Replication stream failed")
            if replica_path.exists(): os.remove(replica_path)
            send_json_with_header(self.network_socket, {"status": "error", "message": "Internal error"})
        finally:
            self.network_socket.settimeout(None)

    def _receive_file_bytes(self, sock: socket.socket, file_path: str, expected_size: int) -> Optional[int]:
        """Utility method to receive a fixed amount of bytes from the socket and write to a file."""
        bytes_received = 0
        try:
            with open(file_path, "wb") as f:
                while bytes_received < expected_size:
                    to_read = min(8192, expected_size - bytes_received)
                    # Use the reliable global recv_exact utility
                    chunk = recv_exact(sock, to_read) 
                    if not chunk:
                        # Connection closed or timeout
                        return None 
                    
                    f.write(chunk)
                    bytes_received += len(chunk)
            
            if bytes_received == expected_size:
                return bytes_received
            else:
                return None # Incomplete transfer
        except Exception:
            logger.exception("Error receiving file bytes for %s", file_path)
            return None

    # --- Other Methods (Placeholder/Unchanged) ---
    
    def _add_local_file(self, filename: str, file_path: str):
        # Implementation remains the same, but uses updated FileManager methods
        try:
            result = self.file_manager.add_local_file(filename, file_path)
            # Update used_storage calculation
            print(f" File added to local storage: Name: {result['filename']}, ID: {result['file_id'][:8]}...")
        except FileNotFoundError:
            print(f" ERROR: Source file not found at path: {file_path}")
        except Exception as e:
            print(f" Failed to add file: {e}")
            
    # Assuming _show_local_files, _show_node_status, etc. are also present...
    
    def stop_node(self):
        """Cleanly shuts down the node."""
        print(f"\n Shutting down node {self.node_id}...")
        self.running = False
        
        # Send disconnect message to manager
        if self.connected_to_manager and self.network_socket:
            try:
                send_json_with_header(self.network_socket, {"type": "disconnect_node", "node_id": self.node_id})
            except:
                pass

        if self.network_socket:
            try: 
                self.network_socket.close()
            except: 
                pass
            self.connected_to_manager = False
            
        if self.network_listener_thread and self.network_listener_thread.is_alive():
            self.network_listener_thread.join(timeout=3)
            
        print(" Node stopped.")
        
    def start_node(self):
        """Start the node and begin accepting commands (simplified)"""
        if not self.connect_to_network():
            print(" Cannot start node without network connection")
            return
            
        print(f"\n{'='*70}")
        print(f"  STORAGE NODE: {self.node_id} - READY")
        print(f"{'='*70}")
        print("  Commands: addfile <name> <path>, upload <file_id>, status, quit")
        self._command_loop()

    def _command_loop(self):
        # Placeholder for command loop logic
        print("Running command loop. Use 'quit' to exit.")
        while self.running:
            try:
                cmd_input = input(f"\n{self.node_id}> ").strip()
                if not cmd_input: continue
                    
                parts = cmd_input.split()
                cmd = parts[0].lower()
                
                if cmd == 'quit': break
                elif cmd == 'status': print("Status placeholder")
                elif cmd == 'addfile' and len(parts) >= 3:
                    self._add_local_file(parts[1], ' '.join(parts[2:]))
                elif cmd == 'upload' and len(parts) >= 2:
                    self._upload_file_to_cloud(parts[1])
                else: print(" Unknown command or invalid syntax")
                    
            except KeyboardInterrupt: break
            except Exception as e: print(f" Command error: {e}")
                
        self.stop_node()