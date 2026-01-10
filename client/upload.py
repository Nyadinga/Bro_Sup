import grpc, hashlib, os
from generated import bluetap_pb2 as pb
from generated import bluetap_pb2_grpc as rpc

def upload_file(gateway_addr, token, filepath, chunk_size=1024*1024, replication=2):
    filename = os.path.basename(filepath)
    filesize = os.path.getsize(filepath)

    # 1. Configuration for Large Files
    # We increase the max message size to 2GB to prevent "Message too large" errors
    options = [
        ('grpc.max_send_message_length', 2 * 1024 * 1024 * 1024),
        ('grpc.max_receive_message_length', 2 * 1024 * 1024 * 1024),
    ]

    # STEP 1 → Ask gateway for metadata
    channel = grpc.insecure_channel(gateway_addr, options=options)
    stub = rpc.GatewayStub(channel)

    meta = stub.PutMeta(pb.PutMetaRequest(
        token=token,
        filename=filename,
        filesize=filesize,
        chunk_size=chunk_size,
        replication=replication,
    ))

    upload_id = meta.upload_id
    nodes = meta.nodes

    if not nodes:
        print("ERROR: Gateway returned no nodes.")
        return

    # STEP 2 → Parallel Upload (RAID-1 Style)
    # To properly mirror the data, we must send it to ALL nodes assigned by the Gateway
    for node in nodes:
        node_addr = f"{node.ip}:{node.port}"
        print(f"🚀 Replicating to {node.node_id} at {node_addr}...")

        node_channel = grpc.insecure_channel(node_addr, options=options)
        node_stub = rpc.NodeServiceStub(node_channel)

        def chunk_stream():
            with open(filepath, "rb") as f:
                chunk_id = 0
                while True:
                    data = f.read(chunk_size)
                    if not data:
                        break

                    checksum = hashlib.sha256(data).hexdigest()
                    
                    # We yield the chunk. Because it's a generator, 
                    # we only keep 1 chunk in RAM at a time.
                    yield pb.ChunkUpload(
                        upload_id=upload_id,
                        filename=filename,
                        chunk_id=chunk_id,
                        data=data,
                        checksum=checksum,
                    )
                    chunk_id += 1

        try:
            resp = node_stub.PutChunks(chunk_stream())
            print(f"✅ {node.node_id} Result: {resp.message}")
        except grpc.RpcError as e:
            print(f"❌ {node.node_id} Failed: {e.details()}")

    print("\n--- Distributed Upload Complete ---")