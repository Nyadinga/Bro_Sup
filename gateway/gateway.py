import os, time, uuid, sys, random
from concurrent import futures
import grpc

if __package__ is None:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from generated import bluetap_pb2 as pb
from generated import bluetap_pb2_grpc as rpc
from gateway.db import MetadataDB

class GatewayServicer(rpc.GatewayServicer):
    def __init__(self, db: MetadataDB):
        self.db = db

    def PutMeta(self, request, context):
        username = self.db.validate_token(request.token)
        if not username: context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid token")

        # Use the metadata field to carry the folder name
        target_folder = request.metadata if request.metadata else "General"

        all_nodes = self.db.list_nodes()
        live_nodes = [row for row in all_nodes if row[4] and (time.time() - row[4] < 30)]
        
        if len(live_nodes) < 1: context.abort(grpc.StatusCode.UNAVAILABLE, "No live nodes!")

        selected_nodes = random.sample(live_nodes, min(len(live_nodes), request.replication))
        upload_id = str(uuid.uuid4())
        total_chunks = (request.filesize + request.chunk_size - 1) // request.chunk_size
        
        # Save to DB with folder support
        self.db.save_file_metadata(upload_id, request.filename, username, request.filesize, 
                                   request.chunk_size, total_chunks, [n.node_id for n in selected_nodes],
                                   folder_name=target_folder)

        return pb.PutMetaResponse(upload_id=upload_id, nodes=selected_nodes, 
                                  total_chunks=total_chunks, chunk_size=request.chunk_size)

    def ListFiles(self, request, context):
        username = self.db.validate_token(request.token)
        rows = self.db.get_user_files(username)
        res = [pb.FileSummary(filename=r[0], upload_id=r[1], filesize=r[2], 
                              created_at=time.ctime(r[3]), folder_name=r[4]) for r in rows]
        return pb.ListFilesResponse(files=res, total=len(res))

def serve():
    db = MetadataDB()
    # Support 2GB Transfers
    options = [('grpc.max_send_message_length', 2*1024**3), ('grpc.max_receive_message_length', 2*1024**3)]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10), options=options)
    rpc.add_GatewayServicer_to_server(GatewayServicer(db), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("Gateway Online")
    try:
        while True: time.sleep(60)
    except KeyboardInterrupt: server.stop(0)

if __name__ == "__main__":
    serve()