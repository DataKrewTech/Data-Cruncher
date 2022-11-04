import grpc
from concurrent import futures
import time
import apps.post_processing.grpc_services.main_pb2_grpc as pb2_grpc
import apps.post_processing.grpc_services.main_pb2 as pb2


class GrpcServer(pb2_grpc.MainServicer):

    def __init__(self, *args, **kwargs):
        pass

    def GetServerResponse(self, request, context):

        # get the string from the incoming request
        message = request.message
        result = f'Hello I am up and running received "{message}" message from you'
        result = {'message': result, 'received': True}

        return pb2.MessageResponse(**result)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_MainServicer_to_server(GrpcServer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()