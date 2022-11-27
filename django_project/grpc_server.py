import grpc
from concurrent import futures
import time
import apps.post_processing.grpc_services.grpc_service_pb2_grpc as pb2_grpc
import apps.post_processing.grpc_services.grpc_service_pb2 as pb2
import environ


class GrpcServer(pb2_grpc.GrpcServiceServicer):

    def __init__(self, *args, **kwargs):
        pass

    def FetchDataSets(self, request, context):

        # get the string from the incoming request
        #message = request.body
        print(request)
        #result = f'Hello I am up and running received "{message}" message from you'
        result = {'status': 200, 'body': "dcjndsjn"}

        return pb2.Response(**result)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_GrpcServiceServicer_to_server(GrpcServer(), server)
    env = environ.Env()
    environ.Env.read_env()
    url = "{}:{}".format(env('PYTHON_SERVICE_IP'), env('PYTHON_GRPC_PORT'))
    print(url)
    server.add_insecure_port(url)
    server.start()
    server.wait_for_termination()