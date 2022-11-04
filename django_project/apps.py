from django.apps import AppConfig

class MyAppConfig(AppConfig):
  name = 'django_project'
  verbose_name = "Post Processing Service"

  def ready(self):
    from apps.post_processing.scheduler import offloading_scheduler
    from django_project import grpc_server

    offloading_scheduler.start()
    grpc_server.serve()

    # from post_processing import scheduler
    #     scheduler.start()