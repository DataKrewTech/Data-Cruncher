from django.apps import AppConfig
class MyAppConfig(AppConfig):
  name = 'django_project'
  verbose_name = "Post Processing Service"
  def ready(self):
      # if 'runserver' not in sys.argv:
      #   return True
      # from .models import MyModel
      from apps.post_processing.scheduler import offloading_scheduler
      #from articles.models import Article
      offloading_scheduler.start()

      # from post_processing import scheduler
      #     scheduler.start()