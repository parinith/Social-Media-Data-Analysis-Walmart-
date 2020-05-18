from django.apps import AppConfig


class AppWalmartConfig(AppConfig):
    name = 'app_walmart'
    def ready(self):
        from app_walmart.walmart import walmart
        walmart()
