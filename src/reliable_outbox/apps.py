from django.apps import AppConfig
from django.utils.module_loading import autodiscover_modules

from .conf import get_settings


class ReliableOutboxConfig(AppConfig):
    name = "reliable_outbox"
    verbose_name = "Reliable outbox"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self) -> None:
        from . import checks  # noqa: F401, PLC0415 - registers the system checks

        # Validate eagerly: a bad setting should stop `runserver`, not the first publish.
        config = get_settings()
        # Jobs and handlers register themselves on import. The worker process
        # never imports your views, so it has to go looking for them.
        autodiscover_modules(*config.AUTODISCOVER)
