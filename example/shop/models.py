from django.db import models


class Order(models.Model):
    email = models.EmailField()
    total_cents = models.PositiveIntegerField()
    receipt_sent = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self) -> str:
        return f"Order #{self.pk}"
