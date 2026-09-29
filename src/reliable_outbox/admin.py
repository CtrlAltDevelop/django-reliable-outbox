from __future__ import annotations

from typing import TYPE_CHECKING

from django.contrib import admin, messages
from django.db.models import Q, QuerySet
from django.db.models.functions import Now
from django.http import HttpRequest

from .models import InboxMessage, OutboxMessage, Status
from .operations import requeue

# ModelAdmin is generic only in django-stubs; at runtime it can't be subscripted.
if TYPE_CHECKING:
    OutboxAdminBase = admin.ModelAdmin[OutboxMessage]
    InboxAdminBase = admin.ModelAdmin[InboxMessage]
else:
    OutboxAdminBase = InboxAdminBase = admin.ModelAdmin


class StateFilter(admin.SimpleListFilter):
    """The operational view of a row, including states that aren't a status value."""

    title = "state"
    parameter_name = "state"

    def lookups(self, request: HttpRequest, model_admin: OutboxAdminBase) -> list[tuple[str, str]]:
        return [
            ("ready", "Ready to run"),
            ("in_flight", "In flight (leased)"),
            ("scheduled", "Scheduled / backing off"),
            ("retrying", "Failed at least once"),
        ]

    def queryset(
        self, request: HttpRequest, queryset: QuerySet[OutboxMessage]
    ) -> QuerySet[OutboxMessage]:
        pending = queryset.filter(status=Status.PENDING)
        unleased = Q(locked_until__isnull=True) | Q(locked_until__lt=Now())
        match self.value():
            case "ready":
                return pending.filter(unleased, run_at__lte=Now())
            case "in_flight":
                return pending.filter(locked_until__gte=Now())
            case "scheduled":
                return pending.filter(unleased, run_at__gt=Now())
            case "retrying":
                return pending.exclude(last_error="")
        return queryset


@admin.register(OutboxMessage)
class OutboxMessageAdmin(OutboxAdminBase):
    list_display = (
        "id",
        "name",
        "kind",
        "key",
        "status",
        "attempts",
        "max_attempts",
        "run_at",
        "created_at",
        "short_error",
    )
    list_filter = ("status", StateFilter, "kind", "destination", "created_at")
    search_fields = ("=message_id", "name", "=key", "last_error")
    ordering = ("-id",)
    date_hierarchy = "created_at"
    actions = ("requeue_selected",)
    # Rows are written by publish() and the worker; editing them by hand would
    # sidestep the leases and fencing that keep delivery honest.
    readonly_fields = tuple(
        f.name for f in OutboxMessage._meta.get_fields() if f.concrete and f.name != "id"
    )
    # A full COUNT(*) over a large outbox makes the changelist crawl.
    show_full_result_count = False

    def has_add_permission(self, request: HttpRequest) -> bool:
        return False

    def has_change_permission(self, request: HttpRequest, obj: OutboxMessage | None = None) -> bool:
        # Viewing is allowed via view permission; this keeps the form read-only.
        return False

    @admin.display(description="last error")
    def short_error(self, obj: OutboxMessage) -> str:
        first_line = obj.last_error.splitlines()[0] if obj.last_error else ""
        return first_line[:80]

    @admin.action(description="Requeue selected dead messages", permissions=["requeue"])
    def requeue_selected(self, request: HttpRequest, queryset: QuerySet[OutboxMessage]) -> None:
        count = requeue(queryset)
        skipped = queryset.count() - count
        text = f"Requeued {count} dead message(s)."
        if skipped:
            text += f" Skipped {skipped} that were not dead."
        self.message_user(request, text, messages.SUCCESS if count else messages.WARNING)

    def has_requeue_permission(self, request: HttpRequest) -> bool:
        opts = self.opts
        return request.user.has_perm(f"{opts.app_label}.change_{opts.model_name}")


@admin.register(InboxMessage)
class InboxMessageAdmin(InboxAdminBase):
    list_display = ("consumer", "message_id", "received_at")
    list_filter = ("consumer",)
    search_fields = ("=message_id",)
    ordering = ("-received_at",)
    readonly_fields = ("consumer", "message_id", "received_at")

    def has_add_permission(self, request: HttpRequest) -> bool:
        return False

    def has_change_permission(self, request: HttpRequest, obj: InboxMessage | None = None) -> bool:
        return False
