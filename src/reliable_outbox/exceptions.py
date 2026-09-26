class PermanentError(Exception):
    """Raise from a handler or job when retrying cannot help.

    The message goes straight to the dead-letter state with this error as its
    reason, instead of burning through its remaining attempts. Use it for input
    that will never be valid (a 404 for a deleted customer, a schema mismatch),
    not for outages, which are exactly what retries are for.
    """
