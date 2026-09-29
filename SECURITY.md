# Security policy

## Supported versions

Only the latest minor release receives security fixes while the project is
below 1.0.

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub's
[security advisories](https://github.com/CtrlAltDevelop/django-reliable-outbox/security/advisories/new),
not in a public issue. Include the version, a description of the impact, and
steps to reproduce if you have them. You can expect an acknowledgement within
a week and a fix or a mitigation plan within 30 days for confirmed issues.

## Things worth knowing when you deploy this

- **Payloads are stored in plain text** in the outbox table and shown in the
  Django admin. Do not publish secrets or data you would not put in a log; put
  an identifier in the payload and look the sensitive data up in the handler.
- **Webhook secrets** live in settings. Load them from the environment or a
  secret store, and rotate them by adding the new secret on the receiver
  before switching the sender. Receivers must verify the signature over the
  raw request body and reject stale timestamps (`verify_signature` does both).
- **The admin requeue action** needs the change permission on outbox messages.
  Requeueing re-runs side effects; grant it accordingly.
- **Handlers run with the worker's database credentials** and inside its
  transaction. Treat them as trusted application code.
