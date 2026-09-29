# Example project

A shop with one model. Placing an order writes the order, an `order.created`
event and a `send_receipt` job in one transaction.

```bash
make pg-up                      # from the repository root
cd example
uv run python manage.py migrate
uv run python manage.py runserver &
uv run python manage.py outbox_worker            # in another terminal
curl -X POST localhost:8000/orders/ -d '{"email": "a@example.com", "total_cents": 1250}'
```

Or without a web server:

```bash
uv run python manage.py shell -c "from shop.services import place_order; place_order('a@example.com', 1250)"
uv run python manage.py outbox_worker --burst
```
