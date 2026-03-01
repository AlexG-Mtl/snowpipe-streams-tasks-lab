import json
import os
import random
import uuid
from datetime import datetime, timezone, timedelta

STATUSES = ["CREATED", "PAID", "SHIPPED", "DELIVERED"]

def utc_now():
    return datetime.now(timezone.utc)

def load_state(state_path: str) -> dict:
    if os.path.exists(state_path):
        with open(state_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"orders": {}, "batch_id": 0}

def save_state(state_path: str, state: dict) -> None:
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state, f)

def next_status(current: str) -> str:
    try:
        i = STATUSES.index(current)
        return STATUSES[min(i + 1, len(STATUSES) - 1)]
    except ValueError:
        return "CREATED"

def main():
    random.seed()  # non-deterministic per run

    out_dir = os.environ.get("OUT_DIR", "out")
    os.makedirs(out_dir, exist_ok=True)

    state_path = os.environ.get("STATE_PATH", os.path.join(out_dir, "state.json"))
    n_new = int(os.environ.get("N_NEW", "20"))
    n_updates = int(os.environ.get("N_UPDATES", "10"))
    dup_rate = float(os.environ.get("DUP_RATE", "0.10"))  # 10% duplicates
    late_rate = float(os.environ.get("LATE_RATE", "0.10"))  # 10% late events

    state = load_state(state_path)
    state["batch_id"] += 1
    batch_id = state["batch_id"]

    orders = state["orders"]  # dict keyed by order_id -> last known record
    now = utc_now()

    events = []

    # Create new orders
    for _ in range(n_new):
        order_id = str(uuid.uuid4())
        customer_id = random.randint(1, 5000)
        amount = round(random.uniform(10, 500), 2)

        rec = {
            "batch_id": batch_id,
            "event_type": "ORDER_CREATED",
            "order_id": order_id,
            "customer_id": customer_id,
            "status": "CREATED",
            "amount": amount,
            "event_ts": now.isoformat(),
            "updated_at": now.isoformat()
        }
        orders[order_id] = rec
        events.append(rec)

    # Update existing orders
    existing_ids = list(orders.keys())
    random.shuffle(existing_ids)
    for order_id in existing_ids[: min(n_updates, len(existing_ids))]:
        prev = orders[order_id]
        new_status = next_status(prev["status"])

        # sometimes simulate late events
        event_time = now
        if random.random() < late_rate:
            event_time = now - timedelta(minutes=random.randint(10, 120))

        rec = {
            "batch_id": batch_id,
            "event_type": "ORDER_STATUS_UPDATED",
            "order_id": order_id,
            "customer_id": prev["customer_id"],
            "status": new_status,
            "amount": prev["amount"],
            "event_ts": event_time.isoformat(),
            "updated_at": now.isoformat()
        }
        orders[order_id] = rec
        events.append(rec)

    # Inject duplicates
    if events:
        dup_count = int(len(events) * dup_rate)
        for _ in range(dup_count):
            events.append(random.choice(events))

    # Write JSON lines file
    ts = now.strftime("%Y-%m-%dT%H-%M-%SZ")
    filename = f"orders_events_{ts}_batch{batch_id}.jsonl"
    out_path = os.path.join(out_dir, filename)

    with open(out_path, "w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")

    save_state(state_path, state)

    print(f"Wrote {len(events)} records to {out_path}")

if __name__ == "__main__":
    main()
