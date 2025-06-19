import json
from kafka import KafkaConsumer
from collections import defaultdict
from dataclasses import dataclass

# --- Venue class ---
@dataclass
class Venue:
    ask: float
    ask_size: int
    fee: float = 0.0
    rebate: float = 0.0

# --- Allocator functions ---
def compute_cost(split, venues, order_size, λo, λu, θ):
    executed = 0
    cash_spent = 0.0

    for i in range(len(venues)):
        exe = min(split[i], venues[i].ask_size)
        executed += exe
        cash_spent += exe * (venues[i].ask + venues[i].fee)
        maker_rebate = max(split[i] - exe, 0) * venues[i].rebate
        cash_spent -= maker_rebate

    underfill = max(order_size - executed, 0)
    overfill = max(executed - order_size, 0)
    risk_pen = θ * (underfill + overfill)
    cost_pen = λu * underfill + λo * overfill

    return cash_spent + risk_pen + cost_pen

def allocate(order_size, venues, λo, λu, θ):
    step = 100
    splits = [[]]

    for v in range(len(venues)):
        new_splits = []
        for alloc in splits:
            used = sum(alloc)
            max_v = min(order_size - used, venues[v].ask_size)
            for q in range(0, max_v + 1, step):
                new_splits.append(alloc + [q])
        splits = new_splits

    best_cost = float('inf')
    best_split = []

    for alloc in splits:
        if sum(alloc) != order_size:
            continue
        cost = compute_cost(alloc, venues, order_size, λo, λu, θ)
        if cost < best_cost:
            best_cost = cost
            best_split = alloc

    return best_split, best_cost

# --- Kafka consumer setup ---
consumer = KafkaConsumer(
    'mock_l1_stream',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

snapshot_buffer = defaultdict(list)
print("Consuming snapshots...")

for message in consumer:
    snapshot = message.value
    timestamp = snapshot["timestamp"]
    snapshot_buffer[timestamp].append(snapshot)

    if timestamp >= "13:45:14.000":
        break

print(f"\nTotal timestamps collected: {len(snapshot_buffer)}")

# --- Execution parameters ---
λo = 0.3
λu = 0.3
θ  = 0.1
order_size = 5000
remaining = order_size
total_cash = 0.0

# --- Order execution loop ---
for timestamp in sorted(snapshot_buffer.keys()):
    if remaining <= 0:
        break

    snapshots = snapshot_buffer[timestamp]
    venues = []

    for s in snapshots:
        venues.append(Venue(
            ask=s["ask_px"],
            ask_size=s["ask_sz"],
            fee=0.0,
            rebate=0.0
        ))

    split, _ = allocate(remaining, venues, λo, λu, θ)
    filled = sum(min(split[i], venues[i].ask_size) for i in range(len(venues)))
    cash = sum(min(split[i], venues[i].ask_size) * venues[i].ask for i in range(len(venues)))

    remaining -= filled
    total_cash += cash

    print(f"{timestamp} | Filled {filled} shares | Remaining: {remaining}")

# --- Summary ---
avg_fill_px = total_cash / (order_size - remaining) if (order_size - remaining) > 0 else 0.0

final_result = {
    "best_parameters": {
        "lambda_over": λo,
        "lambda_under": λu,
        "theta_queue": θ
    },
    "optimized": {
        "total_cash": round(total_cash, 2),
        "avg_fill_px": round(avg_fill_px, 4)
    }
}

print("\n===== Execution Summary =====")
print(json.dumps(final_result, indent=2))
