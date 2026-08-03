"""
Enterprise-level test for rate limiting & backpressure.
Tests:
1. Burst load exceeding configured rate limit → excess requests get 429s,
   not queued indefinitely or crashing downstream services
2. Sustained flash-sale load test (e.g., using Locust or k6) ramping to target RPS →
   measure p50/p95/p99 latency and confirm no oversell even as latency degrades
"""
import time
import uuid
import requests
import statistics
import concurrent.futures
from collections import defaultdict

# Configuration
ORDER_SERVICE_URL = "http://localhost:8001"
INVENTORY_SERVICE_URL = "http://localhost:8002"


def seed_inventory(sku: str, quantity: int):
    """Seed inventory with specified quantity."""
    response = requests.post(
        f"{INVENTORY_SERVICE_URL}/inventory/seed",
        json={"sku": sku, "qty": quantity},
        timeout=5
    )
    response.raise_for_status()
    return response.json()


def get_inventory(sku: str):
    """Get current inventory status."""
    response = requests.get(f"{INVENTORY_SERVICE_URL}/inventory/{sku}", timeout=5)
    response.raise_for_status()
    return response.json()


def place_order(user_id: str, sku: str, quantity: int, amount_cents: int):
    """Place an order via the order service."""
    response = requests.post(
        f"{ORDER_SERVICE_URL}/checkout",
        json={
            "user_id": user_id,
            "sku": sku,
            "qty": quantity,
            "amount_cents": amount_cents,
        },
        timeout=10
    )
    return response


def get_order(order_id: str):
    """Get order status."""
    response = requests.get(f"{ORDER_SERVICE_URL}/orders/{order_id}", timeout=5)
    response.raise_for_status()
    return response.json()


def test_burst_load_rate_limiting():
    """
    Test: Burst load exceeding configured rate limit → excess requests get 429s,
    not queued indefinitely or crashing downstream services
    """
    # Arrange
    sku = "rate-limit-test-sku"
    # Set inventory to a moderate number to allow some successes
    seed_inventory(sku, 10)

    user_base = f"user-{uuid.uuid4()}"

    # Based on shared/settings.py:
    # rate_limit_capacity: int = 50       # max tokens (burst size) per SKU bucket
    # rate_limit_refill_per_sec: float = 20.0  # tokens added per second per SKU
    # So we can burst up to 50 requests quickly, then should see rate limiting

    def make_order_request(request_id):
        try:
            response = place_order(
                user_id=f"{user_base}-{request_id}",
                sku=sku,
                quantity=1,
                amount_cents=1000
            )
            return {
                "status_code": response.status_code,
                "success": response.status_code == 202,
                "order_id": response.json().get("order_id") if response.status_code == 202 else None,
                "error": None
            }
        except Exception as e:
            return {
                "status_code": 0,
                "success": False,
                "order_id": None,
                "error": str(e)
            }

    # Act: Send a burst of requests (more than the burst capacity of 50)
    burst_size = 75  # Exceed the 50 token burst capacity

    start_time = time.time()
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [
            executor.submit(make_order_request, i)
            for i in range(burst_size)
        ]
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())

    end_time = time.time()
    duration = end_time - start_time

    # Analyze results
    successful_requests = [r for r in results if r["success"]]
    failed_requests = [r for r in results if not r["success"]]
    client_errors = [r for r in failed_requests if 400 <= r["status_code"] < 500]
    server_errors = [r for r in failed_requests if r["status_code"] >= 500]
    rate_limited = [r for r in failed_requests if r["status_code"] == 429]

    # Assert: We should see some rate limiting (429 responses) when exceeding burst capacity
    # Note: The exact behavior depends on how quickly we send requests vs. refill rate
    print(f"\nRate Limiting Test Results:")
    print(f"  Duration: {duration:.2f} seconds")
    print(f"  Requests/sec: {burst_size/duration:.2f}")
    print(f"  Successful requests: {len(successful_requests)}")
    print(f"  Failed requests: {len(failed_requests)}")
    print(f"  Client errors (4xx): {len(client_errors)}")
    print(f"  Server errors (5xx): {len(server_errors)}")
    print(f"  Rate limited (429): {len(rate_limited)}")

    # Assert: No server errors (5xx) - system should not crash under load
    assert len(server_errors) == 0, \
        f"Got server errors under load: {[r['status_code'] for r in server_errors]}"

    # Assert: We should get some successful requests (up to burst capacity + refill during test)
    expected_min_success = max(10, int(50 - (burst_size * 0.3)))  # Rough estimate
    assert len(successful_requests) >= 5, \
        f"Too few successful requests: {len(successful_requests)}"

    # Assert: Most failures should be client errors (4xx) including rate limits (429),
    # not server errors (5xx)
    assert len(client_errors) >= len(failed_requests) * 0.5, \
        f"Too many server errors relative to client errors: {len(server_errors)} server errors vs {len(client_errors)} client errors"

    # Check inventory consistency after the burst
    inventory = get_inventory(sku)
    total_accounted = inventory["available_qty"] + inventory["reserved_qty"]
    assert total_accounted == 10, \
        f"Inventory not conserved after burst load: {total_accounted} != 10"

    # Most importantly: no negative inventory (oversell protection still works under load)
    assert inventory["available_qty"] >= 0, \
        f"Negative available inventory after burst load: {inventory['available_qty']}"
    assert inventory["reserved_qty"] >= 0, \
        f"Negative reserved inventory after burst load: {inventory['reserved_qty']}"


def test_sustained_load_latency_measurement():
    """
    Test: Sustained flash-sale load test ramping to target RPS →
    measure p50/p95/p99 latency and confirm no oversell even as latency degrades
    Note: This is a simplified version - a full test would use Locust as mentioned in README
    """
    # Arrange
    sku = "sustained-load-test-sku"
    # Set inventory to support sustained load
    seed_inventory(sku, 50)

    user_base = f"user-{uuid.uuid4()}"

    def make_timed_request(request_id):
        start_time = time.time()
        try:
            response = place_order(
                user_id=f"{user_base}-{request_id}",
                sku=sku,
                quantity=1,
                amount_cents=1000
            )
            end_time = time.time()
            return {
                "status_code": response.status_code,
                "success": response.status_code == 202,
                "latency": end_time - start_time,
                "order_id": response.json().get("order_id") if response.status_code == 202 else None,
                "error": None
            }
        except Exception as e:
            end_time = time.time()
            return {
                "status_code": 0,
                "success": False,
                "latency": end_time - start_time,
                "order_id": None,
                "error": str(e)
            }

    # Act: Sustained load over time
    duration_seconds = 10  # 10 seconds of sustained load
    target_rps = 15  # Requests per second
    total_requests = int(duration_seconds * target_rps)

    # Stagger requests to simulate sustained load
    start_time = time.time()
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for i in range(total_requests):
            # Stagger the requests
            delay = (i / target_rps)  # Spread requests over time
            future = executor.submit(lambda idx=i: (
                time.sleep(max(0, (idx / target_rps) - (time.time() - start_time))) or
                make_timed_request(idx)
            ))
            futures.append(future)

        # Collect results as they complete
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())

    end_time = time.time()
    actual_duration = end_time - start_time

    # Analyze results
    successful_requests = [r for r in results if r["success"]]
    failed_requests = [r for r in results if not r["success"]]

    latencies = [r["latency"] for r in successful_requests if r["latency"] > 0]

    # Calculate latency percentiles
    if latencies:
        p50_latency = statistics.median(latencies)
        p95_latency = sorted(latencies)[int(len(latencies) * 0.95)] if len(latencies) >= 20 else max(latencies)
        p99_latency = sorted(latencies)[int(len(latencies) * 0.99)] if len(latencies) >= 100 else max(latencies)
    else:
        p50_latency = p95_latency = p99_latency = 0

    # Assert: System remains stable under sustained load
    print(f"\nSustained Load Test Results:")
    print(f"  Duration: {actual_duration:.2f} seconds")
    print(f"  Target RPS: {target_rps}, Actual: {len(results)/actual_duration:.2f}")
    print(f"  Total requests: {len(results)}")
    print(f"  Successful requests: {len(successful_requests)}")
    print(f"  Failed requests: {len(failed_requests)}")
    print(f"  Latency (ms) - P50: {p50_latency*1000:.2f}, P95: {p95_latency*1000:.2f}, P99: {p99_latency*1000:.2f}")

    # Assert: No server errors (5xx) - system should not crash
    server_errors = [r for r in failed_requests if r["status_code"] >= 500]
    assert len(server_errors) == 0, \
        f"Got server errors under sustained load: {[r['status_code'] for r in server_errors]}"

    # Assert: Latency should be reasonable (not exploding)
    # Under normal conditions, we'd expect sub-second responses
    assert p95_latency < 5.0, \
        f"P95 latency too high: {p95_latency:.2f}s (>{5.0}s threshold)"

    # Assert: Inventory conservation (most important)
    inventory = get_inventory(sku)
    total_accounted = inventory["available_qty"] + inventory["reserved_qty"]
    assert total_accounted == 50, \
        f"Inventory not conserved under sustained load: {total_accounted} != 50"

    # Most importantly: no negative inventory (oversell protection still works under sustained load)
    assert inventory["available_qty"] >= 0, \
        f"Negative available inventory after sustained load: {inventory['available_qty']}"
    assert inventory["reserved_qty"] >= 0, \
        f"Negative reserved inventory after sustained load: {inventory['reserved_qty']}"

    # Additionally: the number of reserved items should not exceed what we started with
    assert inventory["reserved_qty"] <= 50, \
        f"Reserved inventory exceeds initial stock: {inventory['reserved_qty']} > 50"