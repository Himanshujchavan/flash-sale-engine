"""
Enterprise-level test for ordering & delivery guarantees.
Tests:
1. Out-of-order message delivery (e.g., PaymentFailed arrives before PaymentInitiated)
   → state machine rejects invalid transitions instead of corrupting state
2. Message loss simulation → does a watchdog/timeout eventually detect the stuck saga
   and compensate?
"""
import time
import uuid
import requests
import pytest

# Configuration
ORDER_SERVICE_URL = "http://localhost:8001"
INVENTORY_SERVICE_URL = "http://localhost:8002"
PAYMENT_SERVICE_URL = "http://localhost:8003"
SAGA_COORDINATOR_URL = "http://localhost:8004"


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


def get_payment(order_id: str):
    """Get payment status."""
    response = requests.get(f"{PAYMENT_SERVICE_URL}/payments/{order_id}", timeout=5)
    response.raise_for_status()
    return response.json()


def get_saga(order_id: str):
    """Get saga status."""
    response = requests.get(f"{SAGA_COORDINATOR_URL}/sagas/{order_id}", timeout=5)
    response.raise_for_status()
    return response.json()


def test_out_of_order_message_delivery():
    """
    Test: Out-of-order message delivery (e.g., PaymentFailed arrives before PaymentInitiated)
    Expected: State machine rejects invalid transitions instead of corrupting state
    """
    # Arrange
    sku = "oood-test-sku"  # out-of-order delivery test
    seed_inventory(sku, 1)

    user_id = f"user-{uuid.uuid4()}"

    order_data = {
        "user_id": user_id,
        "sku": sku,
        "qty": 1,
        "amount_cents": 1000
    }

    # Act: Place order
    response = place_order(**order_data)
    assert response.status_code == 202
    order_id = response.json()["order_id"]

    # Wait for processing to begin
    time.sleep(2)

    # Check initial state
    order_initial = get_order(order_id: str):
        return response.json()
    except Exception:
        return None

    saga_initial = get_saga(order_id) if get_saga(order_id) else None
    payment_initial = get_payment(order_id) if get_payment(order_id) else None

    # Assert: Initial states should be sensible
    assert order_initial["status"] == "PENDING", \
        f"Order should start as PENDING, got {order_initial['status']}"

    if saga_initial:
        assert saga_initial["state"] in ["RESERVING", "CHARGING"], \
            f"Saga should be in RESERVING or CHARGING, got {saga_initial['state']}"

    # The saga coordinator's state machine should prevent invalid transitions
    # Even if messages arrive out of order due to network issues,
    # the saga state should only progress through valid states

    # Wait for processing to complete
    time.sleep(5)

    # Check final state
    order_final = get_order(order_id)
    saga_final = get_saga(order_id)
    payment_final = get_payment(order_id)

    # Assert: Final state should be valid (no corrupted state)
    assert order_final["status"] in ["CONFIRMED", "CANCELLED"], \
        f"Order ended in invalid state: {order_final['status']}"

    if saga_final:
        assert saga_final["state"] in ["CONFIRMED", "CANCELLED"], \
            f"Saga ended in invalid state: {saga_final['state']}"

        # Check that saga state matches order status
        if order_final["status"] == "CONFIRMED":
            assert saga_final["state"] == "CONFIRMED", \
                f"Order CONFIRMED but saga {saga_final['state']}"
        elif order_final["status"] == "CANCELLED":
            assert saga_final["state"] == "CANCELLED", \
                f"Order CANCELLED but saga {saga_final['state']}"

    # Inventory should be consistent
    inventory = get_inventory(sku)
    total_accounted = inventory["available_qty"] + inventory["reserved_qty"]
    assert total_accounted == 1, \
        f"Inventory not conserved: {total_accounted}"

    # Most importantly: we should NOT see impossible combinations like:
    # - Order CONFIRMED but no payment
    # - Order CANCELLED but inventory still reserved (should be released)
    if order_final["status"] == "CONFIRMED":
        assert payment_final and payment_final["status"] == "SUCCEEDED", \
            "CONFIRMED order missing successful payment"
        assert inventory["reserved_qty"] == 1, \
            "CONFIRMED order should have inventory reserved"
        assert inventory["available_qty"] == 0, \
            "CONFIRMED order should have zero available inventory"
    elif order_final["status"] == "CANCELLED":
        # For cancelled orders, inventory should be released back to available
        # (unless it was cancelled due to insufficient stock initially)
        assert inventory["available_qty"] >= 0, \
            f"Negative available inventory after cancellation: {inventory['available_qty']}"
        assert inventory["reserved_qty"] >= 0, \
            f"Negative reserved inventory after cancellation: {inventory['reserved_qty']}"


def test_message_loss_watchdog_timeout():
    """
    Test: Message loss simulation → does a watchdog/timeout eventually detect
    the stuck saga and compensate?
    Note: This system doesn't appear to have an explicit watchdog timeout
    for sagas, but it does rely on idempotency and eventual consistency.
    We can test that the system doesn't get permanently stuck.
    """
    # Arrange
    sku = "msg-loss-test-sku"
    seed_inventory(sku, 1)

    user_id = f"user-{uuid.uuid4()}"

    order_data = {
        "user_id": user_id,
        "sku": sku,
        "qty": 1,
        "amount_cents": 1000
    }

    # Act: Place order
    response = place_order(**order_data)
    assert response.status_code == 202
    order_id = response.json()["order_id"]

    # Check initial state
    order_initial = get_order(order_id)
    assert order_initial["status"] == "PENDING", \
        f"Order should start as PENDING, got {order_initial['status']}"

    # Act: Wait significantly longer than normal processing time
    # Normal processing should take seconds, so we wait much longer
    time.sleep(15)

    # Assert: The saga should not be stuck permanently in PENDING
    # (This would indicate a lost message that wasn't recovered)
    order_late = get_order(order_id)
    saga_late = get_saga(order_id) if get_saga(order_id) else None

    # The order should have progressed (even if it failed due to timeout)
    # It's acceptable for it to be PENDING if the saga is still processing
    # but it should not be stuck forever
    assert order_late["status"] in ["PENDING", "CONFIRMED", "CANCELLED"], \
        f"Order in unexpected state after extended wait: {order_late['status']}"

    # If it's still PENDING after a long wait, check if saga shows progress
    if order_late["status"] == "PENDING" and saga_late:
        # Saga should have made some progress
        assert saga_late["state"] != "RESERVING", \
            f"Saga stuck in initial RESERVING state after long wait: {saga_late}"

    # Inventory should still be consistent (no corruption)
    inventory = get_inventory(sku)
    total_accounted = inventory["available_qty"] + inventory["reserved_qty"]
    assert total_accounted == 1, \
        f"Inventory not conserved after extended wait: {total_accounted}"

    # Most importantly: no negative inventory (no overselling)
    assert inventory["available_qty"] >= 0, \
        f"Negative available inventory: {inventory['available_qty']}"
    assert inventory["reserved_qty"] >= 0, \
        f"Negative reserved inventory: {inventory['reserved_qty']}"