"""
Seeds the Reservations domain database with realistic synthetic data,
including deliberate state transitions designed to be meaningful CDC
events once Debezium is watching this database (Phase 3).

Usage:
    python data_generation\seed_reservations.py
"""
import logging
import os
import random
import string
import sys
from datetime import datetime, timedelta

import psycopg2
from dotenv import load_dotenv
from faker import Faker

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()
fake = Faker()
Faker.seed(360)
random.seed(360)

FARE_CLASSES = [("Y", "Economy"), ("J", "Business"), ("F", "First")]
BOOKING_CHANNELS = ["WEB", "MOBILE", "AGENT", "CALL_CENTER"]
PAYMENT_METHODS = ["CARD", "MPESA", "BANK_TRANSFER"]
CURRENCIES = ["KES", "USD", "EUR", "GBP"]

# Realistic AAA flight_id format: AAA + 3 digits (cross-system reference,
# no FK — flightops domain owns the real flight records)
def random_flight_id():
    return f"AAA{random.randint(100, 999)}"


def get_connection(dbname="reservations"):
    return psycopg2.connect(
        host="localhost",
        port=5433,
        dbname=dbname,
        user="aaa_admin",
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def seed_fare_classes(cur):
    logger.info("Seeding fare classes...")
    ids = []
    for code, name in FARE_CLASSES:
        cur.execute(
            "INSERT INTO fare_classes (class_code, class_name) VALUES (%s, %s) RETURNING fare_class_id;",
            (code, name),
        )
        ids.append(cur.fetchone()[0])

    for fc_id, (code, _) in zip(ids, FARE_CLASSES):
        refundable = code == "F"
        fee = 0 if code == "F" else (5000 if code == "J" else 2000)
        baggage = 40 if code == "F" else (32 if code == "J" else 23)
        cur.execute(
            """INSERT INTO fare_rules (fare_class_id, refundable, change_fee, baggage_allowance_kg)
               VALUES (%s, %s, %s, %s);""",
            (fc_id, refundable, fee, baggage),
        )
    logger.info("Fare classes and rules seeded.")
    return ids


def seed_customers(cur, n=300):
    logger.info("Seeding %d customers...", n)
    ids = []
    for _ in range(n):
        cur.execute(
            """INSERT INTO customers (first_name, last_name, email, phone, nationality, date_of_birth)
               VALUES (%s, %s, %s, %s, %s, %s) RETURNING customer_id;""",
            (
                fake.first_name(), fake.last_name(), fake.email(), fake.phone_number(),
                fake.country(), fake.date_of_birth(minimum_age=18, maximum_age=80),
            ),
        )
        ids.append(cur.fetchone()[0])
    logger.info("Customers seeded.")
    return ids


def seed_passengers(cur, customer_ids, n=400):
    """Some passengers are the customer themselves; some are booked on behalf of others (family, etc)."""
    logger.info("Seeding %d passengers...", n)
    ids = []
    for _ in range(n):
        customer_id = random.choice(customer_ids)
        cur.execute(
            """INSERT INTO passengers (customer_id, first_name, last_name, passport_number, date_of_birth)
               VALUES (%s, %s, %s, %s, %s) RETURNING passenger_id;""",
            (
                customer_id, fake.first_name(), fake.last_name(),
                fake.bothify(text="P########"), fake.date_of_birth(minimum_age=1, maximum_age=90),
            ),
        )
        ids.append(cur.fetchone()[0])
    logger.info("Passengers seeded.")
    return ids


def seed_bookings_and_related(cur, customer_ids, passenger_ids, fare_class_ids, n_bookings=500):
    logger.info("Seeding %d bookings with items, tickets, and payments...", n_bookings)
    booking_ids = []

    for i in range(n_bookings):
        customer_id = random.choice(customer_ids)
        ref = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
        booking_date = fake.date_time_between(start_date="-60d", end_date="now")
        channel = random.choice(BOOKING_CHANNELS)
        currency = random.choice(CURRENCIES)

        # Everything starts PENDING — the state-transition pass (Step 2 of seeding)
        # will move a realistic subset forward to CONFIRMED/CANCELLED.
        cur.execute(
            """INSERT INTO bookings (customer_id, booking_reference, booking_status, booking_channel,
                                      booking_date, total_amount, currency)
               VALUES (%s, %s, 'PENDING', %s, %s, 0, %s) RETURNING booking_id;""",
            (customer_id, ref, channel, booking_date, currency),
        )
        booking_id = cur.fetchone()[0]
        booking_ids.append(booking_id)

        n_items = random.randint(1, 3)
        total = 0
        for _ in range(n_items):
            passenger_id = random.choice(passenger_ids)
            fare_class_id = random.choice(fare_class_ids)
            fare_amount = round(random.uniform(8000, 150000), 2)  # KES-scale fares
            total += fare_amount

            cur.execute(
                """INSERT INTO booking_items (booking_id, passenger_id, flight_id, fare_class_id,
                                               seat_number, fare_amount)
                   VALUES (%s, %s, %s, %s, %s, %s) RETURNING booking_item_id;""",
                (booking_id, passenger_id, random_flight_id(), fare_class_id,
                 f"{random.randint(1,40)}{random.choice('ABCDEF')}", fare_amount),
            )
            booking_item_id = cur.fetchone()[0]

            ticket_number = "AAA" + "".join(random.choices(string.digits, k=10))
            cur.execute(
                """INSERT INTO tickets (booking_item_id, ticket_number, ticket_status)
                   VALUES (%s, %s, 'ISSUED');""",
                (booking_item_id, ticket_number),
            )

        cur.execute("UPDATE bookings SET total_amount = %s WHERE booking_id = %s;", (round(total, 2), booking_id))

        # Payment starts PENDING alongside the booking
        cur.execute(
            """INSERT INTO payments (booking_id, payment_method, amount, currency, payment_status)
               VALUES (%s, %s, %s, %s, 'PENDING');""",
            (booking_id, random.choice(PAYMENT_METHODS), round(total, 2), currency),
        )

    logger.info("Bookings, items, tickets, and payments seeded.")
    return booking_ids


def apply_state_transitions(cur, booking_ids):
    """
    Second pass — the real point of this generator. Moves a realistic
    subset of bookings/payments/tickets through their natural lifecycle.
    Each UPDATE here is a genuine before/after CDC event once Debezium
    is watching this database.
    """
    logger.info("Applying realistic state transitions (this is what CDC will capture)...")

    # 70% of bookings get confirmed (payment completes)
    to_confirm = random.sample(booking_ids, k=int(len(booking_ids) * 0.7))
    for booking_id in to_confirm:
        cur.execute("UPDATE bookings SET booking_status = 'CONFIRMED', updated_at = NOW() WHERE booking_id = %s;", (booking_id,))
        cur.execute(
            "UPDATE payments SET payment_status = 'COMPLETED', paid_at = NOW(), updated_at = NOW() WHERE booking_id = %s;",
            (booking_id,),
        )

    # Of the confirmed ones, 8% later get cancelled (realistic churn)
    to_cancel = random.sample(to_confirm, k=int(len(to_confirm) * 0.08))
    for booking_id in to_cancel:
        cur.execute("UPDATE bookings SET booking_status = 'CANCELLED', updated_at = NOW() WHERE booking_id = %s;", (booking_id,))
        cur.execute(
            """INSERT INTO cancellations (booking_id, cancelled_by, cancellation_reason)
               VALUES (%s, %s, %s);""",
            (booking_id, random.choice(["CUSTOMER", "AIRLINE"]), random.choice(
                ["Change of plans", "Flight schedule change", "Found better fare elsewhere", "Medical emergency"]
            )),
        )
        cur.execute(
            "UPDATE tickets SET ticket_status = 'CANCELLED', updated_at = NOW() WHERE booking_item_id IN "
            "(SELECT booking_item_id FROM booking_items WHERE booking_id = %s);",
            (booking_id,),
        )

        # Some cancellations trigger a refund request
        if random.random() < 0.6:
            cur.execute("SELECT payment_id, amount FROM payments WHERE booking_id = %s;", (booking_id,))
            payment_id, amount = cur.fetchone()
            refund_status = random.choice(["REQUESTED", "APPROVED", "PROCESSED"])
            cur.execute(
                """INSERT INTO refunds (payment_id, refund_amount, refund_reason, refund_status, processed_at)
                   VALUES (%s, %s, %s, %s, %s);""",
                (payment_id, amount, "Booking cancelled", refund_status,
                 datetime.now() if refund_status == "PROCESSED" else None),
            )

    logger.info(
        "State transitions applied: %d confirmed, %d cancelled (subset of confirmed).",
        len(to_confirm), len(to_cancel),
    )


def main():
    conn = get_connection()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            fare_class_ids = seed_fare_classes(cur)
            customer_ids = seed_customers(cur)
            passenger_ids = seed_passengers(cur, customer_ids)
            booking_ids = seed_bookings_and_related(cur, customer_ids, passenger_ids, fare_class_ids)
        conn.commit()
        logger.info("Initial seed committed.")

        # Separate transaction for state transitions — this is deliberate:
        # in Phase 3, Debezium will only be watching from the point it starts,
        # so we can re-run apply_state_transitions() later as a standalone
        # script to generate fresh CDC events on demand.
        with conn.cursor() as cur:
            apply_state_transitions(cur, booking_ids)
        conn.commit()
        logger.info("State transitions committed successfully.")

    except Exception as e:
        conn.rollback()
        logger.error("Seeding failed, rolled back: %s", e)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()