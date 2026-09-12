"""
Seeds the Customer & Loyalty domain: tiers, loyalty accounts (linked to
real Reservations customer_ids), an append-only miles transaction ledger,
and customer preferences. Includes tier-upgrade logic driven by
accumulated miles - the same threshold-triggered pattern used in the
Maintenance domain, applied to a different business concept.

Usage:
    python data_generation\\seed_loyalty.py
"""
import logging
import os
import random
import sys
from datetime import datetime, timedelta

import psycopg2
from dotenv import load_dotenv
from faker import Faker

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()
fake = Faker()
Faker.seed(500)
random.seed(500)

TIERS = [
    ("Bronze", 0, "Priority check-in"),
    ("Silver", 25000, "Priority check-in, lounge access (regional)"),
    ("Gold", 75000, "Lounge access, extra baggage, priority boarding"),
    ("Platinum", 150000, "All Gold benefits, guaranteed upgrades, dedicated support"),
]

SEAT_PREFS = ["WINDOW", "AISLE", "MIDDLE", None]
MEAL_PREFS = ["STANDARD", "VEGETARIAN", "HALAL", "KOSHER", "DIABETIC", None]
LANGUAGES = ["English", "Swahili", "French", "Arabic"]


def get_connection():
    return psycopg2.connect(host="localhost", port=5436, dbname="loyalty", user="aaa_admin", password=os.getenv("POSTGRES_PASSWORD"))


def load_reservation_customer_ids():
    """
    Queries the Reservations database directly for customer IDs, rather
    than going through an intermediate file (which introduces encoding
    fragility on Windows). This also better reflects how a real
    integration would look up cross-system reference data.
    """
    conn = psycopg2.connect(
        host="localhost", port=5433, dbname="reservations",
        user="aaa_admin", password=os.getenv("POSTGRES_PASSWORD"),
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT customer_id FROM customers ORDER BY customer_id;")
            ids = [str(row[0]) for row in cur.fetchall()]
    finally:
        conn.close()
    logger.info("Loaded %d customer IDs directly from Reservations domain.", len(ids))
    return ids


def seed_tiers(cur):
    logger.info("Seeding loyalty tiers...")
    tier_ids = []
    for name, min_miles, benefits in TIERS:
        cur.execute(
            "INSERT INTO loyalty_tiers (tier_name, min_miles, benefits) VALUES (%s,%s,%s) RETURNING tier_id;",
            (name, min_miles, benefits),
        )
        tier_ids.append((cur.fetchone()[0], name, min_miles))
    logger.info("Loyalty tiers seeded.")
    return tier_ids


def tier_for_miles(tier_ids, miles):
    """Returns the correct tier_id for a given miles balance, threshold-based."""
    eligible = [t for t in tier_ids if miles >= t[2]]
    return max(eligible, key=lambda t: t[2])  # highest threshold met


def seed_loyalty_accounts_and_activity(cur, customer_ids, tier_ids, enroll_fraction=0.7):
    """
    Not every Reservations customer is a loyalty member - 70% enrollment
    rate is realistic. For enrolled customers, generates an earn/redeem
    transaction history and derives their current tier from accumulated miles.
    """
    enrolled_customers = random.sample(customer_ids, k=int(len(customer_ids) * enroll_fraction))
    logger.info("Enrolling %d of %d customers into the loyalty program...", len(enrolled_customers), len(customer_ids))

    account_ids = []
    tier_counts = {name: 0 for _, name, _ in tier_ids}

    for customer_id in enrolled_customers:
        enrollment_date = fake.date_between(start_date="-3y", end_date="-30d")
        member_number = "AAA" + "".join(random.choices("0123456789", k=8))

        bronze_tier_id = min(tier_ids, key=lambda t: t[2])[0]

        cur.execute(
            """INSERT INTO loyalty_accounts (customer_id, member_number, current_tier_id, enrollment_date)
               VALUES (%s,%s,%s,%s) RETURNING loyalty_account_id;""",
            (customer_id, member_number, bronze_tier_id, enrollment_date),
        )
        account_id = cur.fetchone()[0]

        n_transactions = random.randint(3, 25)
        miles_earned_total = 0
        miles_redeemed_total = 0

        for _ in range(n_transactions):
            txn_date = fake.date_time_between(start_date=enrollment_date, end_date="now")
            if random.random() < 0.75:
                miles = random.randint(500, 8000)
                cur.execute(
                    """INSERT INTO loyalty_transactions (loyalty_account_id, transaction_type, miles_amount,
                                                            reference_type, transaction_date)
                       VALUES (%s,'EARN',%s,'BOOKING',%s);""",
                    (account_id, miles, txn_date),
                )
                miles_earned_total += miles
            else:
                available = miles_earned_total - miles_redeemed_total
                if available > 500:
                    miles = random.randint(500, min(available, 20000))
                    cur.execute(
                        """INSERT INTO loyalty_transactions (loyalty_account_id, transaction_type, miles_amount,
                                                                reference_type, transaction_date)
                           VALUES (%s,'REDEEM',%s,'BOOKING',%s);""",
                        (account_id, miles, txn_date),
                    )
                    miles_redeemed_total += miles

        balance = miles_earned_total - miles_redeemed_total
        correct_tier_id, tier_name, _ = tier_for_miles(tier_ids, miles_earned_total)
        tier_counts[tier_name] += 1

        cur.execute(
            """UPDATE loyalty_accounts
               SET total_miles_earned=%s, total_miles_redeemed=%s, miles_balance=%s,
                   current_tier_id=%s, updated_at=NOW()
               WHERE loyalty_account_id=%s;""",
            (miles_earned_total, miles_redeemed_total, balance, correct_tier_id, account_id),
        )

        account_ids.append(account_id)

    logger.info("Loyalty accounts and transaction history generated. Tier distribution: %s", tier_counts)
    return account_ids


def seed_preferences(cur, customer_ids, fraction=0.6):
    logger.info("Seeding customer preferences...")
    with_prefs = random.sample(customer_ids, k=int(len(customer_ids) * fraction))
    for customer_id in with_prefs:
        cur.execute(
            """INSERT INTO customer_preferences (customer_id, seat_preference, meal_preference,
                                                    preferred_language, marketing_opt_in)
               VALUES (%s,%s,%s,%s,%s);""",
            (customer_id, random.choice(SEAT_PREFS), random.choice(MEAL_PREFS),
             random.choice(LANGUAGES), random.random() > 0.3),
        )
    logger.info("Preferences seeded for %d customers.", len(with_prefs))


def main():
    customer_ids = load_reservation_customer_ids()

    conn = get_connection()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            tier_ids = seed_tiers(cur)
        conn.commit()

        with conn.cursor() as cur:
            seed_loyalty_accounts_and_activity(cur, customer_ids, tier_ids)
        conn.commit()
        logger.info("Loyalty accounts and transactions committed.")

        with conn.cursor() as cur:
            seed_preferences(cur, customer_ids)
        conn.commit()
        logger.info("Preferences committed successfully.")

    except Exception as e:
        conn.rollback()
        logger.error("Seeding failed, rolled back: %s", e)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()