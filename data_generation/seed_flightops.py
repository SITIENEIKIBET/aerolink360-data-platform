"""
Seeds the Flight Operations domain with airports, gates, schedules, and
flight instances that progress through a realistic operational lifecycle
(SCHEDULED -> BOARDING -> DEPARTED -> ARRIVED, with DELAYED/DIVERTED/
CANCELLED branches), producing both state-change UPDATEs and event-log
INSERTs for Debezium to capture in Phase 3.

Usage:
    python data_generation\\seed_flightops.py
"""
import logging
import os
import random
import sys
from datetime import datetime, timedelta, time

import psycopg2
from dotenv import load_dotenv
from faker import Faker

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()
fake = Faker()
Faker.seed(452)
random.seed(452)

# Per the master prompt: airports must be addable dynamically, not hardcoded
# into the schema. This list is just seed data, not a structural constraint.
AIRPORTS = [
    ("NBO", "Jomo Kenyatta International Airport", "Nairobi", "Kenya", "Africa/Nairobi"),
    ("MBA", "Moi International Airport", "Mombasa", "Kenya", "Africa/Nairobi"),
    ("KIS", "Kisumu International Airport", "Kisumu", "Kenya", "Africa/Nairobi"),
    ("EDL", "Eldoret International Airport", "Eldoret", "Kenya", "Africa/Nairobi"),
    ("ADD", "Bole International Airport", "Addis Ababa", "Ethiopia", "Africa/Addis_Ababa"),
    ("JNB", "O.R. Tambo International Airport", "Johannesburg", "South Africa", "Africa/Johannesburg"),
    ("LOS", "Murtala Muhammed International Airport", "Lagos", "Nigeria", "Africa/Lagos"),
    ("ACC", "Kotoka International Airport", "Accra", "Ghana", "Africa/Accra"),
    ("KGL", "Kigali International Airport", "Kigali", "Rwanda", "Africa/Kigali"),
    ("DAR", "Julius Nyerere International Airport", "Dar es Salaam", "Tanzania", "Africa/Dar_es_Salaam"),
    ("DXB", "Dubai International Airport", "Dubai", "UAE", "Asia/Dubai"),
    ("LHR", "Heathrow Airport", "London", "United Kingdom", "Europe/London"),
    ("CDG", "Charles de Gaulle Airport", "Paris", "France", "Europe/Paris"),
    ("AMS", "Schiphol Airport", "Amsterdam", "Netherlands", "Europe/Amsterdam"),
    ("BOM", "Chhatrapati Shivaji Airport", "Mumbai", "India", "Asia/Kolkata"),
]

DELAY_REASONS = ["Air traffic congestion", "Late aircraft arrival", "Weather", "Crew scheduling", "Technical inspection"]
CANCELLATION_REASONS = ["Aircraft unavailable", "Weather", "Low booking demand", "Crew unavailability"]
DIVERSION_REASONS = ["Weather at destination", "Medical emergency onboard", "Technical issue"]


def get_connection():
    return psycopg2.connect(host="localhost", port=5434, dbname="flightops", user="aaa_admin", password=os.getenv("POSTGRES_PASSWORD"))


def seed_airports(cur):
    logger.info("Seeding %d airports...", len(AIRPORTS))
    for code, name, city, country, tz in AIRPORTS:
        cur.execute(
            "INSERT INTO airports (airport_code, airport_name, city, country, timezone) VALUES (%s,%s,%s,%s,%s);",
            (code, name, city, country, tz),
        )
    logger.info("Airports seeded.")


def seed_gates(cur, n_per_airport=6):
    logger.info("Seeding gates...")
    gate_ids = []
    for code, *_ in AIRPORTS:
        for i in range(1, n_per_airport + 1):
            terminal = random.choice(["T1", "T2", "A", "B"])
            cur.execute(
                "INSERT INTO gates (airport_code, gate_number, terminal) VALUES (%s,%s,%s) RETURNING gate_id;",
                (code, f"{terminal}-{i}", terminal),
            )
            gate_ids.append((cur.fetchone()[0], code))
    logger.info("Gates seeded.")
    return gate_ids


def seed_schedules(cur, n=40):
    logger.info("Seeding %d recurring schedules...", n)
    schedule_ids = []
    airport_codes = [a[0] for a in AIRPORTS]
    for i in range(n):
        origin, dest = random.sample(airport_codes, 2)
        dep_hour = random.randint(5, 22)
        dep_time = time(dep_hour, random.choice([0, 15, 30, 45]))
        flight_duration_hrs = random.randint(1, 9)
        arr_time = time((dep_hour + flight_duration_hrs) % 24, dep_time.minute)

        cur.execute(
            """INSERT INTO schedules (flight_number, origin_airport, destination_airport, days_of_week,
                                       scheduled_departure_time, scheduled_arrival_time, effective_from)
               VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING schedule_id;""",
            (f"AAA{100+i}", origin, dest, "MON,TUE,WED,THU,FRI,SAT,SUN", dep_time, arr_time, datetime.now().date() - timedelta(days=90)),
        )
        schedule_ids.append((cur.fetchone()[0], origin, dest, dep_time, arr_time))
    logger.info("Schedules seeded.")
    return schedule_ids


def seed_flights(cur, schedules, gate_lookup, days_back=14, days_forward=3):
    """
    Generates one flight instance per schedule per day across a rolling
    window. All flights start as SCHEDULED — the lifecycle pass moves
    them forward realistically.
    """
    logger.info("Generating flight instances...")
    flight_ids = []
    day_range = list(range(-days_back, days_forward + 1))

    for schedule_id, origin, dest, dep_time, arr_time in schedules:
        for day_offset in day_range:
            flight_date = datetime.now().date() + timedelta(days=day_offset)
            flight_id = f"AAA{100 + schedule_id}{flight_date.strftime('%m%d')}"[:20]

            scheduled_departure = datetime.combine(flight_date, dep_time)
            scheduled_arrival = datetime.combine(flight_date, arr_time)
            if arr_time < dep_time:
                scheduled_arrival += timedelta(days=1)

            gate_id = random.choice([g[0] for g in gate_lookup if g[1] == origin]) if any(g[1] == origin for g in gate_lookup) else None
            aircraft_reg = f"5Y-{random.choice(['AAA','BBB','CCC','DDD','EEE'])}{random.randint(10,99)}"

            cur.execute(
                """INSERT INTO flights (flight_id, schedule_id, flight_date, origin_airport, destination_airport,
                                         scheduled_departure, scheduled_arrival, flight_status, gate_id, aircraft_registration)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,'SCHEDULED',%s,%s);""",
                (flight_id, schedule_id, flight_date, origin, dest, scheduled_departure, scheduled_arrival, gate_id, aircraft_reg),
            )
            flight_ids.append((flight_id, day_offset, scheduled_departure, scheduled_arrival))

    logger.info("Generated %d flight instances.", len(flight_ids))
    return flight_ids


def log_event(cur, flight_id, event_type, details=""):
    cur.execute(
        "INSERT INTO flight_events (flight_id, event_type, event_details, event_timestamp) VALUES (%s,%s,%s, NOW());",
        (flight_id, event_type, details),
    )


def apply_lifecycle(cur, flight_ids):
    """
    Only past/current flights (day_offset <= 0) progress through their
    lifecycle - future flights correctly remain SCHEDULED, since they
    haven't happened yet. This is what makes the dataset believable.
    """
    logger.info("Applying flight lifecycle transitions...")
    past_or_today = [f for f in flight_ids if f[1] <= 0]

    n_delayed = n_cancelled = n_diverted = n_completed = 0

    for flight_id, day_offset, sched_dep, sched_arr in past_or_today:
        roll = random.random()

        if roll < 0.05:
            # Cancelled
            cur.execute("UPDATE flights SET flight_status='CANCELLED', updated_at=NOW() WHERE flight_id=%s;", (flight_id,))
            cur.execute("INSERT INTO cancellations (flight_id, cancellation_reason) VALUES (%s,%s);",
                        (flight_id, random.choice(CANCELLATION_REASONS)))
            log_event(cur, flight_id, "FLIGHT_CANCELLED")
            n_cancelled += 1
            continue

        # Normal progression
        log_event(cur, flight_id, "BOARDING_STARTED")
        cur.execute("UPDATE flights SET flight_status='BOARDING', updated_at=NOW() WHERE flight_id=%s;", (flight_id,))

        delay_minutes = 0
        if roll < 0.20:
            delay_minutes = random.choice([15, 30, 45, 60, 90, 120])
            cur.execute("INSERT INTO delays (flight_id, delay_minutes, delay_reason) VALUES (%s,%s,%s);",
                        (flight_id, delay_minutes, random.choice(DELAY_REASONS)))
            log_event(cur, flight_id, "DELAY_ANNOUNCED", f"{delay_minutes} minutes")
            n_delayed += 1

        actual_departure = sched_dep + timedelta(minutes=delay_minutes)
        cur.execute(
            "UPDATE flights SET flight_status='DEPARTED', actual_departure=%s, updated_at=NOW() WHERE flight_id=%s;",
            (actual_departure, flight_id),
        )
        log_event(cur, flight_id, "DEPARTED")

        if roll < 0.03:
            # Diverted
            cur.execute("SELECT destination_airport FROM flights WHERE flight_id=%s;", (flight_id,))
            original_dest = cur.fetchone()[0]
            diverted_to = random.choice([a[0] for a in AIRPORTS if a[0] != original_dest])
            cur.execute(
                "INSERT INTO diversions (flight_id, original_destination, diverted_to, diversion_reason) VALUES (%s,%s,%s,%s);",
                (flight_id, original_dest, diverted_to, random.choice(DIVERSION_REASONS)),
            )
            cur.execute("UPDATE flights SET flight_status='DIVERTED', updated_at=NOW() WHERE flight_id=%s;", (flight_id,))
            log_event(cur, flight_id, "DIVERTED", f"to {diverted_to}")
            n_diverted += 1
            continue

        actual_arrival = sched_arr + timedelta(minutes=delay_minutes)
        cur.execute(
            "UPDATE flights SET flight_status='ARRIVED', actual_arrival=%s, updated_at=NOW() WHERE flight_id=%s;",
            (actual_arrival, flight_id),
        )
        log_event(cur, flight_id, "ARRIVED")
        n_completed += 1

    logger.info(
        "Lifecycle applied: %d completed, %d delayed, %d diverted, %d cancelled (out of %d past/current flights).",
        n_completed, n_delayed, n_diverted, n_cancelled, len(past_or_today),
    )


def main():
    conn = get_connection()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            seed_airports(cur)
            gate_lookup = seed_gates(cur)
            schedules = seed_schedules(cur)
            flight_ids = seed_flights(cur, schedules, gate_lookup)
        conn.commit()
        logger.info("Initial flight seed committed.")

        with conn.cursor() as cur:
            apply_lifecycle(cur, flight_ids)
        conn.commit()
        logger.info("Lifecycle transitions committed successfully.")

    except Exception as e:
        conn.rollback()
        logger.error("Seeding failed, rolled back: %s", e)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()