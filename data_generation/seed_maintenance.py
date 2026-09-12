"""
Seeds the Aircraft & Maintenance domain: aircraft models, aircraft (with
accumulated flight hours/cycles), engines, maintenance schedules, and
maintenance events/defects. Aircraft registrations are generated using
the same format as Flight Operations so the two domains can later be
joined in the Lakehouse via aircraft_registration.

Usage:
    python data_generation\\seed_maintenance.py
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
Faker.seed(787)
random.seed(787)

AIRCRAFT_MODELS = [
    ("Boeing 787-8", "Boeing", 234),
    ("Boeing 737-800", "Boeing", 162),
    ("Embraer E190", "Embraer", 100),
    ("Airbus A330-300", "Airbus", 277),
]

MAINTENANCE_TYPES = [
    ("A_CHECK", 600, 400, 60),      # every 600 flight hrs / 400 cycles / 60 days
    ("B_CHECK", 2000, 1200, 180),
    ("C_CHECK", 6000, 4000, 730),
    ("D_CHECK", 24000, 16000, 2555),
]

DEFECT_DESCRIPTIONS = [
    "Hydraulic fluid leak detected", "Cabin pressurization anomaly",
    "Landing gear indicator fault", "Avionics software glitch",
    "Engine vibration above threshold", "APU start malfunction",
]


def get_connection():
    return psycopg2.connect(host="localhost", port=5435, dbname="maintenance", user="aaa_admin", password=os.getenv("POSTGRES_PASSWORD"))


def seed_models(cur):
    logger.info("Seeding aircraft models...")
    ids = []
    for name, mfr, seats in AIRCRAFT_MODELS:
        cur.execute(
            "INSERT INTO aircraft_models (model_name, manufacturer, seat_capacity) VALUES (%s,%s,%s) RETURNING model_id;",
            (name, mfr, seats),
        )
        ids.append(cur.fetchone()[0])
    logger.info("Aircraft models seeded.")
    return ids


def seed_maintenance_schedules(cur, model_ids):
    logger.info("Seeding maintenance schedules...")
    schedule_map = {}  # model_id -> list of (schedule_id, type, hrs_interval, cycles_interval, days_interval)
    for model_id in model_ids:
        schedule_map[model_id] = []
        for mtype, hrs, cycles, days in MAINTENANCE_TYPES:
            cur.execute(
                """INSERT INTO maintenance_schedules (model_id, maintenance_type, interval_flight_hours,
                                                        interval_flight_cycles, interval_calendar_days)
                   VALUES (%s,%s,%s,%s,%s) RETURNING schedule_id;""",
                (model_id, mtype, hrs, cycles, days),
            )
            schedule_map[model_id].append((cur.fetchone()[0], mtype, hrs, cycles, days))
    logger.info("Maintenance schedules seeded.")
    return schedule_map


# Matches the format used in seed_flightops.py, so both domains reference
# the same aircraft fleet by registration.
def registration_pool():
    return [f"5Y-{prefix}{num}" for prefix in ['AAA', 'BBB', 'CCC', 'DDD', 'EEE'] for num in range(10, 100)]


def seed_aircraft(cur, model_ids, n=25):
    logger.info("Seeding %d aircraft...", n)
    regs = random.sample(registration_pool(), n)
    aircraft = []
    for reg in regs:
        model_id = random.choice(model_ids)
        manufacture_date = fake.date_between(start_date="-15y", end_date="-1y")
        # Older aircraft naturally accumulate more hours/cycles
        age_years = (datetime.now().date() - manufacture_date).days / 365
        total_hours = round(age_years * random.uniform(800, 1500), 2)
        total_cycles = int(age_years * random.uniform(400, 700))

        cur.execute(
            """INSERT INTO aircraft (aircraft_registration, model_id, manufacture_date, total_flight_hours, total_flight_cycles)
               VALUES (%s,%s,%s,%s,%s);""",
            (reg, model_id, manufacture_date, total_hours, total_cycles),
        )
        aircraft.append((reg, model_id, total_hours, total_cycles))

        # 1-2 engines per aircraft
        for pos in (["LEFT", "RIGHT"] if random.random() > 0.15 else ["CENTER"]):
            cur.execute(
                """INSERT INTO engines (aircraft_registration, engine_serial, engine_position, total_engine_hours)
                   VALUES (%s,%s,%s,%s);""",
                (reg, fake.bothify(text="ENG-#####"), pos, round(total_hours * random.uniform(0.9, 1.0), 2)),
            )
    logger.info("Aircraft and engines seeded.")
    return aircraft


def seed_parts(cur, n=30):
    logger.info("Seeding %d parts...", n)
    part_ids = []
    for _ in range(n):
        cur.execute(
            """INSERT INTO parts (part_name, part_number, stock_quantity, unit_cost)
               VALUES (%s,%s,%s,%s) RETURNING part_id;""",
            (fake.catch_phrase(), fake.bothify(text="PN-#####-??"), random.randint(0, 50), round(random.uniform(50, 15000), 2)),
        )
        part_ids.append(cur.fetchone()[0])
    logger.info("Parts seeded.")
    return part_ids


def generate_maintenance_events(cur, aircraft, schedule_map, part_ids):
    """
    Threshold-triggered logic: a maintenance event becomes DUE when an
    aircraft's accumulated hours/cycles cross the schedule's interval.
    This is a genuinely different trigger mechanism than the random-chance
    branching used in Reservations/FlightOps - it's proportional to real
    accumulated usage.
    """
    logger.info("Generating maintenance events based on accumulated usage...")
    n_due = n_completed = n_unscheduled = 0

    for reg, model_id, total_hours, total_cycles in aircraft:
        for schedule_id, mtype, hrs_interval, cycles_interval, days_interval in schedule_map[model_id]:
            # How many times has this aircraft crossed this maintenance interval?
            times_due_by_hours = int(total_hours // hrs_interval) if hrs_interval else 0
            times_due_by_cycles = int(total_cycles // cycles_interval) if cycles_interval else 0
            times_due = max(times_due_by_hours, times_due_by_cycles)

            for occurrence in range(1, min(times_due, 5) + 1):  # cap at 5 historical occurrences per type
                due_date = datetime.now().date() - timedelta(days=random.randint(1, 700))
                is_completed = random.random() < 0.85  # most historical maintenance is done

                if is_completed:
                    started_at = datetime.combine(due_date, datetime.min.time()) + timedelta(hours=random.randint(0, 12))
                    completed_at = started_at + timedelta(hours=random.randint(4, 72))
                    status = "COMPLETED"
                    n_completed += 1
                else:
                    started_at, completed_at = None, None
                    status = "DUE"
                    n_due += 1

                cur.execute(
                    """INSERT INTO maintenance_events (aircraft_registration, schedule_id, maintenance_type,
                                                          event_status, due_date, started_at, completed_at, is_unscheduled)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,FALSE) RETURNING maintenance_event_id;""",
                    (reg, schedule_id, mtype, status, due_date, started_at, completed_at),
                )
                event_id = cur.fetchone()[0]

                if is_completed:
                    cur.execute(
                        """INSERT INTO maintenance_costs (maintenance_event_id, part_id, labor_cost, parts_cost)
                           VALUES (%s,%s,%s,%s);""",
                        (event_id, random.choice(part_ids), round(random.uniform(20000, 300000), 2),
                         round(random.uniform(5000, 500000), 2)),
                    )

        # Unscheduled maintenance: a defect drives an out-of-cycle event
        if random.random() < 0.3:
            due_date = datetime.now().date() - timedelta(days=random.randint(1, 200))
            cur.execute(
                """INSERT INTO maintenance_events (aircraft_registration, maintenance_type, event_status,
                                                      due_date, is_unscheduled)
                   VALUES (%s,'UNSCHEDULED','COMPLETED',%s,TRUE) RETURNING maintenance_event_id;""",
                (reg, due_date),
            )
            event_id = cur.fetchone()[0]
            severity = random.choice(["MINOR", "MAJOR", "CRITICAL"])
            cur.execute(
                """INSERT INTO defects (aircraft_registration, maintenance_event_id, defect_description,
                                          severity, defect_status, reported_at, resolved_at)
                   VALUES (%s,%s,%s,%s,'RESOLVED',%s,%s);""",
                (reg, event_id, random.choice(DEFECT_DESCRIPTIONS), severity,
                 due_date, due_date + timedelta(days=random.randint(1, 5))),
            )
            n_unscheduled += 1

        # Small chance an aircraft is currently grounded due to an open defect
        if random.random() < 0.08:
            cur.execute(
                "UPDATE aircraft SET is_grounded=TRUE, grounded_reason=%s, updated_at=NOW() WHERE aircraft_registration=%s;",
                (random.choice(DEFECT_DESCRIPTIONS), reg),
            )
            cur.execute(
                """INSERT INTO defects (aircraft_registration, defect_description, severity, defect_status, reported_at)
                   VALUES (%s,%s,'CRITICAL','OPEN', NOW());""",
                (reg, random.choice(DEFECT_DESCRIPTIONS)),
            )

    logger.info(
        "Maintenance events generated: %d completed, %d still due, %d unscheduled (defect-driven).",
        n_completed, n_due, n_unscheduled,
    )


def main():
    conn = get_connection()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            model_ids = seed_models(cur)
            schedule_map = seed_maintenance_schedules(cur, model_ids)
            aircraft = seed_aircraft(cur, model_ids)
            part_ids = seed_parts(cur)
        conn.commit()
        logger.info("Initial maintenance seed committed.")

        with conn.cursor() as cur:
            generate_maintenance_events(cur, aircraft, schedule_map, part_ids)
        conn.commit()
        logger.info("Maintenance events committed successfully.")

    except Exception as e:
        conn.rollback()
        logger.error("Seeding failed, rolled back: %s", e)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()