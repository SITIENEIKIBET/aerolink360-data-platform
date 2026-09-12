-- Flight Operations Domain Schema — AeroLink Africa Airways

DROP TABLE IF EXISTS cancellations CASCADE;
DROP TABLE IF EXISTS diversions CASCADE;
DROP TABLE IF EXISTS delays CASCADE;
DROP TABLE IF EXISTS flight_events CASCADE;
DROP TABLE IF EXISTS aircraft_assignments CASCADE;
DROP TABLE IF EXISTS flight_segments CASCADE;
DROP TABLE IF EXISTS flights CASCADE;
DROP TABLE IF EXISTS schedules CASCADE;
DROP TABLE IF EXISTS gates CASCADE;
DROP TABLE IF EXISTS airports CASCADE;

-- Airports are designed to be added dynamically, not hardcoded.
CREATE TABLE airports (
    airport_code    VARCHAR(4) PRIMARY KEY,   -- IATA code, e.g. NBO, MBA, JFK
    airport_name    VARCHAR(150) NOT NULL,
    city            VARCHAR(100) NOT NULL,
    country         VARCHAR(100) NOT NULL,
    timezone        VARCHAR(50),
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE gates (
    gate_id         SERIAL PRIMARY KEY,
    airport_code    VARCHAR(4) REFERENCES airports(airport_code),
    gate_number     VARCHAR(10) NOT NULL,
    terminal        VARCHAR(20),
    created_at      TIMESTAMP DEFAULT NOW()
);

-- The recurring template a real operating flight is instantiated from.
CREATE TABLE schedules (
    schedule_id             SERIAL PRIMARY KEY,
    flight_number            VARCHAR(20) NOT NULL,   -- e.g. AA452
    origin_airport           VARCHAR(4) REFERENCES airports(airport_code),
    destination_airport      VARCHAR(4) REFERENCES airports(airport_code),
    days_of_week              VARCHAR(50),            -- e.g. 'MON,WED,FRI'
    scheduled_departure_time  TIME NOT NULL,
    scheduled_arrival_time    TIME NOT NULL,
    effective_from            DATE NOT NULL,
    effective_to              DATE,
    created_at                TIMESTAMP DEFAULT NOW()
);

-- One row per actual operating flight instance on a specific date.
CREATE TABLE flights (
    flight_id           VARCHAR(20) PRIMARY KEY,  -- e.g. AAA452 (matches reservations.booking_items.flight_id)
    schedule_id         INTEGER REFERENCES schedules(schedule_id),
    flight_date          DATE NOT NULL,
    origin_airport        VARCHAR(4) REFERENCES airports(airport_code),
    destination_airport   VARCHAR(4) REFERENCES airports(airport_code),
    scheduled_departure    TIMESTAMP NOT NULL,
    scheduled_arrival       TIMESTAMP NOT NULL,
    actual_departure         TIMESTAMP,
    actual_arrival            TIMESTAMP,
    flight_status              VARCHAR(30) NOT NULL,  -- SCHEDULED, BOARDING, DEPARTED, ARRIVED, DELAYED, CANCELLED, DIVERTED
    gate_id                     INTEGER REFERENCES gates(gate_id),
    aircraft_registration        VARCHAR(20),          -- cross-system ref to maintenance domain, no FK
    created_at                    TIMESTAMP DEFAULT NOW(),
    updated_at                     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE flight_segments (
    segment_id       SERIAL PRIMARY KEY,
    flight_id         VARCHAR(20) REFERENCES flights(flight_id),
    segment_sequence   INTEGER NOT NULL DEFAULT 1,
    origin_airport       VARCHAR(4) REFERENCES airports(airport_code),
    destination_airport   VARCHAR(4) REFERENCES airports(airport_code),
    created_at             TIMESTAMP DEFAULT NOW()
);

CREATE TABLE aircraft_assignments (
    assignment_id         SERIAL PRIMARY KEY,
    flight_id              VARCHAR(20) REFERENCES flights(flight_id),
    aircraft_registration    VARCHAR(20) NOT NULL,
    assigned_at               TIMESTAMP DEFAULT NOW()
);

-- Append-heavy event log — a different CDC shape than the mostly-UPDATE
-- pattern in Reservations. Mostly INSERT events: boarding started,
-- doors closed, pushback, wheels up, etc.
CREATE TABLE flight_events (
    event_id        SERIAL PRIMARY KEY,
    flight_id        VARCHAR(20) REFERENCES flights(flight_id),
    event_type        VARCHAR(50) NOT NULL,  -- BOARDING_STARTED, GATE_CHANGED, DEPARTED, ARRIVED, etc.
    event_details       TEXT,
    event_timestamp       TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE delays (
    delay_id        SERIAL PRIMARY KEY,
    flight_id        VARCHAR(20) REFERENCES flights(flight_id),
    delay_minutes      INTEGER NOT NULL,
    delay_reason         VARCHAR(255),
    recorded_at            TIMESTAMP DEFAULT NOW()
);

CREATE TABLE diversions (
    diversion_id            SERIAL PRIMARY KEY,
    flight_id                VARCHAR(20) REFERENCES flights(flight_id),
    original_destination       VARCHAR(4) REFERENCES airports(airport_code),
    diverted_to                  VARCHAR(4) REFERENCES airports(airport_code),
    diversion_reason               VARCHAR(255),
    diverted_at                      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE cancellations (
    cancellation_id      SERIAL PRIMARY KEY,
    flight_id              VARCHAR(20) REFERENCES flights(flight_id),
    cancellation_reason       VARCHAR(255),
    cancelled_at                TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_gates_airport ON gates(airport_code);
CREATE INDEX idx_flights_schedule ON flights(schedule_id);
CREATE INDEX idx_flights_status ON flights(flight_status);
CREATE INDEX idx_flight_events_flight ON flight_events(flight_id);
CREATE INDEX idx_delays_flight ON delays(flight_id);