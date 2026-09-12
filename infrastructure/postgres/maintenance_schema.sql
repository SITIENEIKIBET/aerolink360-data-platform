-- Aircraft & Maintenance Domain Schema — AeroLink Africa Airways

DROP TABLE IF EXISTS maintenance_costs CASCADE;
DROP TABLE IF EXISTS defects CASCADE;
DROP TABLE IF EXISTS maintenance_events CASCADE;
DROP TABLE IF EXISTS maintenance_schedules CASCADE;
DROP TABLE IF EXISTS parts CASCADE;
DROP TABLE IF EXISTS engines CASCADE;
DROP TABLE IF EXISTS aircraft CASCADE;
DROP TABLE IF EXISTS aircraft_models CASCADE;

CREATE TABLE aircraft_models (
    model_id        SERIAL PRIMARY KEY,
    model_name      VARCHAR(100) NOT NULL,   -- e.g. Boeing 787-8, Embraer E190
    manufacturer    VARCHAR(100) NOT NULL,
    seat_capacity   INTEGER,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE aircraft (
    aircraft_registration   VARCHAR(20) PRIMARY KEY,  -- e.g. 5Y-AAA12 (matches flightops.flights.aircraft_registration)
    model_id                 INTEGER REFERENCES aircraft_models(model_id),
    manufacture_date           DATE,
    total_flight_hours           NUMERIC(10,2) DEFAULT 0,
    total_flight_cycles            INTEGER DEFAULT 0,
    is_grounded                      BOOLEAN DEFAULT FALSE,
    grounded_reason                    VARCHAR(255),
    created_at                           TIMESTAMP DEFAULT NOW(),
    updated_at                             TIMESTAMP DEFAULT NOW()
);

CREATE TABLE engines (
    engine_id           SERIAL PRIMARY KEY,
    aircraft_registration VARCHAR(20) REFERENCES aircraft(aircraft_registration),
    engine_serial          VARCHAR(50) NOT NULL,
    engine_position           VARCHAR(20),   -- e.g. LEFT, RIGHT
    total_engine_hours          NUMERIC(10,2) DEFAULT 0,
    installed_at                   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE parts (
    part_id             SERIAL PRIMARY KEY,
    part_name             VARCHAR(150) NOT NULL,
    part_number             VARCHAR(50) NOT NULL,
    stock_quantity             INTEGER DEFAULT 0,
    unit_cost                    NUMERIC(10,2),
    created_at                     TIMESTAMP DEFAULT NOW()
);

-- Defines WHEN maintenance becomes due, based on accumulated hours/cycles
-- or a fixed calendar interval - genuinely different logic from the
-- random-chance branching in Reservations/FlightOps.
CREATE TABLE maintenance_schedules (
    schedule_id            SERIAL PRIMARY KEY,
    model_id                 INTEGER REFERENCES aircraft_models(model_id),
    maintenance_type            VARCHAR(50) NOT NULL,  -- A_CHECK, B_CHECK, C_CHECK, D_CHECK
    interval_flight_hours          NUMERIC(10,2),
    interval_flight_cycles           INTEGER,
    interval_calendar_days              INTEGER,
    created_at                            TIMESTAMP DEFAULT NOW()
);

CREATE TABLE maintenance_events (
    maintenance_event_id      SERIAL PRIMARY KEY,
    aircraft_registration        VARCHAR(20) REFERENCES aircraft(aircraft_registration),
    schedule_id                     INTEGER REFERENCES maintenance_schedules(schedule_id),
    maintenance_type                   VARCHAR(50) NOT NULL,
    event_status                         VARCHAR(30) NOT NULL,  -- DUE, SCHEDULED, IN_PROGRESS, COMPLETED
    due_date                               DATE,
    started_at                               TIMESTAMP,
    completed_at                               TIMESTAMP,
    is_unscheduled                               BOOLEAN DEFAULT FALSE,
    created_at                                     TIMESTAMP DEFAULT NOW(),
    updated_at                                       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE defects (
    defect_id              SERIAL PRIMARY KEY,
    aircraft_registration     VARCHAR(20) REFERENCES aircraft(aircraft_registration),
    maintenance_event_id        INTEGER REFERENCES maintenance_events(maintenance_event_id),
    defect_description             TEXT NOT NULL,
    severity                          VARCHAR(20),  -- MINOR, MAJOR, CRITICAL
    defect_status                        VARCHAR(30) NOT NULL, -- OPEN, IN_REPAIR, RESOLVED
    reported_at                             TIMESTAMP DEFAULT NOW(),
    resolved_at                               TIMESTAMP
);

CREATE TABLE maintenance_costs (
    cost_id                 SERIAL PRIMARY KEY,
    maintenance_event_id       INTEGER REFERENCES maintenance_events(maintenance_event_id),
    part_id                       INTEGER REFERENCES parts(part_id),
    labor_cost                       NUMERIC(12,2) DEFAULT 0,
    parts_cost                          NUMERIC(12,2) DEFAULT 0,
    currency                               VARCHAR(3) DEFAULT 'KES',
    recorded_at                              TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_aircraft_model ON aircraft(model_id);
CREATE INDEX idx_engines_aircraft ON engines(aircraft_registration);
CREATE INDEX idx_maint_events_aircraft ON maintenance_events(aircraft_registration);
CREATE INDEX idx_maint_events_status ON maintenance_events(event_status);
CREATE INDEX idx_defects_aircraft ON defects(aircraft_registration);