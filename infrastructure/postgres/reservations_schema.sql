-- Reservations Domain Schema — AeroLink Africa Airways
-- Deliberately normalized (OLTP-style) to mirror a real reservation system.

DROP TABLE IF EXISTS cancellations CASCADE;
DROP TABLE IF EXISTS refunds CASCADE;
DROP TABLE IF EXISTS payments CASCADE;
DROP TABLE IF EXISTS tickets CASCADE;
DROP TABLE IF EXISTS booking_items CASCADE;
DROP TABLE IF EXISTS bookings CASCADE;
DROP TABLE IF EXISTS fare_rules CASCADE;
DROP TABLE IF EXISTS fare_classes CASCADE;
DROP TABLE IF EXISTS passengers CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

CREATE TABLE customers (
    customer_id     SERIAL PRIMARY KEY,
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    email           VARCHAR(255),
    phone           VARCHAR(50),
    nationality     VARCHAR(100),
    date_of_birth   DATE,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE passengers (
    passenger_id    SERIAL PRIMARY KEY,
    customer_id     INTEGER REFERENCES customers(customer_id),
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    passport_number VARCHAR(50),
    date_of_birth   DATE,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE fare_classes (
    fare_class_id   SERIAL PRIMARY KEY,
    class_code      VARCHAR(10) NOT NULL,     -- e.g. Y, J, F
    class_name      VARCHAR(50) NOT NULL,     -- Economy, Business, First
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE fare_rules (
    fare_rule_id        SERIAL PRIMARY KEY,
    fare_class_id       INTEGER REFERENCES fare_classes(fare_class_id),
    refundable          BOOLEAN DEFAULT FALSE,
    change_fee          NUMERIC(10,2) DEFAULT 0,
    baggage_allowance_kg INTEGER DEFAULT 23,
    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE bookings (
    booking_id      SERIAL PRIMARY KEY,
    customer_id     INTEGER REFERENCES customers(customer_id),
    booking_reference VARCHAR(20) UNIQUE NOT NULL,
    booking_status  VARCHAR(30) NOT NULL,     -- PENDING, CONFIRMED, CANCELLED, REFUNDED
    booking_channel VARCHAR(30),              -- WEB, MOBILE, AGENT, CALL_CENTER
    booking_date    TIMESTAMP NOT NULL,
    total_amount    NUMERIC(12,2) NOT NULL,
    currency        VARCHAR(3) DEFAULT 'KES',
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE booking_items (
    booking_item_id SERIAL PRIMARY KEY,
    booking_id      INTEGER REFERENCES bookings(booking_id),
    passenger_id    INTEGER REFERENCES passengers(passenger_id),
    flight_id       VARCHAR(20) NOT NULL,     -- references flightops domain (cross-system, no FK)
    fare_class_id   INTEGER REFERENCES fare_classes(fare_class_id),
    seat_number     VARCHAR(10),
    fare_amount     NUMERIC(10,2) NOT NULL,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE tickets (
    ticket_id       SERIAL PRIMARY KEY,
    booking_item_id INTEGER REFERENCES booking_items(booking_item_id),
    ticket_number   VARCHAR(30) UNIQUE NOT NULL,
    ticket_status   VARCHAR(30) NOT NULL,     -- ISSUED, USED, CANCELLED, REFUNDED
    issued_at       TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE payments (
    payment_id      SERIAL PRIMARY KEY,
    booking_id      INTEGER REFERENCES bookings(booking_id),
    payment_method  VARCHAR(30),              -- CARD, MPESA, BANK_TRANSFER
    amount          NUMERIC(12,2) NOT NULL,
    currency        VARCHAR(3) DEFAULT 'KES',
    payment_status  VARCHAR(30) NOT NULL,     -- PENDING, COMPLETED, FAILED
    paid_at         TIMESTAMP,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE refunds (
    refund_id       SERIAL PRIMARY KEY,
    payment_id      INTEGER REFERENCES payments(payment_id),
    refund_amount   NUMERIC(12,2) NOT NULL,
    refund_reason   VARCHAR(255),
    refund_status   VARCHAR(30) NOT NULL,     -- REQUESTED, APPROVED, PROCESSED, REJECTED
    requested_at    TIMESTAMP DEFAULT NOW(),
    processed_at    TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE cancellations (
    cancellation_id SERIAL PRIMARY KEY,
    booking_id      INTEGER REFERENCES bookings(booking_id),
    cancelled_by    VARCHAR(30),              -- CUSTOMER, AIRLINE
    cancellation_reason VARCHAR(255),
    cancelled_at    TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_passengers_customer ON passengers(customer_id);
CREATE INDEX idx_bookings_customer ON bookings(customer_id);
CREATE INDEX idx_booking_items_booking ON booking_items(booking_id);
CREATE INDEX idx_tickets_booking_item ON tickets(booking_item_id);
CREATE INDEX idx_payments_booking ON payments(booking_id);