-- Customer & Loyalty Domain Schema — AeroLink Africa Airways

DROP TABLE IF EXISTS loyalty_transactions CASCADE;
DROP TABLE IF EXISTS loyalty_accounts CASCADE;
DROP TABLE IF EXISTS loyalty_tiers CASCADE;
DROP TABLE IF EXISTS customer_preferences CASCADE;

CREATE TABLE loyalty_tiers (
    tier_id         SERIAL PRIMARY KEY,
    tier_name       VARCHAR(30) NOT NULL,     -- Bronze, Silver, Gold, Platinum
    min_miles       INTEGER NOT NULL,
    benefits        TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE loyalty_accounts (
    loyalty_account_id   SERIAL PRIMARY KEY,
    customer_id             VARCHAR(20) NOT NULL,  -- cross-system ref to reservations.customers, no FK
    member_number              VARCHAR(30) UNIQUE NOT NULL,
    current_tier_id               INTEGER REFERENCES loyalty_tiers(tier_id),
    total_miles_earned              INTEGER DEFAULT 0,
    total_miles_redeemed               INTEGER DEFAULT 0,
    miles_balance                        INTEGER DEFAULT 0,
    enrollment_date                        DATE NOT NULL,
    created_at                               TIMESTAMP DEFAULT NOW(),
    updated_at                                 TIMESTAMP DEFAULT NOW()
);

-- Append-only ledger - every mile earned or redeemed is a row, never updated.
-- A different CDC shape again: pure INSERT stream, no UPDATEs at all.
CREATE TABLE loyalty_transactions (
    transaction_id          SERIAL PRIMARY KEY,
    loyalty_account_id        INTEGER REFERENCES loyalty_accounts(loyalty_account_id),
    transaction_type             VARCHAR(20) NOT NULL,  -- EARN, REDEEM, EXPIRE, ADJUSTMENT
    miles_amount                    INTEGER NOT NULL,
    reference_type                     VARCHAR(30),      -- BOOKING, PROMOTION, PARTNER, MANUAL
    reference_id                          VARCHAR(50),
    transaction_date                         TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE customer_preferences (
    preference_id        SERIAL PRIMARY KEY,
    customer_id             VARCHAR(20) NOT NULL,
    seat_preference            VARCHAR(20),   -- WINDOW, AISLE, MIDDLE
    meal_preference               VARCHAR(30),
    preferred_language              VARCHAR(30),
    marketing_opt_in                  BOOLEAN DEFAULT TRUE,
    created_at                          TIMESTAMP DEFAULT NOW(),
    updated_at                            TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_loyalty_accounts_customer ON loyalty_accounts(customer_id);
CREATE INDEX idx_loyalty_txns_account ON loyalty_transactions(loyalty_account_id);
CREATE INDEX idx_prefs_customer ON customer_preferences(customer_id);