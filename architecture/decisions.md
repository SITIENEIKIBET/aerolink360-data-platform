\# Architecture Decision Records — AeroLink360



\## ADR-001: Tiered Domain Depth



\*\*Context:\*\* Building all 10 business domains (from the master prompt) to

equal depth would spread effort thin and risk becoming a shallow

"many technologies, no real integration" project.



\*\*Decision:\*\*

\- Tier 1 (full depth — CDC, streaming, SCD2, dbt, DQ, Power BI):

&#x20; Reservations, Flight Operations, Aircraft \& Maintenance, Customer \& Loyalty

\- Tier 2 (real but batch-only, lighter streaming): Baggage, Finance

\- Tier 3 (modeled, not fully operationalized): Cargo, Crew, Commercial,

&#x20; Airport Operations



\*\*Consequences:\*\* Depth over breadth. Every Tier 1 domain must

demonstrate the full CDC→streaming→medallion→dbt→BI chain end to end;

Tier 3 domains have honest schema/sample data but no claim of a live

pipeline.



\## ADR-002: Kafka + Debezium, Self-Hosted in Docker



\*\*Context:\*\* The project requires realistic CDC and event streaming

without incurring cloud costs.



\*\*Decision:\*\* Run Kafka (KRaft mode, no Zookeeper) and Debezium

(Kafka Connect) entirely in Docker, with Debezium reading Postgres's

write-ahead log directly — genuine log-based CDC, not a simulation.



\*\*Alternatives considered:\*\* Managed cloud Kafka (Confluent Cloud,

Azure Event Hubs) — rejected due to cost; polling-based CDC simulation —

rejected because it wouldn't demonstrate real CDC mechanics (before/after

images, transaction IDs from the WAL).



\## ADR-003: Databricks Community Edition



\*\*Context:\*\* Full Databricks trial/paid workspaces require cloud

subscription billing (Azure/AWS/GCP), which previously required card

verification we don't have access to (see prior project's Azure DevOps

constraint).



\*\*Decision:\*\* Use Databricks Community Edition — free, no card required.



\*\*Trade-offs:\*\* Single-node cluster only, no Unity Catalog, some

streaming/job-scheduling features unavailable. Documented explicitly

rather than silently worked around.



\## ADR-004: Fabric as BI/Consumption Layer Only



\*\*Context:\*\* Fabric trial has a finite, uncertain remaining duration.



\*\*Decision:\*\* Scope Fabric's role to Lakehouse shortcuts + Power BI

semantic model — a consumption layer, not core pipeline infrastructure.



\*\*Consequences:\*\* If the trial expires mid-project, only the

presentation layer is affected; Databricks + local Kafka/Spark remain

the durable core.



\## ADR-005: Moderate Data Volume with Documented Scaling Path



\*\*Context:\*\* Free-tier/trial compute (demonstrated repeatedly in the

prior project) throttles hard under real load.



\*\*Decision:\*\* Generate 100k-500k rows per major fact table — enough to

exercise partitioning, incremental processing, and real query

performance characteristics — rather than literal millions.



\*\*Consequences:\*\* Every volume-sensitive design decision (partitioning

strategy, incremental vs. full load, indexing) is documented with an

explicit "how this scales to millions" note, so the architectural

reasoning is provable even without processing literal millions of rows.


## ADR-006: Hardcoded Connector Passwords (Known Shortcut, Documented)

**Context:** Debezium connector configs are registered via Kafka Connect's
REST API as JSON, which does not read .env files. All four connector
configs (kafka/*.json) contain the Postgres password in plaintext.

**Decision:** Accept this as a documented, temporary shortcut for local
development rather than solving it prematurely with infrastructure we
don't yet need.

**Production-correct alternatives (not implemented here):**
- Kafka Connect's built-in ConfigProvider mechanism (e.g. FileConfigProvider
  or EnvVarConfigProvider) to inject secrets at runtime without embedding
  them in the connector JSON.
- HashiCorp Vault or Azure Key Vault integration for genuine secrets
  management.

**Trade-offs:** kafka/*.json files must never be treated as safe to make
public as-is; they are gitignored... [continued below]

**Consequences:** kafka/*.json is added to .gitignore. A sanitized
kafka/*.example.json template (password placeholder) is committed instead,
matching the .env.example pattern used throughout this project.

**Trade-offs:** kafka/*.json connector files must never be treated as
safe to commit as-is, since they contain real database credentials.

**Consequences:** kafka/*-connector.json is gitignored. Sanitized
kafka/*-connector.example.json templates (password placeholder) are
committed instead, matching the .env.example pattern used throughout
this project.

**Trade-offs:** kafka/*.json connector files must never be treated as
safe to commit as-is, since they contain real database credentials.

**Consequences:** kafka/*-connector.json is gitignored. Sanitized
kafka/*-connector.example.json templates (password placeholder) are
committed instead, matching the .env.example pattern used throughout
this project.


## ADR-007: Per-Table Topic Naming (Debezium Default) Over Per-Event-Type

**Context:** Debezium's default topic naming is
{topic.prefix}.{schema}.{table} (e.g. aaa.reservations.public.bookings).
An alternative convention groups by business event type instead
(aaa.bookings, aaa.payments) using Debezium's ByLogicalTableRouter
transform.

**Decision:** Keep Debezium's per-table default. 17 topics currently
exist, each traceable 1:1 to a source table, verified against Phase 2's
known row counts (e.g. aaa.reservations.public.bookings = 501 messages,
exactly matching the 500-row snapshot + 1 live UPDATE proven in Phase 3).

**Alternatives considered:** ByLogicalTableRouter transform, consolidating
multiple tables into fewer business-event topics — rejected for now since
it requires reconfiguring 4 already-verified-working connectors for a
naming preference with no functional benefit at current scale; genuinely
worth revisiting if a consumer needed to subscribe to "all payment
activity" as one stream spanning tables.

**Consequences:** Consumers (Spark Structured Streaming in Phase 5) will
subscribe per-table, or use a topic pattern/regex to consume multiple
related topics from one domain at once (e.g. `aaa.reservations.*`).