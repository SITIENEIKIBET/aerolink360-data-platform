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

