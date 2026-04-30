# Architecture and Message Flow

This document describes the high-level architecture of the `hi-volume-services` platform and the end-to-end event flow through all services.

## 1) Platform Architecture Diagram

```mermaid
graph TD
    EXT[External Producer] --> INBOUND[(Kafka Topic: inbound)]

    INBOUND --> DEC[declaration-service]

    DEC --> PG[(PostgreSQL)]
    DEC --> OUT_DEC[(Kafka: declaration.created)]

    OUT_DEC --> VAL[validate-service]
    OUT_DEC --> AUD[audit-service]
    OUT_DEC --> RISK[risk-service]

    VAL --> OUT_VAL_PASS[(Kafka: validation.passed)]
    VAL --> OUT_VAL_FAIL[(Kafka: validation.failed)]
    AUD --> OUT_AUD[(Kafka: audit.logged)]
    RISK --> OUT_RISK_ASSESSED[(Kafka: risk.assessed)]
    RISK --> OUT_RISK_FLAGGED[(Kafka: risk.flagged)]

    OUT_VAL_PASS --> CALC[calculate-service]
    OUT_RISK_ASSESSED --> CALC

    CALC --> OUT_CALC[(Kafka: calculation.completed)]
    OUT_CALC --> PAY[payment-service]
    PAY --> OUT_PAY[(Kafka: payment.completed)]

    OUT_PAY --> NOTIFY[notify-service]
    OUT_VAL_FAIL --> NOTIFY
    OUT_RISK_FLAGGED --> NOTIFY
    NOTIFY --> OUT_NOTIFY[(Kafka: notification.sent)]

    OUT_PAY --> PRESENT[present-service]
    OUT_NOTIFY --> PRESENT
    PRESENT --> OUT_DONE[(Kafka: transaction.completed)]

    OUT_DONE --> CONSUMER[External Consumer / Downstream Systems]
```

## 2) Message Flow Chart

```mermaid
flowchart LR
    A[inbound event] --> B[declaration-service consumes]
    B --> C[Persist declaration + outbox]
    C --> D[Emit declaration.created]

    D --> E1[validate-service]
    D --> E2[audit-service]
    D --> E3[risk-service]

    E1 --> F1[validation.passed or validation.failed]
    E2 --> F2[audit.logged]
    E3 --> F3[risk.assessed and optional risk.flagged]

    F1 -->|validation.passed| G[calculate-service waits for both]
    F3 -->|risk.assessed| G

    G --> H[Emit calculation.completed]
    H --> I[payment-service]
    I --> J[Emit payment.completed]

    J --> K1[notify-service]
    F1 -->|validation.failed| K1
    F3 -->|risk.flagged| K1
    K1 --> L[Emit notification.sent]

    J --> M[present-service waits for both]
    L --> M
    M --> N[Emit transaction.completed]
```

## Notes

- Every service writes to its own schema/tables and emits follow-up events.
- The pattern uses asynchronous Kafka communication and per-service persistence.
- `calculate-service` and `present-service` are join/aggregation points that wait on multiple prerequisite events.
