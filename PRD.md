# Product Requirements Document
## High-Volume Microservices Platform

**Document Version:** 1.0  
**Last Updated:** 2026-04-30  
**Author:** Martin Nicholson  
**Status:** DRAFT

---

## Executive Summary

This document defines the architecture, requirements, and implementation details for a distributed microservices platform designed to process high-volume business transactions through eight independently scalable services. The system leverages event-driven architecture with Kafka for asynchronous communication, PostgreSQL for persistent storage, and Kubernetes for orchestration. The entire platform can be deployed locally using Docker containers with a single command, enabling development and testing in environments identical to production.

---

## 1. Vision & Objectives

### 1.1 Vision Statement

Create a scalable, event-driven microservices platform that processes business transactions through specialized, domain-focused services while maintaining data consistency and auditability through event sourcing patterns.

### 1.2 Core Objectives

- **Scalability**: Each service scales independently based on demand
- **Reliability**: Asynchronous event processing with guaranteed message delivery
- **Auditability**: Complete audit trail of all state changes through event logs
- **Developer Experience**: Single-command local deployment matching production
- **Separation of Concerns**: Each service owns its domain and database
- **Observability**: Clear event flows and service interactions

---

## 2. Problem Statement

Organizations processing high volumes of business transactions need to:
- Handle complex business workflows spanning multiple domains
- Maintain data consistency across distributed systems
- Audit all changes for compliance and debugging
- Scale individual components independently
- Enable development without infrastructure complexity

This platform addresses these challenges through a carefully designed microservices architecture with event-driven communication.

---

## 3. System Architecture

### 3.1 Overview

The platform consists of eight specialized microservices, each responsible for a specific business domain. Services communicate exclusively through Kafka events, ensuring loose coupling and independent scalability.

```
Inbound Kafka Topic
         ↓
    Declaration Service
         ↓
    Kafka Events
    (with Debezium Outbox)
         ↙ ↓ ↘
    [Parallel Processing]
     ↙    ↓    ↘
Validate  Audit  Risk
     ↘    ↓    ↙
    Kafka Events
         ↓
    Parallel Processing
    ↙       ↓       ↘
Calculate  Payment  Notify
     ↘      ↓      ↙
    Kafka Events
         ↓
    Present Service
         ↓
    Output / Database
```

### 3.2 Technology Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Language | Go | Latest Stable | All microservices |
| Database | PostgreSQL | 15+ | Data persistence per service |
| Message Queue | Apache Kafka | 3.x | Event streaming |
| Change Data Capture | Debezium | Latest | Database → Kafka synchronization |
| Container Runtime | Docker | Latest | Service containerization |
| Orchestration | Kubernetes | 1.27+ | Production deployment |
| Local Development | Docker Compose | Latest | Single-command local setup |

### 3.3 Service Descriptions

#### 3.3.1 Declaration Service
- **Purpose**: Entry point for business transactions
- **Input**: Inbound Kafka topic with raw transaction data
- **Responsibilities**:
  - Validate incoming message format
  - Create declaration records in database
  - Emit DeclarationCreated events via outbox pattern
- **Database**: PostgreSQL (declarations schema)
- **Dependencies**: None (entry point)
- **Key Events**:
  - `declaration.created`
  - `declaration.validated`

#### 3.3.2 Validate Service
- **Purpose**: Validate business rules and data integrity
- **Input**: DeclarationCreated event
- **Responsibilities**:
  - Validate declarations against business rules
  - Check data completeness and accuracy
  - Emit validation results
- **Database**: PostgreSQL (validation schema)
- **Dependencies**: Listens to declaration service
- **Key Events**:
  - `validation.passed`
  - `validation.failed`

#### 3.3.3 Audit Service
- **Purpose**: Track all changes for compliance and debugging
- **Input**: DeclarationCreated event
- **Responsibilities**:
  - Record audit trail of all transactions
  - Maintain immutable audit logs
  - Enable historical queries
- **Database**: PostgreSQL (audit schema)
- **Dependencies**: Listens to declaration service
- **Key Events**:
  - `audit.logged`

#### 3.3.4 Risk Service
- **Purpose**: Perform risk assessment and analysis
- **Input**: DeclarationCreated event
- **Responsibilities**:
  - Analyze transaction risk factors
  - Apply risk scoring algorithms
  - Flag high-risk transactions
- **Database**: PostgreSQL (risk schema)
- **Dependencies**: Listens to declaration service
- **Key Events**:
  - `risk.assessed`
  - `risk.flagged`

#### 3.3.5 Calculate Service
- **Purpose**: Perform financial and computational calculations
- **Input**: validation.passed, risk.assessed events
- **Responsibilities**:
  - Calculate fees, totals, and allocations
  - Apply business logic computations
  - Generate calculated results
- **Database**: PostgreSQL (calculation schema)
- **Dependencies**: Validate, Risk services
- **Key Events**:
  - `calculation.completed`

#### 3.3.6 Payment Service
- **Purpose**: Handle payment processing
- **Input**: calculation.completed event
- **Responsibilities**:
  - Process payments to designated accounts
  - Track payment status and history
  - Handle payment failures and retries
- **Database**: PostgreSQL (payment schema)
- **Dependencies**: Calculate service
- **Key Events**:
  - `payment.initiated`
  - `payment.completed`
  - `payment.failed`

#### 3.3.7 Notify Service
- **Purpose**: Send notifications and alerts
- **Input**: Events from throughout the workflow
- **Responsibilities**:
  - Send notifications (email, SMS, webhooks)
  - Alert on validation failures and high-risk transactions
  - Manage notification delivery and retries
- **Database**: PostgreSQL (notification schema)
- **Responsibilities**: Track notification delivery
- **Dependencies**: All services
- **Key Events**:
  - `notification.sent`
  - `notification.failed`

#### 3.3.8 Present Service
- **Purpose**: Aggregate results and present outcomes
- **Input**: payment.completed, notification.sent events
- **Responsibilities**:
  - Aggregate transaction results
  - Format output for consumption
  - Archive completed transactions
  - Provide query interface for results
- **Database**: PostgreSQL (presentation schema)
- **Dependencies**: All other services
- **Key Events**:
  - `transaction.completed`

---

## 4. Technical Requirements

### 4.1 Service Implementation (Go)

All services must be implemented in Go and follow these requirements:

#### 4.1.1 Standard Service Structure

```
service-name/
├── cmd/
│   └── main.go                 # Entry point
├── internal/
│   ├── domain/                 # Domain models
│   ├── handler/                # Event/HTTP handlers
│   ├── repository/             # Data access layer
│   ├── config/                 # Configuration
│   └── service/                # Business logic
├── migrations/                 # Database migrations (SQL)
├── Dockerfile                  # Container image
├── go.mod & go.sum
├── README.md
└── tests/                      # Unit & integration tests
```

#### 4.1.2 Core Libraries & Dependencies

- **Kafka Client**: `segmentio/kafka-go` or `shopify/sarama`
- **Database Driver**: `pq` (PostgreSQL)
- **ORM** (optional): `gorm` or hand-written SQL
- **HTTP Framework** (optional): `gin`, `echo`, or `chi`
- **Configuration**: `viper` or environment variables
- **Logging**: `zap` or `logrus` (structured logging)
- **JSON Serialization**: Standard `encoding/json`
- **Testing**: `testify` for assertions

#### 4.1.3 Service Startup Requirements

Each service must:
- Read configuration from environment variables
- Connect to PostgreSQL database
- Run database migrations automatically
- Connect to Kafka cluster
- Register event listeners
- Perform health checks
- Start HTTP server for metrics/health endpoints

#### 4.1.4 Error Handling & Resilience

- Implement exponential backoff for Kafka retries
- Handle database connection failures gracefully
- Provide circuit breaker patterns for external calls
- Log all errors with context and trace IDs
- Implement health check endpoints (`/health`)
- Support graceful shutdown (signal handling)

### 4.2 Database Requirements

#### 4.2.1 PostgreSQL Configuration

- **Version**: 15 or later
- **Per-Service Schema**: Each service owns a schema in a shared PostgreSQL instance
- **Connection Pooling**: Use pgBouncer or connection pooling in application
- **Backup**: Daily backups to persistent storage

#### 4.2.2 Schema Design

Each service database schema must include:

- **Core Tables**: Domain-specific tables for the service
- **Outbox Table**: For Debezium CDC pattern
  ```sql
  CREATE TABLE outbox (
    id BIGSERIAL PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    processed BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMP
  );
  ```
- **Migrations**: All schema changes as versioned SQL files

#### 4.2.3 Data Consistency

- Use transactions for multi-step operations
- Implement idempotent operations (safe to retry)
- Use optimistic locking where appropriate
- No direct cross-database joins (use application-level joins)

### 4.3 Kafka Event Streaming

#### 4.3.1 Topic Design

- **Naming Convention**: `{service}.{event-type}` (e.g., `declaration.created`)
- **Partitioning**: Partition by aggregate ID for ordering guarantees
- **Retention**: 7 days (configurable)
- **Replication Factor**: 3 (production)

#### 4.3.2 Event Format

All events must follow this JSON schema:

```json
{
  "id": "UUID",
  "timestamp": "ISO-8601",
  "aggregateId": "UUID",
  "aggregateType": "string",
  "eventType": "string",
  "version": 1,
  "payload": {
    // Event-specific data
  },
  "metadata": {
    "correlationId": "UUID",
    "causationId": "UUID",
    "traceId": "UUID",
    "source": "service-name"
  }
}
```

#### 4.3.3 Outbox Pattern Implementation

- **Debezium Connector**: CDC from PostgreSQL outbox tables to Kafka
- **Single Transaction**: Database write and outbox entry in same transaction
- **Processing**: Debezium automatically publishes outbox entries to Kafka
- **Configuration**: Deploy Debezium connector per service

### 4.4 Kubernetes Deployment

#### 4.4.1 Deployment Strategy

- **StatefulSets**: For services with persistent state
- **Deployments**: For stateless services
- **ConfigMaps**: Service configuration
- **Secrets**: Sensitive data (credentials, API keys)
- **PersistentVolumes**: PostgreSQL data storage

#### 4.4.2 Resource Requirements

Each service deployment must specify:
- **CPU Requests/Limits**: Based on performance testing
- **Memory Requests/Limits**: Based on workload
- **Liveness Probes**: `/health` endpoint (10s interval)
- **Readiness Probes**: Service-specific check (5s interval)

#### 4.4.3 Scaling & Load Balancing

- **Horizontal Pod Autoscaling**: Based on CPU/memory metrics
- **Service Discovery**: Kubernetes DNS
- **Load Balancing**: Round-robin across pod replicas
- **Network Policies**: Restrict inter-service communication

---

## 5. Local Development Environment

### 5.1 Single-Command Deployment

The entire platform must start with a single command:

```bash
make dev-up
# or
docker-compose -f docker-compose.dev.yml up
```

This command must:
1. Build all service Docker images
2. Start PostgreSQL container with all schemas
3. Start Kafka broker and Zookeeper
4. Start Debezium connector containers
5. Run database migrations
6. Start all 8 microservices
7. Create Kafka topics
8. Expose services on localhost with predictable ports

### 5.2 Local Docker Compose Setup

#### 5.2.1 Services to Deploy

```yaml
Services:
  - postgres (port 5432)
  - kafka (port 9092)
  - zookeeper (port 2181)
  - debezium (port 8083)
  - declaration-service (port 8001)
  - validate-service (port 8002)
  - audit-service (port 8003)
  - risk-service (port 8004)
  - calculate-service (port 8005)
  - payment-service (port 8006)
  - notify-service (port 8007)
  - present-service (port 8008)
```

#### 5.2.2 Volume Mounts

- **PostgreSQL Data**: Named volume `postgres-data`
- **Kafka Data**: Named volume `kafka-data`
- **Source Code**: Bind mounts for local development

#### 5.2.3 Environment Configuration

All services must work with environment variables:
- `DATABASE_URL`: PostgreSQL connection string
- `KAFKA_BROKERS`: Comma-separated Kafka broker addresses
- `SERVICE_PORT`: HTTP server port
- `LOG_LEVEL`: Logging verbosity
- `ENVIRONMENT`: dev/staging/prod

### 5.3 Development Workflow

Developers can:
- Modify code locally and rebuild services
- Attach debuggers to running containers
- View logs from all services: `docker-compose logs -f`
- Reset environment: `docker-compose down -v && docker-compose up`
- Access databases directly for inspection
- Manually publish test events to Kafka

### 5.4 Testing Strategy

#### 5.4.1 Unit Testing

- Go test files co-located with source (`*_test.go`)
- Minimum 70% code coverage
- Mock external dependencies

#### 5.4.2 Integration Testing

- Docker Compose with test databases
- Test event flow end-to-end
- Verify database state transitions
- Test Kafka message processing

#### 5.4.3 Load Testing

- Simulate high-volume event ingestion
- Monitor service performance under load
- Verify horizontal scaling
- Test Kafka throughput limits

---

## 6. Data Flow & Event Sequencing

### 6.1 Happy Path: Transaction Processing

```
1. Inbound Event (External Source)
   └─> Kafka Topic: "inbound"

2. Declaration Service
   ├─> Consumes: inbound event
   ├─> Creates: declaration record in DB
   ├─> Publishes (Outbox Pattern):
   │   └─> declaration.created event
   └─> Kafka: declaration.created

3. Parallel Processing (Fan-out)
   Validate Service, Audit Service, Risk Service
   consume declaration.created:
   
   ├─> Validate Service
   │   ├─> Validates business rules
   │   ├─> Updates validation records
   │   └─> Publishes: validation.passed/failed
   │
   ├─> Audit Service
   │   ├─> Records audit trail
   │   └─> Publishes: audit.logged
   │
   └─> Risk Service
       ├─> Calculates risk score
       ├─> Updates risk records
       └─> Publishes: risk.assessed

4. Conditional Processing
   Calculate Service
   ├─> Consumes: validation.passed AND risk.assessed
   ├─> Performs calculations
   ├─> Updates calculation records
   └─> Publishes: calculation.completed

5. Sequential Processing
   ├─> Payment Service
   │   ├─> Consumes: calculation.completed
   │   ├─> Processes payment
   │   └─> Publishes: payment.completed/failed
   │
   └─> Notify Service
       ├─> Consumes: events from all services
       ├─> Sends notifications
       └─> Publishes: notification.sent

6. Aggregation
   Present Service
   ├─> Consumes: payment.completed, notification.sent
   ├─> Aggregates results
   ├─> Stores final transaction
   └─> Publishes: transaction.completed
```

### 6.2 Error Handling & Compensations

- **Validation Failure**: Emit validation.failed, alert via Notify service
- **Payment Failure**: Retry with exponential backoff, alert stakeholders
- **Risk Flags**: Alert operations team, may require manual intervention
- **Notification Failure**: Retry via dead-letter queue, escalate if repeated

---

## 7. Event Catalog

### 7.1 Declaration Service Events

| Event | Schema | Trigger | Consumers |
|-------|--------|---------|-----------|
| `declaration.created` | DeclarationCreated | Raw inbound event received | Validate, Audit, Risk |
| `declaration.invalid` | DeclarationInvalid | Format validation failed | Notify |

### 7.2 Validate Service Events

| Event | Schema | Trigger | Consumers |
|-------|--------|---------|-----------|
| `validation.passed` | ValidationPassed | All business rules satisfied | Calculate |
| `validation.failed` | ValidationFailed | Business rule violation | Notify, Audit |

### 7.3 Audit Service Events

| Event | Schema | Trigger | Consumers |
|-------|--------|---------|-----------|
| `audit.logged` | AuditLogged | Audit record created | Present (optional) |

### 7.4 Risk Service Events

| Event | Schema | Trigger | Consumers |
|-------|--------|---------|-----------|
| `risk.assessed` | RiskAssessed | Risk calculation completed | Calculate, Notify |
| `risk.flagged` | RiskFlagged | High-risk transaction detected | Notify, Audit |

### 7.5 Calculate Service Events

| Event | Schema | Trigger | Consumers |
|-------|--------|---------|-----------|
| `calculation.completed` | CalculationCompleted | Calculations finished | Payment |

### 7.6 Payment Service Events

| Event | Schema | Trigger | Consumers |
|-------|--------|---------|-----------|
| `payment.initiated` | PaymentInitiated | Payment processing started | Notify |
| `payment.completed` | PaymentCompleted | Payment successful | Present, Notify |
| `payment.failed` | PaymentFailed | Payment declined/error | Notify, Audit |

### 7.7 Notify Service Events

| Event | Schema | Trigger | Consumers |
|-------|--------|---------|-----------|
| `notification.sent` | NotificationSent | Message sent to recipient | Present |
| `notification.failed` | NotificationFailed | Send failure/retry exhausted | Audit |

### 7.8 Present Service Events

| Event | Schema | Trigger | Consumers |
|-------|--------|---------|-----------|
| `transaction.completed` | TransactionCompleted | All processing finished | External systems (optional) |

---

## 8. Non-Functional Requirements

### 8.1 Performance

- **Latency**: 95th percentile transaction processing < 5 seconds (end-to-end)
- **Throughput**: Support 10,000+ events per second at peak
- **Service Response Time**: Individual service calls < 500ms (p95)
- **Database Query Performance**: All queries complete within 100ms (p95)

### 8.2 Availability & Reliability

- **Uptime**: 99.9% availability target
- **Message Delivery**: Exactly-once semantics for critical events
- **Data Durability**: No data loss for committed transactions
- **Graceful Degradation**: Partial failures don't cascade

### 8.3 Security

- **Authentication**: Service-to-service authentication (mTLS or JWT)
- **Authorization**: Role-based access control (RBAC)
- **Encryption**: TLS for all network communication
- **Data Protection**: Encrypt sensitive data at rest
- **Secrets Management**: Use Kubernetes Secrets for credentials
- **Audit Logging**: All actions logged with audit service

### 8.4 Observability

- **Logging**: Structured JSON logs with trace IDs
- **Metrics**: Prometheus metrics for all services
- **Distributed Tracing**: Trace requests across services
- **Alerting**: Alert on service degradation, errors, latency spikes
- **Health Checks**: Liveness and readiness probes on all services

### 8.5 Maintainability

- **Code Quality**: Follow Go best practices (gofmt, golint)
- **Documentation**: README for each service, inline comments for complexity
- **Testing**: Minimum 70% code coverage
- **CI/CD**: Automated testing and deployment pipeline
- **Versioning**: Semantic versioning for all services

---

## 9. Deployment Architecture

### 9.1 Local Development (Docker Compose)

- All services run in containers
- Single-command startup
- Persistent volumes for databases
- Network isolation between services

### 9.2 Production (Kubernetes)

```yaml
Namespace: microservices
Components:
  - StatefulSet: PostgreSQL (single instance with backups)
  - StatefulSet: Kafka cluster (3 brokers)
  - StatefulSet: Zookeeper cluster (3 nodes)
  - Deployment: Each microservice (3 replicas)
  - Deployment: Debezium connectors (1 per service)
  - Service: Internal DNS for all components
  - ConfigMap: Service configuration
  - Secret: Credentials and secrets
  - PersistentVolume: Data storage
  - HorizontalPodAutoscaler: Scale services based on metrics
```

### 9.3 Infrastructure Considerations

- **Database**: Managed PostgreSQL (AWS RDS, Azure Database, GCP Cloud SQL) recommended for production
- **Kafka**: Managed Kafka (AWS MSK, Confluent Cloud) recommended for production
- **Container Registry**: Private Docker registry (ECR, GCR, ACR)
- **Monitoring**: Prometheus + Grafana for metrics, ELK for logs

---

## 10. Development Milestones

### Phase 1: Foundation (Weeks 1-2)
- [ ] Set up project repository structure
- [ ] Create Docker Compose configuration
- [ ] Implement Declaration service (entry point)
- [ ] Set up PostgreSQL with schemas
- [ ] Configure Kafka cluster locally
- [ ] Implement Debezium CDC pattern

### Phase 2: Core Services (Weeks 3-4)
- [ ] Implement Validate service
- [ ] Implement Audit service
- [ ] Implement Risk service
- [ ] Create integration tests
- [ ] Add structured logging

### Phase 3: Business Logic (Weeks 5-6)
- [ ] Implement Calculate service
- [ ] Implement Payment service
- [ ] Implement Notify service
- [ ] Add error handling & retries
- [ ] Create load testing scenarios

### Phase 4: Aggregation & Polish (Weeks 7-8)
- [ ] Implement Present service
- [ ] Add distributed tracing
- [ ] Implement health checks & monitoring
- [ ] Performance optimization
- [ ] Documentation and runbooks

### Phase 5: Kubernetes & Production (Weeks 9-10)
- [ ] Create Kubernetes manifests
- [ ] Set up CI/CD pipeline
- [ ] Performance testing at scale
- [ ] Security hardening
- [ ] Production deployment

---

## 11. Success Criteria

- [x] Single-command local deployment works reliably
- [x] All 8 services process events end-to-end
- [x] Messages maintain order per partition
- [x] Database consistency maintained across services
- [x] Audit trail complete for all transactions
- [x] Services scale horizontally under load
- [x] Failures don't cascade (circuit breakers, timeouts)
- [x] Code coverage minimum 70% across all services
- [x] Prometheus metrics collected and visualized
- [x] Distributed tracing shows request flow
- [x] Kubernetes manifests support multi-environment deployment

---

## 12. Risks & Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| Kafka message loss | Low | Critical | Use replication factor 3, exactly-once semantics |
| Database consistency issues | Medium | High | Use transactions, implement idempotent operations |
| Service cascade failures | Medium | High | Circuit breakers, timeouts, graceful degradation |
| Performance bottleneck at Kafka | Low | High | Partition strategy, partition count tuning |
| Data migration complexity | High | Medium | Use blue-green deployments, feature flags |
| Operational complexity | High | Medium | Strong monitoring, runbooks, chaos engineering |

---

## 13. Appendix

### 13.1 Environment Variables Template

```bash
# PostgreSQL
DATABASE_HOST=postgres
DATABASE_PORT=5432
DATABASE_NAME=microservices
DATABASE_USER=postgres
DATABASE_PASSWORD=devpassword

# Kafka
KAFKA_BROKERS=kafka:9092
KAFKA_CONSUMER_GROUP=service-name-group
KAFKA_AUTO_OFFSET_RESET=earliest

# Service Configuration
SERVICE_NAME=declaration-service
SERVICE_PORT=8001
LOG_LEVEL=debug
ENVIRONMENT=development

# Tracing & Monitoring
JAEGER_AGENT_HOST=localhost
JAEGER_AGENT_PORT=6831
PROMETHEUS_PORT=9090
```

### 13.2 Docker Compose Health Check Template

```yaml
healthcheck:
  test: ["CMD", "curl", "-f", "http://localhost:8001/health"]
  interval: 10s
  timeout: 5s
  retries: 3
  start_period: 10s
```

### 13.3 Kubernetes Service Template

```yaml
apiVersion: v1
kind: Service
metadata:
  name: declaration-service
  namespace: microservices
spec:
  selector:
    app: declaration-service
  ports:
    - port: 8001
      targetPort: 8001
      name: http
    - port: 9090
      targetPort: 9090
      name: metrics
  type: ClusterIP
```

### 13.4 Go Service Template (main.go)

```go
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Load configuration
	cfg := loadConfig()

	// Initialize database
	db := initDatabase(cfg)
	defer db.Close()

	// Initialize Kafka consumer
	consumer := initKafkaConsumer(cfg)
	defer consumer.Close()

	// Set up HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/ready", readinessHandler)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: mux,
	}

	// Start Kafka event processing
	go processEvents(consumer, db, cfg)

	// Start HTTP server
	go func() {
		log.Printf("Starting server on port %d", cfg.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	server.Shutdown(ctx)
	log.Println("Server stopped")
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"healthy"}`))
}

func readinessHandler(w http.ResponseWriter, r *http.Request) {
	// Check database and Kafka connectivity
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"ready":true}`))
}
```

---

**End of Document**

