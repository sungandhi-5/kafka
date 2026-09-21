# Kafka Node.js Microservices

A simple hands-on project demonstrating how Apache Kafka can be used for
asynchronous communication between Node.js microservices.

This project contains:

- A Producer Service
- A Consumer / Payment Service
- A Kafka broker running with Docker
- Kafka Consumer Groups
- Kafka Partitions
- Manual offset management
- Retry handling
- Dead Letter Queue (DLQ)
- A separate DLQ consumer

The project is intended for learning and understanding Kafka concepts
using Node.js and KafkaJS.

---

## Architecture

The project currently contains two Node.js services:

```text
                    ┌─────────────────────┐
                    │   Producer Service  │
                    │     Node.js API     │
                    └──────────┬──────────┘
                               │
                               │ Publish Order
                               ▼
                    ┌─────────────────────┐
                    │        Kafka        │
                    │                     │
                    │   Topic: orders     │
                    └──────────┬──────────┘
                               │
                               │ Consume Order
                               ▼
                    ┌─────────────────────┐
                    │   Consumer Service  │
                    │   Payment Service   │
                    └──────────┬──────────┘
                               │
                         Payment fails
                               │
                               ▼
                    ┌─────────────────────┐
                    │    orders-dlq       │
                    │   Kafka Topic       │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │    DLQ Consumer     │
                    └─────────────────────┘

```

---

### What Is This Project Used For?

This project demonstrates event-driven communication between
microservices using Apache Kafka.

Instead of one microservice directly calling another microservice using
HTTP:

```
Order Service
      |
      | HTTP Request
      ▼
Payment Service
```

the Producer Service publishes an event to Kafka:

```
Producer Service
      |
      | ORDER_CREATED
      ▼
Kafka
      |
      ▼
Payment Service
```

This allows the services to communicate asynchronously.

The Producer Service does not need to wait for the Payment Service to finish
processing the order.

---

# Why Kafka?

Kafka is useful when services need to communicate asynchronously and when
events need to be processed reliably and independently.

For example, when an order is created:

```
Order Service
      |
      ▼
Kafka
      |
      ├──────► Payment Service
      |
      ├──────► Inventory Service
      |
      ├──────► Notification Service
      |
      └──────► Analytics Service
```

The Order Service only needs to publish the ORDER_CREATED event.

Other services can consume the event independently.

---

# Kafka vs HTTP

## HTTP Communication

With HTTP, the Order Service directly calls the Payment Service:

```
Order Service
      |
      | POST /payments
      ▼
Payment Service
```

The Order Service depends on the Payment Service being available at that
moment.

---

## Kafka Communication

With Kafka:

```
Order Service
      |
      | ORDER_CREATED
      ▼
Kafka
      |
      ▼
Payment Service
```

Kafka stores the event until the consumer processes it.

If the Payment Service is temporarily unavailable, the event can remain in
Kafka and be processed when the Payment Service becomes available again.

---

# Prerequisites

- Node.js (Version 18+)
- Docker

# Setup on Localhost

1. Clone the repository
2. Start Kafka

The project contains a docker-compose.yml file in the root directory.

```bash
docker compose up -d
```
The -d option starts the containers in detached mode.

Check running containers:

```bash
docker ps
```

3. Install Producer Service Dependencies
4. Install Consumer Service Dependencies

5. Start the Consumer Service

```bash
cd consumer-service && node index.js
```

6. Start the Consumer Service

```bash
cd producer-service && node index.js
```

7. Test Communication

```bash
curl -X POST http://localhost:3000/orders \
-H "Content-Type: application/json" \
-d '{"customer":"Jyorge","amount":999}'
```

# Message Flow

Once both services are running, the message flow is:

```
Producer Service
       |
       | Publish Order
       ▼
    Kafka
       |
       | orders topic
       ▼
Payment / Consumer Service
       |
       ├── Payment Successful
       |
       └── Payment Failed
                |
                ▼
            orders-dlq
                |
                ▼
           DLQ Consumer
```

---

# Create DLQ Topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders-dlq \
  --partitions 1 \
  --replication-factor 1
```

---

# Testing Partition Distribution
Create a topic with three partitions:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 1
```

Run two consumer instances:

```
Payment-1
Payment-2
```

Both should use:

```
groupId: payment-group
```

Kafka will distribute the partitions between the consumers.

# Useful Kafka Commands

## List Topics

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

## Describe the Orders Topic


```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders
```
