# Fault-Tolerant Webhook Delivery & Async Task Engine

This project is a fault-tolerant, asynchronous webhook delivery and background task execution engine. It is designed to handle distributed system notifications reliably across untrusted networks, handling upstream event ingestion, rate limiting, and external dispatching.

## Key Features

- **Task Ingestion API:** Fast RESTful endpoint (`POST /api/v1/tasks/dispatch`) for queuing webhooks.
- **Fair Rate Limiting:** Dynamic sliding-window rate limits per tenant using Redis.
- **Payload Security:** Cryptographic payload signing using HMAC-SHA256.
- **Asynchronous Dispatch:** Non-blocking HTTP dispatch using Java 21 Virtual Threads.
- **Exponential Backoff:** Configurable retries using RabbitMQ Delayed Message Exchanges.
- **Dead Letter Queue (DLQ):** Quarantine for persistently failing tasks.
- **Audit Logging:** Postgres tracking for every lifecycle transition.

## Architecture

We use a combination of Spring Boot (Java 21), PostgreSQL (audit logs), Redis (rate limiting), and RabbitMQ (async message brokering).

## Learning Objectives

While building this project, we will cover:
- How to implement and leverage Java 21 Virtual Threads for non-blocking I/O.
- Managing distributed rate limiting with Redis Lua scripts.
- Building robust retry mechanisms and Dead Letter Queues with RabbitMQ.
- Ensuring payload authenticity using cryptographic HMAC-SHA256 signatures.

## Getting Started

*(Instructions for local setup, Docker Compose, etc. will be added here as we progress through the build)*
