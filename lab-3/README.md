# Distributed Systems Labs

This project demonstrates a distributed system using Kafka for communication between various services. The system includes a Kafka broker, a Kafka admin service, a machine learning service, an analytics service, and a client service.

## Project Structure

```
lab-3/
├── docker-compose.yaml
├── analytics_service/
│   ├── analytics_service.py
│   └── Dockerfile
├── client/
│   ├── client.py
│   └── Dockerfile
├── kafka_admin/
│   ├── admin.py
│   └── Dockerfile
├── ml_service/
│   ├── ml_service.py
│   └── Dockerfile
└── README.md
```

### Services

1. **Kafka Broker**: Manages the message queue and facilitates communication between services.
2. **Kafka Admin Service**: Creates necessary Kafka topics.
3. **Machine Learning Service**: Listens to specific topics, processes events, and sends targeted ads.
4. **Analytics Service**: Listens to specific topics and updates user action counts.
5. **Client Service**: Produces events and listens for targeted ads.

## Flow

1. **Kafka Admin Service**:
   - Creates topics specified in the environment variable `TOPICS`.
   - Topics: `view`, `click`, `purchase`.

2. **Client Service**:
   - Produces events (`view`, `click`, `purchase`) to Kafka.
   - Listens for targeted ads on a specific topic.

3. **Machine Learning Service**:
   - Consumes events from `view` and `purchase` topics.
   - Processes events and sends targeted ads to specific user topics.

4. **Analytics Service**:
   - Consumes events from `view` and `click` topics.
   - Updates and logs user action counts.

## Usage

### Prerequisites

- Docker
- Docker Compose

### Steps

1. **Clone the repository**:
   ```sh
   git clone <repository-url>
   cd Distributed-Systems-Labs/lab-3
   ```

2. **Build and start the services**:
   ```sh
   docker-compose up --build
   ```

3. **Verify the services**:
   - Kafka Admin Service should create the topics.
   - Client Service should start producing events and listening for ads.
   - Machine Learning Service should process events and send ads.
   - Analytics Service should update and log user action counts.

### Environment Variables

- `KAFKA_HOST`: Kafka broker address (e.g., `kafka:9093`).
- `TOPICS`: List of topics to create (e.g., `["view", "click", "purchase"]`).
- `SELECTED_TOPICS`: Topics for ML and Analytics services to listen to.
- `EVENT_LISTENER_ADDRESS`: Kafka broker address for event consumption.
- `ADS_EMITTER_ADDRESS`: Kafka broker address for ad emission.
- `CLIENT_ID`: Unique ID for the client.
- `ACTIONS`: List of actions the client can perform (e.g., `["view", "click", "purchase"]`).
