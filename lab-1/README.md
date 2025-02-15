# Lab Introduction

In this lab, you will explore different concurrency models by implementing and testing various server architectures. You will use Docker to containerize the servers and JMeter to perform load testing. The goal is to understand the performance characteristics of each concurrency model under different workloads.

# Dependency Installation

Before you begin, ensure you have the following dependencies installed on your system:

- Docker: [Install Docker](https://docs.docker.com/get-docker/)
- Docker Compose: [Install Docker Compose](https://docs.docker.com/compose/install/)
- Python 3.x: [Install Python](https://www.python.org/downloads/)
- JMeter: [Install JMeter](https://jmeter.apache.org/download_jmeter.cgi)

You can verify the installation of these dependencies by running the following commands:

```sh
docker --version
docker-compose --version
python3 --version
jmeter --version
```

# How to Build and Run the Servers

## Using Docker Compose

### Build and Start the Servers:
```bash
docker-compose up
```

### Stop the Servers:
```bash
docker-compose down
```

## How to Run the Client
The client can be run to send requests to the servers.

### Run the Client:
```bash
cd client
python client.py --num_requests 10
```




# JMeter Load Test Script - Example Usage

### To run a CPU-bound test on the sync sequential server:
```bash
./run_test.sh cpu 8081 10
```

### To run an IO-bound test on the async IO multiplexing server:
```bash
./run_test.sh io 8081 10
```