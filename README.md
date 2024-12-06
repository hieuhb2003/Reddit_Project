# Reddit Data Pipeline Project

## Usage

1. Start the Kafka, Spark, and PostgreSQL containers:

   ```bash
   docker-compose up -d
   ```

2. Run the Kafka producer to collect Reddit data:

   ```bash
   python producer/producer.py
   ```
