#!/bin/bash

set -e

echo "========================================="
echo "Async Job Example - Quick Start"
echo "========================================="
echo ""

# Check prerequisites
echo "Checking prerequisites..."

if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

if ! command -v java &> /dev/null; then
    echo "❌ Java is not installed. Please install Java 21 or higher."
    exit 1
fi

if ! command -v mvn &> /dev/null; then
    echo "❌ Maven is not installed. Please install Maven 3.6 or higher."
    exit 1
fi

echo "✅ All prerequisites met"
echo ""

# Navigate to examples directory
cd "$(dirname "$0")"

# Start Docker services
echo "Starting infrastructure services (Postgres, Redis, Kafka)..."
docker-compose up -d

echo ""
echo "Waiting for services to be healthy..."
sleep 10

# Check service health
echo "Checking PostgreSQL..."
docker exec async-job-postgres pg_isready -U asyncjob || {
    echo "❌ PostgreSQL is not ready"
    exit 1
}

echo "Checking Redis..."
docker exec async-job-redis redis-cli ping || {
    echo "❌ Redis is not ready"
    exit 1
}

echo "✅ All services are healthy"
echo ""

# Build parent project if needed
if [ ! -f "../async-job-core/target/async-job-core-1.0.0-SNAPSHOT.jar" ]; then
    echo "Building parent project..."
    cd ..
    mvn clean install -DskipTests
    cd examples
fi

echo ""
echo "========================================="
echo "Starting Application..."
echo "========================================="
echo ""
echo "The application will start on http://localhost:8080"
echo ""
echo "To test the API, open another terminal and run:"
echo "  ./test-api.sh"
echo ""
echo "To stop, press Ctrl+C"
echo "========================================="
echo ""

# Start the application
mvn spring-boot:run
