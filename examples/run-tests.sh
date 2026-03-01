#!/bin/bash

set -e

echo "========================================="
echo "Running Integration Tests"
echo "========================================="
echo ""

# Check Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

echo "✅ Docker is running"
echo ""

# Navigate to examples directory
cd "$(dirname "$0")"

echo "Running tests with Testcontainers..."
echo "This will automatically start PostgreSQL, Redis, and Kafka containers."
echo ""

# Run tests
mvn test

echo ""
echo "========================================="
echo "Tests Complete!"
echo "========================================="
