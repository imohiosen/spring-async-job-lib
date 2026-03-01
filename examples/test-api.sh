#!/bin/bash

# Example API requests for the Async Job Example Application
# Base URL
BASE_URL="http://localhost:8080"

echo "========================================="
echo "Async Job Example - API Test Scripts"
echo "========================================="
echo ""

# 1. Health Check
echo "1. Health Check"
curl -s "${BASE_URL}/api/jobs/health"
echo -e "\n"

# 2. Submit Email Notification Job
echo "2. Submit Email Notification Job"
EMAIL_JOB=$(curl -s -X POST "${BASE_URL}/api/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "jobName": "send-welcome-emails",
    "correlationId": "test-campaign-001",
    "deadlineHours": 2,
    "tasks": [
      {
        "taskType": "EMAIL_NOTIFICATION",
        "payload": "{\"recipient\":\"alice@example.com\",\"subject\":\"Welcome to Our Service!\",\"body\":\"Thank you for signing up.\"}"
      },
      {
        "taskType": "EMAIL_NOTIFICATION",
        "payload": "{\"recipient\":\"bob@example.com\",\"subject\":\"Welcome!\",\"body\":\"We are glad to have you.\"}"
      },
      {
        "taskType": "EMAIL_NOTIFICATION",
        "payload": "{\"recipient\":\"charlie@example.com\",\"subject\":\"Hello!\",\"body\":\"Thanks for joining us.\"}"
      }
    ]
  }')
echo "$EMAIL_JOB" | jq '.'
EMAIL_JOB_ID=$(echo "$EMAIL_JOB" | jq -r '.id')
echo -e "\nEmail Job ID: $EMAIL_JOB_ID\n"

# 3. Submit Report Generation Job
echo "3. Submit Report Generation Job"
REPORT_JOB=$(curl -s -X POST "${BASE_URL}/api/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "jobName": "monthly-sales-report",
    "correlationId": "report-2026-03",
    "deadlineHours": 4,
    "tasks": [
      {
        "taskType": "REPORT_GENERATION",
        "payload": "{\"reportType\":\"SALES\",\"startDate\":\"2026-03-01\",\"endDate\":\"2026-03-31\"}"
      },
      {
        "taskType": "REPORT_GENERATION",
        "payload": "{\"reportType\":\"INVENTORY\",\"startDate\":\"2026-03-01\",\"endDate\":\"2026-03-31\"}"
      }
    ]
  }')
echo "$REPORT_JOB" | jq '.'
REPORT_JOB_ID=$(echo "$REPORT_JOB" | jq -r '.id')
echo -e "\nReport Job ID: $REPORT_JOB_ID\n"

# 4. Submit Data Export Job
echo "4. Submit Data Export Job"
EXPORT_JOB=$(curl -s -X POST "${BASE_URL}/api/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "jobName": "customer-data-export",
    "correlationId": "export-202603",
    "deadlineHours": 6,
    "tasks": [
      {
        "taskType": "DATA_EXPORT",
        "payload": "{\"entity\":\"customers\",\"format\":\"CSV\",\"recordCount\":10000}"
      },
      {
        "taskType": "DATA_EXPORT",
        "payload": "{\"entity\":\"orders\",\"format\":\"JSON\",\"recordCount\":25000}"
      }
    ]
  }')
echo "$EXPORT_JOB" | jq '.'
EXPORT_JOB_ID=$(echo "$EXPORT_JOB" | jq -r '.id')
echo -e "\nExport Job ID: $EXPORT_JOB_ID\n"

# 5. Mixed Job Type
echo "5. Submit Mixed Task Type Job"
MIXED_JOB=$(curl -s -X POST "${BASE_URL}/api/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "jobName": "end-of-month-processing",
    "correlationId": "eom-2026-03",
    "deadlineHours": 8,
    "tasks": [
      {
        "taskType": "REPORT_GENERATION",
        "payload": "{\"reportType\":\"MONTHLY_SUMMARY\",\"startDate\":\"2026-03-01\",\"endDate\":\"2026-03-31\"}"
      },
      {
        "taskType": "DATA_EXPORT",
        "payload": "{\"entity\":\"transactions\",\"format\":\"CSV\",\"recordCount\":50000}"
      },
      {
        "taskType": "EMAIL_NOTIFICATION",
        "payload": "{\"recipient\":\"finance@example.com\",\"subject\":\"Monthly Report Ready\",\"body\":\"Your monthly report is ready for review.\"}"
      }
    ]
  }')
echo "$MIXED_JOB" | jq '.'
MIXED_JOB_ID=$(echo "$MIXED_JOB" | jq -r '.id')
echo -e "\nMixed Job ID: $MIXED_JOB_ID\n"

# Wait for processing
echo "========================================="
echo "Waiting 5 seconds for processing..."
echo "========================================="
sleep 5

# 6. Query Email Job Status
echo -e "\n6. Query Email Job Status"
if [ ! -z "$EMAIL_JOB_ID" ]; then
  curl -s "${BASE_URL}/api/jobs/${EMAIL_JOB_ID}" | jq '.'
  echo -e "\n"
fi

# 7. Query Email Job Tasks
echo "7. Query Email Job Tasks"
if [ ! -z "$EMAIL_JOB_ID" ]; then
  curl -s "${BASE_URL}/api/jobs/${EMAIL_JOB_ID}/tasks" | jq '.'
  echo -e "\n"
fi

# 8. Query Report Job Status
echo "8. Query Report Job Status"
if [ ! -z "$REPORT_JOB_ID" ]; then
  curl -s "${BASE_URL}/api/jobs/${REPORT_JOB_ID}" | jq '.'
  echo -e "\n"
fi

# 9. Query Export Job Tasks
echo "9. Query Export Job Tasks"
if [ ! -z "$EXPORT_JOB_ID" ]; then
  curl -s "${BASE_URL}/api/jobs/${EXPORT_JOB_ID}/tasks" | jq '.'
  echo -e "\n"
fi

echo "========================================="
echo "Test Complete!"
echo "========================================="
echo ""
echo "Job IDs created:"
echo "  Email Job: $EMAIL_JOB_ID"
echo "  Report Job: $REPORT_JOB_ID"
echo "  Export Job: $EXPORT_JOB_ID"
echo "  Mixed Job: $MIXED_JOB_ID"
echo ""
echo "You can query these jobs anytime using:"
echo "  curl ${BASE_URL}/api/jobs/{JOB_ID}"
echo "  curl ${BASE_URL}/api/jobs/{JOB_ID}/tasks"
