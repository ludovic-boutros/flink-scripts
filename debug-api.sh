#!/bin/bash

# Debug script to test Confluent Cloud API endpoints
# Usage: ./debug-api.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CREDENTIALS_FILE="$SCRIPT_DIR/credentials.properties"

# Function to load credentials
load_credentials() {
    if [[ ! -f "$CREDENTIALS_FILE" ]]; then
        echo "Error: credentials.properties not found at $CREDENTIALS_FILE"
        if [[ -f "$SCRIPT_DIR/credentials.properties.template" ]]; then
            echo "Found credentials.properties.template - please copy it to credentials.properties and fill in your actual values:"
            echo "  cp credentials.properties.template credentials.properties"
            echo "  # Then edit credentials.properties with your actual credentials"
        fi
        exit 1
    fi
    
    source "$CREDENTIALS_FILE"
    
    if [[ -z "$management_api_key" || -z "$management_api_secret" || -z "$environment_id" || -z "$organization_id" ]]; then
        echo "Error: Missing required credentials in credentials.properties"
        exit 1
    fi
}

# Function to test API endpoint
test_endpoint() {
    local endpoint="$1"
    local description="$2"
    
    echo "Testing: $description"
    echo "Endpoint: $base_url$endpoint"
    echo "Environment ID: $environment_id"
    echo ""
    
    local response
    response=$(curl -s -w "HTTPSTATUS:%{http_code}\nRESPONSETIME:%{time_total}" \
        -X GET \
        -H "Authorization: Basic $(echo -n "$management_api_key:$management_api_secret" | base64)" \
        -H "Content-Type: application/json" \
        "$base_url$endpoint")
    
    local http_code=$(echo "$response" | tr -d '\n' | sed -e 's/.*HTTPSTATUS://' | sed -e 's/RESPONSETIME.*//')
    local response_time=$(echo "$response" | tr -d '\n' | sed -e 's/.*RESPONSETIME://')
    local body=$(echo "$response" | sed -E 's/HTTPSTATUS:[0-9]+RESPONSETIME:[0-9.]+$//')
    
    echo "HTTP Status: $http_code"
    echo "Response Time: ${response_time}s"
    echo "Response Body:"
    echo "$body" | jq . 2>/dev/null || echo "$body"
    echo ""
    echo "----------------------------------------"
    echo ""
}

# Main debug function
main() {
    load_credentials
    
    echo "🔍 Confluent Cloud API Debug"
    echo "=========================================="
    echo "Base URL: $base_url"
    echo "Management API Key: ${management_api_key:0:8}..."
    echo "Organization ID: $organization_id"
    echo "Environment ID: $environment_id"
    echo "=========================================="
    echo ""
    
    # Test Flink API endpoints with correct structure
    test_endpoint "/sql/v1/organizations/$organization_id/environments/$environment_id/statements" "List Flink SQL statements (corrected endpoint)"
    
    # Test compute pools
    if [[ -n "$compute_pool_id" ]]; then
        test_endpoint "/fcpm/v2/compute-pools/$compute_pool_id" "Get compute pool details"
    fi
}

main "$@"