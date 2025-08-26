#!/bin/bash

# Unified script for deploying and managing Flink SQL statements in Confluent Cloud
# Usage: ./flink-statement.sh <action> [options]

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
    
    # Source the properties file
    source "$CREDENTIALS_FILE"
    
    # Validate required credentials for management operations
    if [[ -z "$management_api_key" || -z "$management_api_secret" || -z "$environment_id" || -z "$organization_id" ]]; then
        echo "Error: Missing required credentials in credentials.properties"
        echo "Required: management_api_key, management_api_secret, environment_id, organization_id"
        exit 1
    fi
}

# Function to validate deployment credentials
validate_deployment_credentials() {
    if [[ -z "$compute_pool_id" || -z "$execution_service_account_id" ]]; then
        echo "Error: Missing required deployment credentials in credentials.properties"
        echo "Required for deployment: compute_pool_id, execution_service_account_id"
        exit 1
    fi
}

# Function to make API calls
api_call() {
    local method="$1"
    local endpoint="$2"
    local data="$3"
    
    local curl_opts=(
        -s -w "%{http_code}"
        -X "$method"
        -H "Authorization: Basic $(echo -n "$management_api_key:$management_api_secret" | base64)"
        -H "Content-Type: application/json"
    )
    
    if [[ -n "$data" ]]; then
        curl_opts+=(-d "$data")
    fi
    
    curl "${curl_opts[@]}" "$base_url$endpoint"
}

# Function to deploy Flink SQL statement
deploy_statement() {
    local sql_file="$1"
    local statement_name="$2"
    
    validate_deployment_credentials
    
    if [[ ! -f "$sql_file" ]]; then
        echo "Error: SQL file '$sql_file' not found"
        exit 1
    fi
    
    local sql_content
    sql_content=$(cat "$sql_file")
    
    # Default statement name if not provided
    if [[ -z "$statement_name" ]]; then
        statement_name="flink-statement-$(date +%s)"
    fi
    
    echo "🚀 Deploying Flink SQL statement: $statement_name"
    echo "Environment ID: $environment_id"
    echo "Compute Pool ID: $compute_pool_id"
    echo "Execution Service Account: $execution_service_account_id"
    
    # Create the JSON payload
    local json_payload
    json_payload=$(cat <<EOF
{
    "name": "$statement_name",
    "spec": {
        "statement": $(echo "$sql_content" | jq -Rs .),
        "compute_pool_id": "$compute_pool_id",
        "principal": "$execution_service_account_id"
    }
}
EOF
)
    
    # Make the API call
    local response
    response=$(api_call "POST" "/sql/v1/organizations/$organization_id/environments/$environment_id/statements" "$json_payload")
    
    local http_code="${response: -3}"
    local body="${response%???}"
    
    if [[ "$http_code" -eq 201 || "$http_code" -eq 200 ]]; then
        echo "✅ Successfully deployed Flink SQL statement"
        echo "$body" | jq .
    else
        echo "❌ Failed to deploy Flink SQL statement (HTTP $http_code)"
        echo "$body" | jq . 2>/dev/null || echo "$body"
        exit 1
    fi
}

# Function to list statements with optional filters
list_statements() {
    local filter_principal=""
    local filter_status=""
    
    # Parse filter options
    while [[ $# -gt 0 ]]; do
        case $1 in
            --principal)
                filter_principal="$2"
                shift 2
                ;;
            --status)
                filter_status="$2"
                shift 2
                ;;
            *)
                echo "Unknown filter option: $1"
                exit 1
                ;;
        esac
    done
    
    local filter_desc=""
    if [[ -n "$filter_principal" ]]; then
        filter_desc+=" (Principal: $filter_principal)"
    fi
    if [[ -n "$filter_status" ]]; then
        filter_desc+=" (Status: $filter_status)"
    fi
    
    echo "📋 Listing Flink SQL statements in environment: $environment_id$filter_desc"
    
    local response
    response=$(api_call "GET" "/sql/v1/organizations/$organization_id/environments/$environment_id/statements")
    
    local http_code="${response: -3}"
    local body="${response%???}"
    
    if [[ "$http_code" -eq 200 ]]; then
        # Build jq filter based on options
        local jq_filter='.data[]'
        
        if [[ -n "$filter_principal" ]]; then
            jq_filter+=" | select(.spec.principal == \"$filter_principal\")"
        fi
        
        if [[ -n "$filter_status" ]]; then
            jq_filter+=" | select(.status.phase == \"$filter_status\")"
        fi
        
        jq_filter+=' | "Name: \(.name) | Status: \(.status.phase) | Principal: \(.spec.principal) | Created: \(.metadata.created_at)"'
        
        local filtered_output
        filtered_output=$(echo "$body" | jq -r "$jq_filter")
        
        if [[ -z "$filtered_output" ]]; then
            echo "No statements found matching the specified filters."
        else
            echo "$filtered_output"
        fi
    else
        echo "❌ Failed to list statements (HTTP $http_code)"
        echo "$body" | jq . 2>/dev/null || echo "$body"
        exit 1
    fi
}

# Function to get a specific statement
get_statement() {
    local statement_id="$1"
    
    if [[ -z "$statement_id" ]]; then
        echo "Error: Statement name required"
        echo "Usage: $0 get <statement-name>"
        exit 1
    fi
    
    echo "🔍 Getting statement details for: $statement_id"
    
    local response
    response=$(api_call "GET" "/sql/v1/organizations/$organization_id/environments/$environment_id/statements/$statement_id")
    
    local http_code="${response: -3}"
    local body="${response%???}"
    
    if [[ "$http_code" -eq 200 ]]; then
        echo "$body" | jq .
    else
        echo "❌ Failed to get statement (HTTP $http_code)"
        echo "$body" | jq . 2>/dev/null || echo "$body"
        exit 1
    fi
}

# Function to delete a statement
delete_statement() {
    local statement_id="$1"
    
    if [[ -z "$statement_id" ]]; then
        echo "Error: Statement name required"
        echo "Usage: $0 delete <statement-name>"
        exit 1
    fi
    
    echo "🗑️  Deleting statement: $statement_id"
    
    local response
    response=$(api_call "DELETE" "/sql/v1/organizations/$organization_id/environments/$environment_id/statements/$statement_id")
    
    local http_code="${response: -3}"
    local body="${response%???}"
    
    if [[ "$http_code" -eq 204 || "$http_code" -eq 202 ]]; then
        if [[ "$http_code" -eq 202 ]]; then
            echo "✅ Statement deletion accepted and is being processed: $statement_id"
        else
            echo "✅ Successfully deleted statement: $statement_id"
        fi
        # Show response body if there's useful information
        if [[ -n "$body" && "$body" != "null" && "$body" != "" ]]; then
            echo "$body" | jq . 2>/dev/null || echo "$body"
        fi
    else
        echo "❌ Failed to delete statement (HTTP $http_code)"
        echo "$body" | jq . 2>/dev/null || echo "$body"
        exit 1
    fi
}

# Function to stop a statement
stop_statement() {
    local statement_id="$1"
    
    if [[ -z "$statement_id" ]]; then
        echo "Error: Statement name required"
        echo "Usage: $0 stop <statement-name>"
        exit 1
    fi
    
    echo "⏹️  Stopping statement: $statement_id"
    
    local json_payload='[{"op": "replace", "path": "/spec/stopped", "value": true}]'
    
    local response
    response=$(api_call "PATCH" "/sql/v1/organizations/$organization_id/environments/$environment_id/statements/$statement_id" "$json_payload")
    
    local http_code="${response: -3}"
    local body="${response%???}"
    
    if [[ "$http_code" -eq 200 || "$http_code" -eq 202 ]]; then
        if [[ "$http_code" -eq 202 ]]; then
            echo "✅ Statement stop request accepted and is being processed: $statement_id"
        else
            echo "✅ Successfully stopped statement: $statement_id"
        fi
        echo "$body" | jq .
    else
        echo "❌ Failed to stop statement (HTTP $http_code)"
        echo "$body" | jq . 2>/dev/null || echo "$body"
        exit 1
    fi
}

# Function to clean up non-running statements
clean_statements() {
    local force_mode=false
    local filter_principal=""
    local filter_status=""
    
    # Parse options
    while [[ $# -gt 0 ]]; do
        case $1 in
            --force)
                force_mode=true
                shift
                ;;
            --principal)
                filter_principal="$2"
                shift 2
                ;;
            --status)
                filter_status="$2"
                shift 2
                ;;
            *)
                echo "Unknown option: $1"
                exit 1
                ;;
        esac
    done
    
    local filter_desc=""
    if [[ -n "$filter_principal" ]]; then
        filter_desc+=" (Principal: $filter_principal)"
    fi
    if [[ -n "$filter_status" ]]; then
        filter_desc+=" (Status: $filter_status)"
    else
        filter_desc+=" (Status: COMPLETED, FAILED, STOPPED)"
    fi
    
    echo "🧹 Cleaning up statements in environment: $environment_id$filter_desc"
    echo ""
    
    # Get all statements
    local response
    response=$(api_call "GET" "/sql/v1/organizations/$organization_id/environments/$environment_id/statements")
    
    local http_code="${response: -3}"
    local body="${response%???}"
    
    if [[ "$http_code" -ne 200 ]]; then
        echo "❌ Failed to list statements (HTTP $http_code)"
        echo "$body" | jq . 2>/dev/null || echo "$body"
        exit 1
    fi
    
    # Build jq filter for statements
    local jq_filter='.data[]'
    
    # Apply status filter (default to non-running if no specific status given)
    if [[ -n "$filter_status" ]]; then
        jq_filter+=" | select(.status.phase == \"$filter_status\")"
    else
        jq_filter+=' | select(.status.phase != "RUNNING")'
    fi
    
    # Apply principal filter if specified
    if [[ -n "$filter_principal" ]]; then
        jq_filter+=" | select(.spec.principal == \"$filter_principal\")"
    fi
    
    jq_filter+=' | "\(.name)|\(.status.phase)|\(.spec.principal)"'
    
    # Extract filtered statements
    local filtered_statements
    filtered_statements=$(echo "$body" | jq -r "$jq_filter")
    
    if [[ -z "$filtered_statements" ]]; then
        echo "✅ No statements found matching the specified criteria. Nothing to clean up."
        return 0
    fi
    
    # If no principal filter specified, separate own statements from others
    local own_statements=""
    local other_statements=""
    local execution_principal="$execution_service_account_id"
    
    if [[ -z "$filter_principal" ]]; then
        # Separate by ownership when no principal filter is applied
        while IFS='|' read -r name phase principal; do
            if [[ "$principal" == "$execution_principal" ]]; then
                own_statements+="$name|$phase|$principal"$'\n'
            else
                other_statements+="$name|$phase|$principal"$'\n'
            fi
        done <<< "$filtered_statements"
    else
        # When principal filter is applied, all statements are "own" for cleanup purposes
        own_statements="$filtered_statements"
    fi
    
    # Remove trailing newlines
    own_statements=$(echo "$own_statements" | sed '/^$/d')
    other_statements=$(echo "$other_statements" | sed '/^$/d')
    
    if [[ -n "$other_statements" ]]; then
        echo "⚠️  Found statements created by other users/service accounts:"
        echo "$other_statements" | while IFS='|' read -r name phase principal; do
            echo "  - $name (Status: $phase, Principal: $principal) - CANNOT DELETE"
        done
        echo ""
    fi
    
    if [[ -z "$own_statements" ]]; then
        echo "❌ No statements found that can be deleted by this service account ($execution_principal)."
        echo "   Only statements created by the same service account can be deleted."
        return 0
    fi
    
    echo "✅ Found statements that can be deleted (created by $execution_principal):"
    echo "$own_statements" | while IFS='|' read -r name phase principal; do
        echo "  - $name (Status: $phase)"
    done
    echo ""
    
    # Confirm before proceeding (unless force mode)
    if [[ "$force_mode" == "true" ]]; then
        echo "⚠️  Force mode enabled - proceeding without confirmation..."
    else
        read -p "Do you want to delete these statements? [y/N]: " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "❌ Clean operation cancelled."
            return 0
        fi
    fi
    
    # Delete each own non-running statement
    local deleted_count=0
    local failed_count=0
    
    while IFS='|' read -r name phase principal; do
        echo "🗑️  Deleting $name (Status: $phase)..."
        
        local delete_response
        delete_response=$(api_call "DELETE" "/sql/v1/organizations/$organization_id/environments/$environment_id/statements/$name")
        
        local delete_http_code="${delete_response: -3}"
        local delete_body="${delete_response%???}"
        
        if [[ "$delete_http_code" -eq 204 || "$delete_http_code" -eq 202 ]]; then
            echo "  ✅ Successfully deleted: $name"
            ((deleted_count++))
        else
            echo "  ❌ Failed to delete $name (HTTP $delete_http_code)"
            echo "     $delete_body" | jq . 2>/dev/null || echo "     $delete_body"
            ((failed_count++))
        fi
    done <<< "$own_statements"
    
    echo ""
    echo "🧹 Clean operation completed:"
    echo "  ✅ Deletion requests sent: $deleted_count statements"
    if [[ $failed_count -gt 0 ]]; then
        echo "  ❌ Failed: $failed_count statements"
    fi
    
    if [[ $deleted_count -gt 0 ]]; then
        echo ""
        echo "⏳ Note: Deletions are processed asynchronously. Checking current status..."
        sleep 2  # Give API time to process
        
        # Re-check what statements still exist
        local verify_response
        verify_response=$(api_call "GET" "/sql/v1/organizations/$organization_id/environments/$environment_id/statements")
        local verify_http_code="${verify_response: -3}"
        local verify_body="${verify_response%???}"
        
        if [[ "$verify_http_code" -eq 200 ]]; then
            local still_existing
            still_existing=$(echo "$verify_body" | jq -r '.data[] | select(.status.phase != "RUNNING") | "\(.name)|\(.status.phase)"')
            
            if [[ -z "$still_existing" ]]; then
                echo "✅ All non-running statements have been processed for deletion."
            else
                echo "📋 Statements still visible (may be processing deletion):"
                echo "$still_existing" | while IFS='|' read -r name phase; do
                    echo "  - $name (Status: $phase)"
                done
                echo ""
                echo "💡 Tip: Some statements may take a few moments to disappear from the list."
                echo "    Run './flink-statement.sh list' again in a few seconds to verify."
            fi
        fi
    fi
}

# Function to get statement offsets
get_offsets() {
    local statement_id=""
    local filter_principal=""
    local filter_status=""
    
    # Parse arguments - first check if first arg is an option or statement name
    while [[ $# -gt 0 ]]; do
        case $1 in
            --principal)
                filter_principal="$2"
                shift 2
                ;;
            --status)
                filter_status="$2"
                shift 2
                ;;
            *)
                # If it doesn't start with --, it's a statement name
                if [[ ! "$1" =~ ^-- ]]; then
                    statement_id="$1"
                    shift
                else
                    echo "Unknown option: $1"
                    exit 1
                fi
                ;;
        esac
    done
    
    # If statement ID provided, show offsets for that specific statement
    if [[ -n "$statement_id" ]]; then
        # Get offsets for specific statement
        echo "📊 Retrieving offsets for statement: $statement_id"
        
        local response
        response=$(api_call "GET" "/sql/v1/organizations/$organization_id/environments/$environment_id/statements/$statement_id")
        
        local http_code="${response: -3}"
        local body="${response%???}"
        
        if [[ "$http_code" -eq 200 ]]; then
            local latest_offsets
            latest_offsets=$(echo "$body" | jq -r '.status.latest_offsets // empty')
            
            local latest_timestamp
            latest_timestamp=$(echo "$body" | jq -r '.status.latest_offsets_timestamp // "N/A"')
            
            local status
            status=$(echo "$body" | jq -r '.status.phase')
            
            local principal
            principal=$(echo "$body" | jq -r '.spec.principal')
            
            echo "Statement: $statement_id"
            echo "Status: $status"
            echo "Principal: $principal"
            echo "Timestamp: $latest_timestamp"
            echo ""
            
            if [[ -n "$latest_offsets" && "$latest_offsets" != "null" ]]; then
                echo "📋 Latest Offsets by Topic:"
                echo "$body" | jq -r '.status.latest_offsets | to_entries[] | "  \(.key): \(.value)"'
            else
                echo "⚠️  No offset information available for this statement."
                echo "   Offsets are typically available for STOPPED statements or statements that have processed data."
            fi
        else
            echo "❌ Failed to get statement (HTTP $http_code)"
            echo "$body" | jq . 2>/dev/null || echo "$body"
            exit 1
        fi
    else
        # Show offsets for all statements (with optional filters)
        echo "📊 Retrieving offsets for statements in environment: $environment_id"
        
        local response
        response=$(api_call "GET" "/sql/v1/organizations/$organization_id/environments/$environment_id/statements")
        
        local http_code="${response: -3}"
        local body="${response%???}"
        
        if [[ "$http_code" -ne 200 ]]; then
            echo "❌ Failed to list statements (HTTP $http_code)"
            echo "$body" | jq . 2>/dev/null || echo "$body"
            exit 1
        fi
        
        # Build jq filter for statements
        local jq_filter='.data[]'
        
        if [[ -n "$filter_principal" ]]; then
            jq_filter+=" | select(.spec.principal == \"$filter_principal\")"
        fi
        
        if [[ -n "$filter_status" ]]; then
            jq_filter+=" | select(.status.phase == \"$filter_status\")"
        fi
        
        # Extract statements with offsets
        local statements_with_offsets
        statements_with_offsets=$(echo "$body" | jq -r "$jq_filter | select(.status.latest_offsets != null) | \"\(.name)|\(.status.phase)|\(.spec.principal)\"")
        
        if [[ -z "$statements_with_offsets" ]]; then
            echo "No statements found with offset information matching the specified filters."
            return 0
        fi
        
        echo ""
        while IFS='|' read -r name phase principal; do
            echo "📍 Statement: $name"
            echo "   Status: $phase | Principal: $principal"
            
            # Get detailed offsets for this statement
            local statement_data
            statement_data=$(echo "$body" | jq ".data[] | select(.name == \"$name\")")
            
            local latest_offsets
            latest_offsets=$(echo "$statement_data" | jq -r '.status.latest_offsets // empty')
            
            local latest_timestamp
            latest_timestamp=$(echo "$statement_data" | jq -r '.status.latest_offsets_timestamp // "N/A"')
            
            if [[ -n "$latest_offsets" && "$latest_offsets" != "null" ]]; then
                echo "   Timestamp: $latest_timestamp"
                echo "   Offsets:"
                echo "$statement_data" | jq -r '.status.latest_offsets | to_entries[] | "     \(.key): \(.value)"'
            else
                echo "   Offsets: No offset information available"
            fi
            echo ""
        done <<< "$statements_with_offsets"
    fi
}

# Function to verify cleanup status
verify_clean() {
    echo "🔍 Checking for non-running statements in environment: $environment_id"
    
    local response
    response=$(api_call "GET" "/sql/v1/organizations/$organization_id/environments/$environment_id/statements")
    
    local http_code="${response: -3}"
    local body="${response%???}"
    
    if [[ "$http_code" -ne 200 ]]; then
        echo "❌ Failed to list statements (HTTP $http_code)"
        echo "$body" | jq . 2>/dev/null || echo "$body"
        exit 1
    fi
    
    local non_running_statements
    non_running_statements=$(echo "$body" | jq -r '.data[] | select(.status.phase != "RUNNING") | "\(.name)|\(.status.phase)|\(.spec.principal)"')
    
    if [[ -z "$non_running_statements" ]]; then
        echo "✅ No non-running statements found - environment is clean!"
        return 0
    fi
    
    # Separate own statements from others
    local own_statements=""
    local other_statements=""
    local execution_principal="$execution_service_account_id"
    
    while IFS='|' read -r name phase principal; do
        if [[ "$principal" == "$execution_principal" ]]; then
            own_statements+="$name|$phase|$principal"$'\n'
        else
            other_statements+="$name|$phase|$principal"$'\n'
        fi
    done <<< "$non_running_statements"
    
    # Remove trailing newlines
    own_statements=$(echo "$own_statements" | sed '/^$/d')
    other_statements=$(echo "$other_statements" | sed '/^$/d')
    
    local total_count=0
    local own_count=0
    local other_count=0
    
    if [[ -n "$own_statements" ]]; then
        own_count=$(echo "$own_statements" | wc -l)
        ((total_count += own_count))
    fi
    
    if [[ -n "$other_statements" ]]; then
        other_count=$(echo "$other_statements" | wc -l)
        ((total_count += other_count))
    fi
    
    echo "📋 Found $total_count non-running statement(s):"
    echo ""
    
    if [[ -n "$own_statements" ]]; then
        echo "✅ Statements you can delete ($own_count, created by $execution_principal):"
        echo "$own_statements" | while IFS='|' read -r name phase principal; do
            echo "  - $name (Status: $phase)"
        done
        echo ""
    fi
    
    if [[ -n "$other_statements" ]]; then
        echo "⚠️  Statements you CANNOT delete ($other_count, created by other principals):"
        echo "$other_statements" | while IFS='|' read -r name phase principal; do
            echo "  - $name (Status: $phase, Principal: $principal)"
        done
        echo ""
    fi
    
    if [[ $own_count -gt 0 ]]; then
        echo "💡 Run './flink-statement.sh clean' to remove your $own_count statement(s)."
    else
        echo "💡 No statements can be cleaned by this service account."
        echo "   Only FlinkAdmin role can delete statements created by other principals."
    fi
}

# Function to show usage
show_usage() {
    cat <<EOF
Usage: $0 <action> [options]

Actions:
  deploy <sql-file> [name]             Deploy a new Flink SQL statement
  list [--principal <id>] [--status <status>]  List statements with optional filters
  get <statement-name>                 Get details of a specific statement
  delete <statement-name>              Delete a statement
  stop <statement-name>                Stop a running statement
  clean [options]                      Delete statements (with filters and options)
  verify-clean                         Check if any non-running statements exist
  offsets [statement-name] [--principal <id>] [--status <status>]  Get latest offsets

Clean Options:
  --force                              Skip confirmation prompt
  --principal <service-account-id>     Filter by statement creator
  --status <phase>                     Filter by statement status

Common Status Values:
  RUNNING, COMPLETED, FAILED, STOPPED, PROVISIONING

Examples:
  $0 deploy sample-query.sql my-analytics-query
  $0 list                                    # List all statements
  $0 list --status RUNNING                   # List only running statements
  $0 list --principal sa-abc123              # List statements by specific principal
  $0 list --principal u-xyz789 --status FAILED  # Combine filters
  $0 get workspace-2025-08-08-092755-01a9f250-5039-4426-8b97-8f42869a9892
  $0 clean                                   # Interactive cleanup of non-running statements
  $0 clean --force                           # Non-interactive cleanup
  $0 clean --principal sa-abc123             # Clean only statements by specific principal
  $0 clean --status COMPLETED --force        # Clean only completed statements
  $0 verify-clean                            # Check cleanup status
  $0 offsets                                 # Show offsets for all statements with offset data
  $0 offsets statement-name                  # Show offsets for specific statement
  $0 offsets --status STOPPED               # Show offsets for all stopped statements

Note: Statements are immutable. To modify a statement, stop/delete it and deploy a new one.
EOF
}

# Main script
main() {
    if [[ $# -lt 1 ]]; then
        show_usage
        exit 1
    fi
    
    load_credentials
    
    case "$1" in
        deploy)
            deploy_statement "$2" "$3"
            ;;
        list)
            shift  # Remove 'list' from arguments
            list_statements "$@"
            ;;
        get)
            get_statement "$2"
            ;;
        delete)
            delete_statement "$2"
            ;;
        stop)
            stop_statement "$2"
            ;;
        clean)
            shift  # Remove 'clean' from arguments
            clean_statements "$@"
            ;;
        verify-clean)
            verify_clean
            ;;
        offsets)
            shift  # Remove 'offsets' from arguments
            get_offsets "$@"
            ;;
        *)
            echo "Error: Unknown action '$1'"
            show_usage
            exit 1
            ;;
    esac
}

main "$@"