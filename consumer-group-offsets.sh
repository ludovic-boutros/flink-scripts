#!/bin/bash

# Script to retrieve consumer group committed offsets and format them for Flink statements
# Usage: ./consumer-group-offsets.sh <consumer-group-name> [options]

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
    
    # Check for Kafka API credentials (required for consumer group operations)
    if [[ -z "$kafka_api_key" || -z "$kafka_api_secret" ]]; then
        echo "Error: Missing Kafka API credentials in credentials.properties"
        echo "Required for consumer group operations: kafka_api_key, kafka_api_secret"
        echo "Note: These are cluster-scoped API keys, different from management API keys"
        exit 1
    fi
    
    # Check if kafka_rest_endpoint is provided (used as bootstrap servers)
    if [[ -z "$kafka_rest_endpoint" ]]; then
        echo "Error: kafka_rest_endpoint not found in credentials.properties"
        echo "This is required for kafka-consumer-groups CLI. Add kafka_rest_endpoint=https://pkc-XXXXX.region.provider.confluent.cloud to your credentials file."
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

# Function to URL encode a string
url_encode() {
    local string="$1"
    local encoded=""
    local char
    
    for (( i=0; i<${#string}; i++ )); do
        char="${string:$i:1}"
        case "$char" in
            [a-zA-Z0-9.~_-]) 
                encoded+="$char"
                ;;
            *) 
                encoded+="$(printf '%%%02X' "'$char")"
                ;;
        esac
    done
    echo "$encoded"
}

# Function to make Kafka REST API calls (different base URL)
kafka_api_call() {
    local method="$1"
    local endpoint="$2"
    local data="$3"
    
    # Use Kafka REST API endpoint
    local kafka_rest_url
    if [[ -n "$kafka_rest_endpoint" ]]; then
        kafka_rest_url="$kafka_rest_endpoint"
    else
        # Construct the default Kafka REST endpoint
        # Format: https://pkc-<identifier>.<region>.<provider>.confluent.cloud
        echo "Error: kafka_rest_endpoint not specified in credentials.properties"
        echo "Add kafka_rest_endpoint=https://pkc-XXXXX.region.provider.confluent.cloud to your credentials file"
        exit 1
    fi
    
    local curl_opts=(
        -s -w "%{http_code}"
        -X "$method"
        -H "Authorization: Basic $(echo -n "$kafka_api_key:$kafka_api_secret" | base64)"
        -H "Content-Type: application/json"
    )
    
    if [[ -n "$data" ]]; then
        curl_opts+=(-d "$data")
    fi
    
    curl "${curl_opts[@]}" "$kafka_rest_url$endpoint"
}

# Function to retrieve consumer group information
get_consumer_group_info() {
    local group_name="$1"
    local topic_filter="$2"
    
    if [[ -z "$group_name" ]]; then
        echo "Error: Consumer group name is required"
        echo "Usage: $0 offsets <consumer-group-name> [--topic <topic-name>]"
        exit 1
    fi
    
    if [[ -z "$cluster_id" ]]; then
        echo "Error: cluster_id is required in credentials.properties for Kafka API operations"
        exit 1
    fi
    
    echo "🔍 Retrieving consumer group information for: $group_name"
    if [[ -n "$topic_filter" ]]; then
        echo "Topic Filter: $topic_filter"
    fi
    echo "Bootstrap Servers: $kafka_rest_endpoint"
    echo ""
    
    # Get committed offsets using CLI
    get_committed_offsets "$group_name" "$topic_filter"
}

# Function to get committed offsets using kafka-consumer-groups CLI
get_committed_offsets() {
    local group_name="$1"
    local topic_filter="$2"
    
    echo "📊 Retrieving consumer group offsets using kafka-consumer-groups CLI..."
    
    # Convert REST API URL to bootstrap servers format
    local bootstrap_servers
    bootstrap_servers=$(echo "$kafka_rest_endpoint" | sed 's|https://||' | sed 's|http://||'):9092
    
    # Create a temporary properties file for kafka-consumer-groups
    local temp_config=$(mktemp)
    cat > "$temp_config" << EOF
bootstrap.servers=$bootstrap_servers
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$kafka_api_key" password="$kafka_api_secret";
EOF

    # Try to use kafka-consumer-groups command
    local kafka_consumer_groups_cmd="kafka-consumer-groups"
    
    # Check if kafka-consumer-groups is available
    if ! command -v "$kafka_consumer_groups_cmd" &> /dev/null; then
        # Try alternative command names
        if command -v "kafka-consumer-groups.sh" &> /dev/null; then
            kafka_consumer_groups_cmd="kafka-consumer-groups.sh"
        else
            echo "❌ kafka-consumer-groups command not found"
            echo ""
            echo "💡 Please install Kafka tools:"
            echo "  • macOS: brew install kafka"
            echo "  • Ubuntu/Debian: sudo apt install kafka"
            echo "  • Or download from: https://kafka.apache.org/downloads"
            echo ""
            echo "🔧 Alternative: Use Confluent CLI instead:"
            echo "  confluent kafka consumer group describe $group_name"
            rm -f "$temp_config"
            return 1
        fi
    fi
    
    echo "Using command: $kafka_consumer_groups_cmd"
    echo "Consumer Group: $group_name"
    echo ""
    
    # Get consumer group offsets
    local describe_output
    describe_output=$($kafka_consumer_groups_cmd --bootstrap-server "$bootstrap_servers" \
        --command-config "$temp_config" \
        --group "$group_name" \
        --describe 2>&1)
    
    local exit_code=$?
    
    # Clean up temp file
    rm -f "$temp_config"
    
    if [[ $exit_code -ne 0 ]]; then
        echo "❌ Failed to retrieve consumer group information"
        echo "Error output:"
        echo "$describe_output"
        echo ""
        echo "💡 Possible causes:"
        echo "  • Consumer group '$group_name' does not exist"
        echo "  • Network connectivity issues"
        echo "  • Authentication problems"
        echo "  • Consumer group has no committed offsets"
        return 1
    fi
    
    # Apply topic filter if specified
    local filtered_output="$describe_output"
    if [[ -n "$topic_filter" ]]; then
        echo "🔍 Filtering for topic: $topic_filter"
        # Keep header line and filter data lines
        local header_line=$(echo "$describe_output" | head -n1)
        local data_lines=$(echo "$describe_output" | tail -n +2 | grep "^[^ ]* *$topic_filter ")
        
        if [[ -z "$data_lines" ]]; then
            echo "⚠️  No data found for topic: $topic_filter"
            echo "Available topics in this consumer group:"
            echo "$describe_output" | tail -n +2 | awk '{print $2}' | grep -v "^TOPIC$" | sort -u | while read topic; do
                echo "  - $topic"
            done
            return 0
        fi
        
        filtered_output="$header_line"$'\n'"$data_lines"
        echo ""
    fi
    
    # Parse and display the output
    echo "📋 Consumer Group Offsets:"
    echo "$filtered_output"
    echo ""
    
    # Format for Flink statements
    format_for_flink_cli "$filtered_output" "$group_name"
}

# Function to format offsets for Flink statements
format_for_flink() {
    local offsets_json="$1"
    local group_name="$2"
    
    echo "🚀 Flink Statement Format:"
    echo "-- Consumer group: $group_name"
    echo "-- Generated on: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo ""
    
    # Group offsets by topic (using consumer-lags response format)
    local topics
    topics=$(echo "$offsets_json" | jq -r '.data[].topic_name' | sort -u)
    
    for topic in $topics; do
        echo "-- Topic: $topic"
        
        # Get all partitions and current offsets for this topic
        local topic_offsets
        topic_offsets=$(echo "$offsets_json" | jq -r --arg topic "$topic" '.data[] | select(.topic_name == $topic) | "\(.partition_id):\(.current_offset)"' | sort -n)
        
        # Build the partition offset map for Flink
        local partition_map=""
        while read -r partition_offset; do
            local partition=$(echo "$partition_offset" | cut -d: -f1)
            local offset=$(echo "$partition_offset" | cut -d: -f2)
            
            # Skip if offset is null or -1 (no committed offset)
            if [[ "$offset" == "null" || "$offset" == "-1" ]]; then
                echo "-- Partition $partition: No committed offset (skipping)"
                continue
            fi
            
            if [[ -n "$partition_map" ]]; then
                partition_map+=", "
            fi
            partition_map+="'$partition': $offset"
        done <<< "$topic_offsets"
        
        # Output Flink-compatible format
        echo "-- For Flink Kafka connector:"
        echo "'scan.startup.mode' = 'specific-offsets',"
        echo "'scan.startup.specific-offsets' = '$topic;$partition_map',"
        echo ""
    done
    
    echo "-- Example Flink table creation with offsets:"
    echo "CREATE TABLE my_kafka_table ("
    echo "  -- Add your table schema here"
    echo "  key STRING,"
    echo "  value STRING"
    echo ") WITH ("
    echo "  'connector' = 'kafka',"
    echo "  'topic' = 'your-topic-name',"
    echo "  'properties.bootstrap.servers' = 'your-bootstrap-servers',"
    echo "  'properties.group.id' = '$group_name',"
    echo "  'scan.startup.mode' = 'specific-offsets',"
    
    # Show example for first topic
    local first_topic
    first_topic=$(echo "$topics" | head -n1)
    if [[ -n "$first_topic" ]]; then
        local first_topic_offsets
        first_topic_offsets=$(echo "$offsets_json" | jq -r --arg topic "$first_topic" '.data[] | select(.topic_name == $topic) | "\(.partition_id):\(.current_offset)"' | sort -n)
        
        local first_partition_map=""
        while read -r partition_offset; do
            local partition=$(echo "$partition_offset" | cut -d: -f1)
            local offset=$(echo "$partition_offset" | cut -d: -f2)
            
            # Skip if offset is null or -1 (no committed offset)
            if [[ "$offset" == "null" || "$offset" == "-1" ]]; then
                continue
            fi
            
            if [[ -n "$first_partition_map" ]]; then
                first_partition_map+=", "
            fi
            first_partition_map+="'$partition': $offset"
        done <<< "$first_topic_offsets"
        
        echo "  'scan.startup.specific-offsets' = '$first_topic;$first_partition_map',"
    fi
    
    echo "  'format' = 'json'"
    echo ");"
    echo ""
}

# Function to format CLI output for Flink statements
format_for_flink_cli() {
    local cli_output="$1"
    local group_name="$2"
    
    echo "🚀 Flink Statement Format:"
    echo "-- Consumer group: $group_name"
    echo "-- Generated on: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo ""
    
    # Parse the kafka-consumer-groups output
    # Expected format: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
    # Skip header line and empty lines
    local data_lines
    data_lines=$(echo "$cli_output" | grep -v "^GROUP" | grep -v "^$")
    
    if [[ -z "$data_lines" ]]; then
        echo "-- No offset data found in CLI output"
        return 0
    fi
    
    # Group by topic (2nd column is TOPIC)
    local topics
    topics=$(echo "$data_lines" | awk '{print $2}' | sort -u)
    
    for topic in $topics; do
        echo "-- Topic: $topic"
        
        # Get all partitions and offsets for this topic
        local partition_map=""
        
        while IFS= read -r line; do
            local group_name_col=$(echo "$line" | awk '{print $1}')
            local topic_name=$(echo "$line" | awk '{print $2}')
            local partition=$(echo "$line" | awk '{print $3}')
            local current_offset=$(echo "$line" | awk '{print $4}')
            
            # Skip if not this topic or no offset
            if [[ "$topic_name" != "$topic" ]]; then
                continue
            fi
            
            # Skip if offset is - or empty (no committed offset)
            if [[ "$current_offset" == "-" || -z "$current_offset" ]]; then
                echo "-- Partition $partition: No committed offset (skipping)"
                continue
            fi
            
            if [[ -n "$partition_map" ]]; then
                partition_map+=";"
            fi
            partition_map+="partition:$partition,offset:$current_offset"
        done <<< "$data_lines"
        
        if [[ -n "$partition_map" ]]; then
            # Output Flink-compatible format
            echo "-- For Flink Kafka connector:"
            echo "'scan.startup.mode' = 'specific-offsets',"
            echo "'scan.startup.specific-offsets' = '$partition_map',"
        else
            echo "-- No valid offsets found for this topic"
        fi
        echo ""
    done
    
    echo "-- Confluent Cloud SQL Hint Examples:"
    echo ""
    
    # Generate SQL hint examples for each topic
    for topic in $topics; do
        local hint_partition_map=""
        
        while IFS= read -r line; do
            local group_name_col=$(echo "$line" | awk '{print $1}')
            local topic_name=$(echo "$line" | awk '{print $2}')
            local partition=$(echo "$line" | awk '{print $3}')
            local current_offset=$(echo "$line" | awk '{print $4}')
            
            if [[ "$topic_name" != "$topic" || "$current_offset" == "-" || -z "$current_offset" ]]; then
                continue
            fi
            
            if [[ -n "$hint_partition_map" ]]; then
                hint_partition_map+=";"
            fi
            hint_partition_map+="partition:$partition,offset:$current_offset"
        done <<< "$data_lines"
        
        if [[ -n "$hint_partition_map" ]]; then
            echo "-- SQL Hint for topic: $topic"
            echo "SELECT * FROM \`$topic\`"
            echo "/*+ OPTIONS("
            echo "  'scan.startup.mode'='specific-offsets',"
            echo "  'scan.startup.specific-offsets'='$hint_partition_map'"
            echo ") */;"
            echo ""
        fi
    done
}

# Function to check cluster type
check_cluster_type() {
    echo "🔍 Checking cluster type (consumer lag APIs require dedicated clusters)..."
    
    # Use management API to get cluster details
    local response
    response=$(api_call "GET" "/cmk/v2/clusters/$cluster_id")
    
    local http_code="${response: -3}"
    local body="${response%???}"
    
    if [[ "$http_code" -eq 200 ]]; then
        local cluster_type
        cluster_type=$(echo "$body" | jq -r '.spec.availability // "unknown"')
        echo "Cluster Type: $cluster_type"
        
        if [[ "$cluster_type" != "DEDICATED" ]]; then
            echo ""
            echo "⚠️  WARNING: Consumer lag APIs are only available for DEDICATED clusters"
            echo "   Your cluster type: $cluster_type"
            echo "   Consumer group offset information is not accessible via REST API for basic/standard clusters"
            echo ""
            echo "🛠️  Alternatives for non-dedicated clusters:"
            echo "   1. Use Confluent CLI: confluent kafka consumer group describe <group-name>"
            echo "   2. Use Confluent Cloud Console → Consumers tab"
            echo "   3. Use Metrics API for lag monitoring"
            return 1
        else
            echo "✅ Dedicated cluster detected - consumer lag APIs should be available"
        fi
    else
        echo "⚠️  Could not determine cluster type (HTTP $http_code)"
        echo "Proceeding anyway..."
    fi
    
    return 0
}

# Function to list all consumer groups using CLI
list_consumer_groups() {
    echo "📋 Listing all consumer groups using kafka-consumer-groups CLI..."
    
    # Convert REST API URL to bootstrap servers format
    local bootstrap_servers
    bootstrap_servers=$(echo "$kafka_rest_endpoint" | sed 's|https://||' | sed 's|http://||'):9092
    
    # Create a temporary properties file
    local temp_config=$(mktemp)
    cat > "$temp_config" << EOF
bootstrap.servers=$bootstrap_servers
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$kafka_api_key" password="$kafka_api_secret";
EOF

    # Check if kafka-consumer-groups is available
    local kafka_consumer_groups_cmd="kafka-consumer-groups"
    if ! command -v "$kafka_consumer_groups_cmd" &> /dev/null; then
        if command -v "kafka-consumer-groups.sh" &> /dev/null; then
            kafka_consumer_groups_cmd="kafka-consumer-groups.sh"
        else
            echo "❌ kafka-consumer-groups command not found"
            echo ""
            echo "💡 Please install Kafka tools:"
            echo "  • macOS: brew install kafka"
            echo "  • Ubuntu/Debian: sudo apt install kafka"
            echo "  • Or download from: https://kafka.apache.org/downloads"
            echo ""
            echo "🔧 Alternative: Use Confluent CLI:"
            echo "  confluent kafka consumer group list"
            rm -f "$temp_config"
            return 1
        fi
    fi
    
    echo "Using command: $kafka_consumer_groups_cmd"
    echo ""
    
    # List consumer groups
    local list_output
    list_output=$($kafka_consumer_groups_cmd --bootstrap-server "$bootstrap_servers" \
        --command-config "$temp_config" \
        --list 2>&1)
    
    local exit_code=$?
    
    # Clean up temp file
    rm -f "$temp_config"
    
    if [[ $exit_code -ne 0 ]]; then
        echo "❌ Failed to list consumer groups"
        echo "Error output:"
        echo "$list_output"
        return 1
    fi
    
    echo "📋 Available Consumer Groups:"
    if [[ -z "$list_output" ]]; then
        echo "  (No consumer groups found)"
    else
        echo "$list_output" | while IFS= read -r group; do
            echo "  - $group"
        done
    fi
    echo ""
    echo "💡 Use './consumer-group-offsets.sh offsets <group-name>' to get offsets for a specific group"
}

# Function to create Kafka properties file in current directory
create_kafka_config() {
    local config_file="$SCRIPT_DIR/kafka-consumer-groups.properties"
    
    echo "📝 Creating Kafka properties file: $config_file"
    
    # Convert REST API URL to bootstrap servers format
    # From: https://pkc-xxxxx.region.provider.confluent.cloud
    # To: pkc-xxxxx.region.provider.confluent.cloud:9092
    local bootstrap_servers
    bootstrap_servers=$(echo "$kafka_rest_endpoint" | sed 's|https://||' | sed 's|http://||'):9092
    
    echo "🔄 Converting REST endpoint to bootstrap servers:"
    echo "   REST API: $kafka_rest_endpoint"
    echo "   Bootstrap: $bootstrap_servers"
    echo ""
    
    cat > "$config_file" << EOF
# Kafka Consumer Groups CLI Configuration
# Generated on: $(date -u '+%Y-%m-%d %H:%M:%S UTC')
# 
# This file contains the configuration needed for kafka-consumer-groups CLI
# to connect to Confluent Cloud

# Bootstrap servers (converted from REST API endpoint)
bootstrap.servers=$bootstrap_servers

# Security configuration for Confluent Cloud
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$kafka_api_key" password="$kafka_api_secret";

# Optional: SSL configuration (usually not needed for Confluent Cloud)
# ssl.endpoint.identification.algorithm=https
# ssl.truststore.location=/path/to/truststore.jks
# ssl.truststore.password=password
EOF

    echo "✅ Kafka properties file created successfully!"
    echo ""
    echo "📋 You can now use this file with kafka-consumer-groups CLI:"
    echo ""
    echo "# List all consumer groups:"
    echo "kafka-consumer-groups --bootstrap-server $bootstrap_servers --command-config $config_file --list"
    echo ""
    echo "# Describe a specific consumer group:"
    echo "kafka-consumer-groups --bootstrap-server $bootstrap_servers --command-config $config_file --group <group-name> --describe"
    echo ""
    echo "# Reset offsets (example):"
    echo "kafka-consumer-groups --bootstrap-server $bootstrap_servers --command-config $config_file --group <group-name> --reset-offsets --to-earliest --topic <topic-name> --execute"
    echo ""
    echo "🔒 Security Note: This file contains your API credentials - keep it secure!"
    echo "💡 The file is automatically ignored by git (check your .gitignore)"
}

# Function to show usage
show_usage() {
    cat <<EOF
Usage: $0 <action> [options]

Actions:
  offsets <consumer-group-name> [--topic <topic-name>]  Get committed offsets and Flink format
  list                                                  List all consumer groups in the cluster
  create-config                                         Create kafka-consumer-groups.properties file
  help                                                  Show this usage information

Examples:
  $0 create-config                     # Create kafka-consumer-groups.properties file
  $0 offsets my-consumer-group         # Get offsets for all topics in group
  $0 offsets my-consumer-group --topic my_input     # Get offsets for specific topic only
  $0 list                              # List all consumer groups
  
Note: 
- Requires kafka-consumer-groups CLI tool (install with: brew install kafka)
- Uses Kafka API keys and kafka_rest_endpoint from credentials.properties
- Works with all Confluent Cloud cluster types (Basic, Standard, Dedicated)
- Output includes both human-readable format and Flink-specific configuration

The script generates Flink-compatible offset configuration that can be used in:
- Flink SQL CREATE TABLE statements
- Flink Kafka connector properties
- Specific offset startup mode configuration

Workflow:
1. First run: ./consumer-group-offsets.sh create-config
2. Then run: ./consumer-group-offsets.sh list (to see available groups)
3. Finally: ./consumer-group-offsets.sh offsets <group-name>
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
        offsets)
            # Parse offsets command with optional --topic filter
            local group_name="$2"
            local topic_filter=""
            
            # Parse additional arguments for --topic
            shift 2  # Remove 'offsets' and group name
            while [[ $# -gt 0 ]]; do
                case $1 in
                    --topic)
                        topic_filter="$2"
                        shift 2
                        ;;
                    *)
                        echo "Error: Unknown option '$1'"
                        echo "Usage: $0 offsets <group-name> [--topic <topic-name>]"
                        exit 1
                        ;;
                esac
            done
            
            get_consumer_group_info "$group_name" "$topic_filter"
            ;;
        list)
            list_consumer_groups
            ;;
        help|--help|-h)
            show_usage
            ;;
        create-config)
            create_kafka_config
            ;;
        *)
            echo "Error: Unknown action '$1'"
            show_usage
            exit 1
            ;;
    esac
}

main "$@"