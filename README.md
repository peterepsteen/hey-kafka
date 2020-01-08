# hey-kafka
> A command line tool used to easily send binary encoded avro messages to Kafka

hey-kafka is a little cli to help test services that respond to avro messages from Kafka. It uses sane defaults to connect to Kafka and optionally a Schema Registry. The goal of this tool is to provide an easy way to test services that utilize an avro encoded Kafka topic to process data.

## Usage

hey-kafka only requires a schema, message, and kafka topic to send to. These can
be provided either through flags, or a config file.
    
    Usage:
    hey-kafka [flags]

    Flags:
    -f, --file string                      
                                            Optional path to a config file to use. Command line options take precedence
                                            
    -h, --help                             help for hey-kafka
    -H, --host string                      
                                            The host that kafka is listening on
                                                (default "localhost")
    -m, --message string                   
                                            Either the message to send in string form, or a filepath containing the message you wish to send
                                            
    -P, --port int                         
                                            The port that kafka is listening on
                                                (default 9092)
    -s, --schema string                    
                                            A filepath to the avro schema you wish to use
                                            
        --schema-registry-address string   
                                            The address of the Schema Registry to use. IE localhost:8081
                                            
    -t, --topic string                     
                                            The topic to send the message to. This is also used for the subject of the schema registry, if configured.
- Flags

    ```bash
    SCHEMA="$(cat <<'EOF'
    {
        "type": "record",
        "name": "UserLogin",
        "fields": [
            {"name": "userId", "type": "string"},
            {"name": "timestamp", "type": "int"},
        ]
    }
    EOF
    )"

    MESSAGE="$(cat <<'EOF'
    {
        "userId": "c2ZkYXNkZmFzZGZhc2ZkYXNmZAo",
        "timestamp": "1578519897"
    }
    EOF
    )"

    hey-kafka -s $SCHEMA -m $MESSAGE -t my-cool-topic
    ```

 - File (all yaml keys are named the same as their flags)
   ```bash
    cat << EOF > /tmp/myconfig.yaml
    schema: |
        {
            "type": "record",
            "name": "UserLogin",
            "fields": [
                {"name": "userId", "type": "string"},
                {"name": "timestamp", "type": "int"},
            ]
        }

    message: |
         {
            "userId": "c2ZkYXNkZmFzZGZhc2ZkYXNmZAo",
            "timestamp": "1578519897"
        }

    topic: my-cool-topic

    EOF

    hey-kafka -f /tmp/myconfig.yaml

   ```