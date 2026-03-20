import yaml
import argparse
from confluent_kafka.admin import AdminClient, NewTopic

def sync_kafka_topics(descriptor_path, bootstrap_servers):
    # 1. Load the YAML descriptor
    with open(descriptor_path, 'r') as file:
        data = yaml.safe_load(file)

    # 2. Initialize the Admin Client
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    
    # 3. Get existing topics to avoid "already exists" errors
    existing_topics = admin_client.list_topics(timeout=10).topics
    
    new_topics_to_create = []

    # 4. Parse projects and topics from YAML
    for project in data.get('projects', []):
        print(f"--- Processing Project: {project['name']} ---")
        
        for topic_cfg in project.get('topics', []):
            topic_name = topic_cfg['name']
            
            if topic_name in existing_topics:
                print(f"INFO: Topic '{topic_name}' already exists. Skipping.")
                continue

            # 5. Build the NewTopic object
            print(f"PLAN: Creating topic '{topic_name}' with {topic_cfg['partitions']} partitions.")
            new_topic = NewTopic(
                topic=topic_name,
                num_partitions=topic_cfg['partitions'],
                replication_factor=topic_cfg['replication'],
                config=topic_cfg.get('config', {})
            )
            new_topics_to_create.append(new_topic)

    # 6. Execute creation
    if new_topics_to_create:
        fs = admin_client.create_topics(new_topics_to_create)

        # Wait for each operation to finish
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print(f"SUCCESS: Topic '{topic}' created.")
            except Exception as e:
                print(f"FAILED: Failed to create topic '{topic}': {e}")
    else:
        print("DONE: No new topics to create.")

if __name__ == "__main__":
    # In your Docker lab, use 'broker-1:19092' if running inside the network
    # or 'localhost:9092' if running from your host machine.
    parser = argparse.ArgumentParser(description="Sync Kafka Topics")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--file", default="topology.yml", help="Path to topology file")
    args = parser.parse_args()

    sync_kafka_topics(args.file, args.bootstrap)
