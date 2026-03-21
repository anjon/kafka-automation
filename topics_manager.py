import yaml
import argparse
from confluent_kafka.admin import AdminClient, NewTopic

def sync_kafka_topics(descriptor_path, bootstrap_servers):
    # 1. Load the Desired State from YAML
    with open(descriptor_path, 'r') as file:
        data = yaml.safe_load(file)

    # Extract all topic names defined in the YAML
    desired_topics = set()
    topic_configs = {}
    for project in data.get('projects', []):
        for topic_cfg in project.get('topics', []):
            name = topic_cfg['name']
            desired_topics.add(name)
            topic_configs[name] = topic_cfg

    # 2. Initialize Admin Client and get Live State
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    metadata = admin_client.list_topics(timeout=10)
    # We filter out internal Kafka topics like __consumer_offsets
    live_topics = {t for t in metadata.topics.keys() if not t.startswith('__')}

    print(f"Found {len(live_topics)} live topics and {len(desired_topics)} desired topics.")

    # --- PART A: CREATE MISSING TOPICS ---
    to_create = []
    for name in desired_topics:
        if name not in live_topics:
            cfg = topic_configs[name]
            print(f"PLAN: [CREATE] topic '{name}'")
            to_create.append(NewTopic(
                topic=name,
                num_partitions=cfg['partitions'],
                replication_factor=cfg['replication'],
                config=cfg.get('config', {})
            ))

    if to_create:
        fs_create = admin_client.create_topics(to_create)
        for topic, f in fs_create.items():
            try:
                f.result()
                print(f"SUCCESS: Created {topic}")
            except Exception as e:
                print(f"ERROR: Could not create {topic}: {e}")

    # --- PART B: DELETE REMOVED TOPICS ---
    to_delete = [t for t in live_topics if t not in desired_topics]
    
    if to_delete:
        print(f"PLAN: [DELETE] the following topics: {to_delete}")
        fs_delete = admin_client.delete_topics(to_delete)
        for topic, f in fs_delete.items():
            try:
                f.result()
                print(f"SUCCESS: Deleted {topic}")
            except Exception as e:
                print(f"ERROR: Could not delete {topic}: {e}")
    else:
        print("INFO: No topics to delete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Topic Reconciler")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--file", default="topology.yml", help="Path to topology file")
    args = parser.parse_args()

    sync_kafka_topics(args.file, args.bootstrap)