import yaml
import argparse
from confluent_kafka.admin import (AdminClient, AclBinding, AclBindingFilter, 
                                   ResourceType, ACLOperation, ACLPermissionType, PatternType)

def sync_acls(descriptor_path, bootstrap_servers):
    with open(descriptor_path, 'r') as file:
        data = yaml.safe_load(file)

    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    
    # 1. Parse Desired State from YAML
    desired_bindings = []
    # We create a set of unique string representations to compare against live ACLs
    desired_fingerprints = set()

    for entry in data.get('acls', []):
        r_type = getattr(ResourceType, entry['resource_type'].upper(), ResourceType.UNKNOWN)
        p_type = getattr(PatternType, entry['pattern_type'].upper(), PatternType.LITERAL)
        
        for rule in entry.get('rules', []):
            op = getattr(ACLOperation, rule['operation'].upper(), ACLOperation.UNKNOWN)
            perm = getattr(ACLPermissionType, rule['permission'].upper(), ACLPermissionType.ALLOW)

            binding = AclBinding(
                restype=r_type,
                name=entry['resource_name'],
                resource_pattern_type=p_type,
                principal=rule['principal'],
                host="*",
                operation=op,
                permission_type=perm
            )
            desired_bindings.append(binding)
            # Create a unique ID for this ACL: "Principal|Operation|Permission|Resource|Pattern"
            fingerprint = f"{rule['principal']}|{rule['operation'].upper()}|{rule['permission'].upper()}|{entry['resource_name']}|{entry['pattern_type'].upper()}"
            desired_fingerprints.add(fingerprint)

    # 2. Apply Desired ACLs (Create)
    if desired_bindings:
        print(f"--- Syncing {len(desired_bindings)} Desired ACLs ---")
        fs_create = admin_client.create_acls(desired_bindings)
        for binding, f in fs_create.items():
            try:
                f.result()
                print(f"VERIFIED/CREATED: {binding.principal} on {binding.name}")
            except Exception as e:
                if "already exists" not in str(e).lower():
                    print(f"ERROR creating ACL: {e}")

    # 3. Cleanup: Remove Stale ACLs (Reconciliation)
    print("--- Running ACL Cleanup ---")
    
    # Create a filter to fetch ALL existing ACLs
    acl_filter = AclBindingFilter(
        restype=ResourceType.ANY,
        resource_pattern_type=PatternType.ANY,
        operation=ACLOperation.ANY,
        permission_type=ACLPermissionType.ANY
    )
    
    # Fetch live ACLs
    live_acls_future = admin_client.describe_acls(acl_filter)
    try:
        live_acls = live_acls_future.result()
        
        to_delete = []
        for acl in live_acls:
            # Skip internal Kafka superuser/system ACLs if they exist
            if acl.principal.startswith("User:admin") or acl.principal == "User:ANONYMOUS":
                continue
                
            # Create fingerprint for the live ACL
            live_fingerprint = f"{acl.principal}|{acl.operation.name}|{acl.permission_type.name}|{acl.name}|{acl.resource_pattern_type.name}"
            
            if live_fingerprint not in desired_fingerprints:
                print(f"PLAN: [DELETE] Stale ACL: {acl.principal} -> {acl.operation.name} on {acl.name}")
                # Create a specific filter to delete exactly this binding
                delete_filter = AclBindingFilter(
                    restype=acl.restype,
                    name=acl.name,
                    resource_pattern_type=acl.resource_pattern_type,
                    principal=acl.principal,
                    host=acl.host,
                    operation=acl.operation,
                    permission_type=acl.permission_type
                )
                to_delete.append(delete_filter)

        if to_delete:
            fs_delete = admin_client.delete_acls(to_delete)
            for delete_filter, f in fs_delete.items():
                try:
                    # delete_acls returns a list of deleted bindings for each filter
                    deleted_bindings = f.result()
                    print(f"SUCCESS: Removed {len(deleted_bindings)} stale ACL bindings.")
                except Exception as e:
                    print(f"ERROR during cleanup: {e}")
        else:
            print("INFO: No stale ACLs found. Cluster is clean.")

    except Exception as e:
        print(f"CRITICAL: Could not fetch live ACLs: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--file", default="acls.yml")
    args = parser.parse_args()
    sync_acls(args.file, args.bootstrap)