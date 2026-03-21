import yaml
import argparse
from confluent_kafka.admin import (
    AdminClient,
    AclBinding,
    AclBindingFilter,
    ResourceType,
    AclOperation,
    AclPermissionType,
    ResourcePatternType
)


def enum_name(e):
    """Safe enum name extraction"""
    return getattr(e, "name", str(e))


def sync_acls(descriptor_path, bootstrap_servers):
    with open(descriptor_path, 'r') as file:
        data = yaml.safe_load(file)

    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    # 1. Parse Desired State from YAML
    desired_bindings = []
    desired_fingerprints = set()

    print(f"--- Loading ACL Descriptor: {descriptor_path} ---")
    for entry in data.get('acls', []):
        r_type = getattr(ResourceType, entry['resource_type'].upper(), ResourceType.UNKNOWN)
        p_type = getattr(ResourcePatternType, entry['pattern_type'].upper(), ResourcePatternType.LITERAL)

        for rule in entry.get('rules', []):
            op = getattr(AclOperation, rule['operation'].upper(), AclOperation.ANY)
            perm = getattr(AclPermissionType, rule['permission'].upper(), AclPermissionType.ANY)

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

            fingerprint = f"{rule['principal']}|{rule['operation'].upper()}|{rule['permission'].upper()}|{entry['resource_name']}|{entry['pattern_type'].upper()}"
            desired_fingerprints.add(fingerprint)

    # 2. Apply Desired ACLs (Create)
    if desired_bindings:
        print(f"--- Syncing {len(desired_bindings)} Desired ACLs ---")
        fs_create = admin_client.create_acls(desired_bindings)

        for binding, f in fs_create.items():
            try:
                f.result()
                print(f"VERIFIED/CREATED: {binding.principal} on {binding.name} ({enum_name(binding.operation)})")
            except Exception as e:
                if "already exists" not in str(e).lower():
                    print(f"ERROR creating ACL: {e}")

    # 3. Cleanup: Remove Stale ACLs
    print("--- Running ACL Cleanup ---")

    # ✅ FIX: provide ALL required fields
    acl_filter = AclBindingFilter(
        restype=ResourceType.ANY,
        name=None,
        resource_pattern_type=ResourcePatternType.ANY,
        principal=None,
        host=None,
        operation=AclOperation.ANY,
        permission_type=AclPermissionType.ANY
    )

    try:
        live_acls_future = admin_client.describe_acls(acl_filter)
        live_acls = live_acls_future.result()

        to_delete = []

        for acl in live_acls:
            # SAFETY
            if acl.principal.startswith("User:admin") or acl.principal == "User:ANONYMOUS":
                continue

            live_fingerprint = (
                f"{acl.principal}|"
                f"{enum_name(acl.operation)}|"
                f"{enum_name(acl.permission_type)}|"
                f"{acl.name}|"
                f"{enum_name(acl.resource_pattern_type)}"
            )

            if live_fingerprint not in desired_fingerprints:
                print(f"PLAN: [DELETE] {acl.principal} -> {enum_name(acl.operation)} on {acl.name}")

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
                    deleted_bindings = f.result()
                    print(f"SUCCESS: Removed {len(deleted_bindings)} stale ACL bindings.")
                except Exception as e:
                    print(f"ERROR during cleanup: {e}")
        else:
            print("INFO: No stale ACLs found. Cluster security is in sync.")

    except Exception as e:
        print(f"CRITICAL: Could not fetch live ACLs: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka ACL Manager")
    parser.add_argument("--bootstrap", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--file", required=True, help="Path to ACL descriptor YAML")
    args = parser.parse_args()

    sync_acls(args.file, args.bootstrap)