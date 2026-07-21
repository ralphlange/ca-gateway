import sys
import json
import re

def convert(pvlist_file, access_file=None):
    clients = [{"name": "internal", "provider": "ca", "addrlist": "localhost", "autoaddrlist": False}]
    servers = [{"name": "gateway", "clients": ["internal"], "statusprefix": "sts:"}]

    if access_file:
        servers[0]["access"] = access_file

    patterns = []
    with open(pvlist_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'): continue
            # Handle EVALUATION ORDER etc
            if line.startswith('EVALUATION'): continue

            parts = line.split()
            if len(parts) >= 2:
                pattern = parts[0]
                action = parts[1]
                if action == "ALLOW":
                    patterns.append({"pattern": pattern, "client": "internal"})

    config = {
        "version": 2,
        "clients": clients,
        "servers": servers,
        "pvlist": patterns
    }
    return config

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: pvlist_to_json.py <pvlist_file> [access_file]")
        sys.exit(1)
    pvlist = sys.argv[1]
    access = sys.argv[2] if len(sys.argv) > 2 else None
    print(json.dumps(convert(pvlist, access), indent=4))
