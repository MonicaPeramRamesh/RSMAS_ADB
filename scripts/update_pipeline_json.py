import json
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--file", required=True)
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema", required=True)
parser.add_argument("--secret_scope", required=True)

args = parser.parse_args()

with open(args.file, "r") as f:
    pipeline = json.load(f)

# Update engine-level catalog & schema
pipeline["catalog"] = args.catalog
pipeline["schema"] = args.schema

# Update runtime configuration
pipeline["configuration"]["rsmas.catalog"] = args.catalog
pipeline["configuration"]["rsmas.schema"] = args.schema
pipeline["configuration"]["rsmas.secret.scope"] = args.secret_scope

with open(args.file, "w") as f:
    json.dump(pipeline, f, indent=2)

print(f"✅ Updated pipeline config: {args.file}")
