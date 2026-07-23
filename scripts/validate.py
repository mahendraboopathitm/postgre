import os
import sys
from typing import Tuple, List, Dict, Any

# Add the script's directory to python path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils import load_yaml

def validate_config(config_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validates the customer ingestion config schema.
    Returns (is_valid, error_messages)
    """
    errors = []
    
    if not config_data:
        return False, ["Configuration data is empty or invalid."]

    # 1. Validate customer section
    customer = config_data.get("customer")
    if not customer:
        errors.append("Missing required top-level 'customer' section.")
    else:
        required_customer_fields = ["name", "workspace_url", "catalog", "schema"]
        for field in required_customer_fields:
            val = customer.get(field)
            if not val:
                errors.append(f"Missing required field: 'customer.{field}'.")
            elif not isinstance(val, str) or not val.strip():
                errors.append(f"Field 'customer.{field}' must be a non-empty string.")

    # 2. Validate connectors section
    connectors = config_data.get("connectors")
    if not connectors:
        errors.append("Missing required top-level 'connectors' section.")
    elif not isinstance(connectors, list):
        errors.append("Field 'connectors' must be a list of connector configurations.")
    else:
        for i, conn in enumerate(connectors):
            conn_name = conn.get("name")
            prefix = f"connectors[{i}] (name: {conn_name or 'unnamed'})"
            
            if not conn_name:
                errors.append(f"connectors[{i}]: Missing required field 'name'.")
            elif not isinstance(conn_name, str) or not conn_name.strip():
                errors.append(f"connectors[{i}].name must be a non-empty string.")
                
            conn_name_val = conn.get("connection_name")
            if not conn_name_val:
                errors.append(f"{prefix}: Missing required field 'connection_name'.")
            elif not isinstance(conn_name_val, str) or not conn_name_val.strip():
                errors.append(f"{prefix}.connection_name must be a non-empty string.")
                
            # Tables validation
            tables = conn.get("tables")
            if tables is None:
                errors.append(f"{prefix}: Missing 'tables' configuration.")
            elif not isinstance(tables, list):
                errors.append(f"{prefix}: 'tables' field must be a list.")
            elif len(tables) == 0:
                errors.append(f"{prefix}: 'tables' list cannot be empty.")
            else:
                for j, table in enumerate(tables):
                    table_prefix = f"{prefix}.tables[{j}]"
                    if not isinstance(table, dict):
                        errors.append(f"{table_prefix} must be a dictionary configuration.")
                        continue
                    
                    src_table = table.get("source_table")
                    if not src_table:
                        errors.append(f"{table_prefix}: Missing required field 'source_table'.")
                    elif not isinstance(src_table, str) or not src_table.strip():
                        errors.append(f"{table_prefix}.source_table must be a non-empty string.")
                        
    return len(errors) == 0, errors

def main():
    if len(sys.argv) < 2:
        print("Usage: python validate.py <config_path>")
        sys.exit(1)
        
    config_path = sys.argv[1]
    print(f"Validating configuration file: {config_path}")
    
    try:
        config_data = load_yaml(config_path)
    except Exception as e:
        print(f"[ERROR] Error loading YAML file: {e}")
        sys.exit(1)
        
    is_valid, errors = validate_config(config_data)
    if is_valid:
        print("[OK] Configuration is VALID.")
        sys.exit(0)
    else:
        print("[ERROR] Configuration is INVALID. Found the following errors:")
        for err in errors:
            print(f" - {err}")
        sys.exit(1)

if __name__ == "__main__":
    main()
