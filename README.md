# OpenBao Database Plugin for ClickHouse

This plugin provides ClickHouse database connectivity for [OpenBao](https://openbao.org/), enabling dynamic credential management using SQL user management.

> **Note**: This plugin is adapted from [ContentSquare/vault-plugin-database-clickhouse](https://github.com/ContentSquare/vault-plugin-database-clickhouse) for use with OpenBao instead of HashiCorp Vault.

## Features

- Dynamic user creation with temporary credentials
- Credential rotation and revocation
- TLS connection support
- SQL-based user management
- Role-based access control integration
- ClickHouse cluster support with `ON CLUSTER` syntax

## Prerequisites

- OpenBao 2.4.4 or later (uses SDK v2.4.0)
- ClickHouse 21.8 or later with SQL user management enabled (tested with v25.12)
- Go 1.23+ (for building from source)

### ClickHouse Requirements

ClickHouse must be configured to use SQL-based user management instead of the default XML-based configuration. This requires:

1. A user with `access_management=1` permission (typically the `default` user)
2. Database roles defined in advance if you want to assign roles to dynamic users

## Building the Plugin

```bash
# Clone the repository
git clone https://github.com/elaunira/openbao-plugin-database-clickhouse.git
cd openbao-plugin-database-clickhouse

# Build the plugin
make build

# Or with a specific version
make build VERSION=v2.4.4

# Calculate SHA256 checksum (needed for plugin registration)
sha256sum clickhouse-database-plugin
```

## Plugin Registration

### 1. Copy the Plugin Binary

Copy the compiled plugin binary to the OpenBao plugin directory:

```bash
# Create plugin directory if it doesn't exist
sudo mkdir -p /etc/openbao/plugins

# Copy the plugin
sudo cp clickhouse-database-plugin /etc/openbao/plugins/

# Set appropriate permissions
sudo chmod 755 /etc/openbao/plugins/clickhouse-database-plugin
```

### 2. Configure OpenBao

Ensure your OpenBao configuration includes the plugin directory:

```hcl
# /etc/openbao/config.hcl
plugin_directory = "/etc/openbao/plugins"

listener "tcp" {
  address     = "0.0.0.0:8200"
  tls_disable = true  # Set to false in production with proper TLS
}

storage "file" {
  path = "/var/lib/openbao/data"
}
```

### 3. Register the Plugin

```bash
# Get the SHA256 checksum of the plugin
PLUGIN_SHA256=$(sha256sum /etc/openbao/plugins/clickhouse-database-plugin | cut -d' ' -f1)

# Register the plugin
bao plugin register -sha256=$PLUGIN_SHA256 database clickhouse-database-plugin
```

### 4. Enable the Database Secrets Engine

```bash
bao secrets enable database
```

## Configuration

### Basic Configuration

```bash
bao write database/config/clickhouse \
    plugin_name=clickhouse-database-plugin \
    allowed_roles="*" \
    connection_url="clickhouse://{{username}}:{{password}}@clickhouse.example.com:9000/default" \
    username="admin" \
    password="admin_password"
```

### Configuration with TLS

For secure connections (port 9440), add `secure=true`:

```bash
bao write database/config/clickhouse \
    plugin_name=clickhouse-database-plugin \
    allowed_roles="*" \
    connection_url="clickhouse://{{username}}:{{password}}@clickhouse.example.com:9440/default?secure=true" \
    username="admin" \
    password="admin_password"
```

### Configuration with TLS and Skip Verification

For self-signed certificates:

```bash
bao write database/config/clickhouse \
    plugin_name=clickhouse-database-plugin \
    allowed_roles="*" \
    connection_url="clickhouse://{{username}}:{{password}}@clickhouse.example.com:9440/default?secure=true&skip_verify=true" \
    username="admin" \
    password="admin_password"
```

### Configuration Parameters

| Parameter | Description | Required |
|-----------|-------------|----------|
| `connection_url` | ClickHouse connection URL | Yes (or use host/port) |
| `host` | ClickHouse server hostname | Yes (if no connection_url) |
| `port` | ClickHouse server port (9000 for native, 9440 for TLS) | Yes (if no connection_url) |
| `username` | Admin username for managing users | Yes |
| `password` | Admin password | Yes |
| `database` | Default database name | No |
| `tls` | Enable TLS connection | No (default: false) |
| `tls_skip_verify` | Skip TLS certificate verification | No (default: false) |
| `max_open_connections` | Maximum open connections | No (default: 4) |
| `max_idle_connections` | Maximum idle connections | No (default: max_open) |
| `max_connection_lifetime` | Connection lifetime in seconds | No (default: 0/unlimited) |
| `username_template` | Template for generating usernames | No |

## Creating Roles

### Basic Role

```bash
bao write database/roles/my-role \
    db_name=clickhouse \
    creation_statements="CREATE USER IF NOT EXISTS '{{name}}' IDENTIFIED BY '{{password}}'" \
    default_ttl="1h" \
    max_ttl="24h"
```

### Role with Database Permissions

```bash
bao write database/roles/readonly \
    db_name=clickhouse \
    creation_statements="CREATE USER IF NOT EXISTS '{{name}}' IDENTIFIED BY '{{password}}'; GRANT SELECT ON mydb.* TO '{{name}}'" \
    revocation_statements="DROP USER IF EXISTS '{{name}}'" \
    default_ttl="1h" \
    max_ttl="24h"
```

### Role with ClickHouse Role Assignment

First, create a role in ClickHouse:

```sql
CREATE ROLE readonly_role;
GRANT SELECT ON mydb.* TO readonly_role;
```

Then create an OpenBao role that assigns this ClickHouse role:

```bash
bao write database/roles/readonly \
    db_name=clickhouse \
    creation_statements="CREATE USER IF NOT EXISTS '{{name}}' IDENTIFIED BY '{{password}}'; GRANT readonly_role TO '{{name}}'" \
    revocation_statements="REVOKE readonly_role FROM '{{name}}'; DROP USER IF EXISTS '{{name}}'" \
    default_ttl="1h" \
    max_ttl="24h"
```

### Role for ClickHouse Cluster

For ClickHouse clusters, use `ON CLUSTER`:

```bash
bao write database/roles/cluster-role \
    db_name=clickhouse \
    creation_statements="CREATE USER IF NOT EXISTS '{{name}}' ON CLUSTER 'my_cluster' IDENTIFIED BY '{{password}}'; GRANT SELECT ON mydb.* TO '{{name}}' ON CLUSTER 'my_cluster'" \
    revocation_statements="DROP USER IF EXISTS '{{name}}' ON CLUSTER 'my_cluster'" \
    default_ttl="1h" \
    max_ttl="24h"
```

## Generating Credentials

```bash
# Generate new credentials
bao read database/creds/my-role

# Example output:
# Key                Value
# ---                -----
# lease_id           database/creds/my-role/abcd1234
# lease_duration     1h
# lease_renewable    true
# password           A1B2C3D4E5F6G7H8
# username           v-token-my-role-abc123def456-1234567890
```

## Statement Template Variables

The following variables are available in creation, revocation, and rotation statements:

| Variable | Description |
|----------|-------------|
| `{{name}}` | Generated username |
| `{{username}}` | Alias for `{{name}}` |
| `{{password}}` | Generated password |
| `{{expiration}}` | Credential expiration time |

## Rotating Root Credentials

```bash
bao write -force database/rotate-root/clickhouse
```

## Username Templates

You can customize the username format using Go template syntax:

```bash
bao write database/config/clickhouse \
    plugin_name=clickhouse-database-plugin \
    allowed_roles="*" \
    connection_url="clickhouse://{{username}}:{{password}}@clickhouse.example.com:9000/default" \
    username="admin" \
    password="admin_password" \
    username_template="{{ printf \"myapp-%s-%s\" (.RoleName | truncate 10) (random 8) }}"
```

Available template functions:
- `random N` - Generate N random characters
- `truncate N` - Truncate to N characters
- `uppercase` / `lowercase` - Case conversion
- `unix_time` - Current Unix timestamp
- `uuid` - Generate UUID

## Testing

Run tests with Docker:

```bash
go test -v ./...
```

Or with an existing ClickHouse instance:

```bash
CLICKHOUSE_URL="clickhouse://localhost:9000?username=default&password=password" go test -v ./...
```

## Troubleshooting

### Plugin not found

Ensure the plugin binary is in the configured plugin directory and has execute permissions:

```bash
ls -la /etc/openbao/plugins/clickhouse-database-plugin
```

### Connection errors

Verify ClickHouse connectivity:

```bash
# Using clickhouse-client
clickhouse-client --host clickhouse.example.com --port 9000 --user admin --password admin_password

# Or using the native protocol
nc -zv clickhouse.example.com 9000
```

### Permission errors

Ensure the admin user has `access_management=1`:

```sql
SHOW GRANTS FOR admin;
```

### TLS issues

For self-signed certificates, use `skip_verify=true` in the connection URL:

```
clickhouse://host:9440?secure=true&skip_verify=true
```

## License

This project is licensed under the Mozilla Public License 2.0 (MPL-2.0).

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## Acknowledgments

This plugin is adapted from [ContentSquare/vault-plugin-database-clickhouse](https://github.com/ContentSquare/vault-plugin-database-clickhouse) for use with OpenBao.
