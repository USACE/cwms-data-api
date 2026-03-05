# Management CLI

The CWMS Access Management CLI (cwms-admin) provides command-line access to manage users, roles, and authorization policies for the CWMS system. It offers an interactive terminal interface with formatted tables, colored output, and loading spinners.

## Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Runtime | Node.js 24+ | JavaScript execution environment |
| Language | TypeScript 5.6+ | Type-safe development |
| CLI Framework | Commander | Command parsing and help generation |
| Terminal UI | Ink | React-based terminal rendering |
| HTTP Client | Axios | API communication |
| Colors | Chalk | Terminal text styling |
| Spinners | Ora | Loading state indicators |
| Logging | Pino | Structured JSON logging |
| Validation | Zod | Schema validation |

## Features

- User management operations (list and view details)
- Role management operations (list and view details)
- Policy management operations (list and view details)
- Authentication with token persistence
- Formatted table output with box-drawing characters
- Colored status indicators
- Loading spinners during API calls
- Structured logging for debugging

## Installation

### NPM Installation (Recommended)

```bash
npm install -g @usace/cwms-admin
```

### Download and Install

Download the appropriate archive for your platform:

| Platform | File |
|----------|------|
| macOS (Apple Silicon) | cwms-admin-v0.1.0-darwin-arm64.tar.gz |
| macOS (Intel) | cwms-admin-v0.1.0-darwin-x64.tar.gz |
| Linux | cwms-admin-v0.1.0-linux-x64.tar.gz |
| Any platform | cwms-admin-v0.1.0-portable.zip |

Extract and run the installer:

```bash
tar -xzf cwms-admin-v0.1.0-*.tar.gz
chmod +x install-from-archive.sh
./install-from-archive.sh
```

### Global Link from Source

For development, link from the built distribution:

```bash
cd dist/apps/cli/management-cli
npm link
```

## System Requirements

| Requirement | Minimum | Recommended |
|-------------|---------|-------------|
| Node.js | 20.0.0 | 24.0.0+ |
| Disk Space | 50MB | 100MB |
| OS | macOS, Linux, Windows 10+ | macOS, Linux |
| Terminal | Unicode support | Color support |

## Configuration

### Configuration File

The CLI stores configuration in `~/.cwms-admin/config.json`:

```json
{
  "apiUrl": "http://localhost:3002",
  "token": "your-auth-token",
  "username": "admin"
}
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| LOG_LEVEL | Logging level (debug, info, warn, error) | info |
| NODE_ENV | Environment (development, production) | production |
| MANAGEMENT_API_URL | Default API URL | http://localhost:3002 |
| KEYCLOAK_ADMIN_USER | Default admin username | admin |
| KEYCLOAK_ADMIN_PASSWORD | Default admin password | admin |

## Command Reference

### Global Options

```bash
cwms-admin --version    # Display version number
cwms-admin --help       # Display help information
```

### Authentication Commands

#### login

Authenticate with the management API and store credentials.

```bash
cwms-admin login [options]
```

| Option | Description | Default |
|--------|-------------|---------|
| -u, --username | Admin username | admin |
| -p, --password | Admin password | admin |
| -a, --api-url | Management API URL | http://localhost:3002 |

Examples:

```bash
cwms-admin login -u admin -p password
cwms-admin login -u admin -p password -a http://api.example.com:3002
```

On success, the authentication token is saved to `~/.cwms-admin/config.json`.

#### logout

Clear stored credentials and log out.

```bash
cwms-admin logout
```

### Users Commands

#### users list

Display all users in a formatted table.

```bash
cwms-admin users list
```

Output columns:

| Column | Description |
|--------|-------------|
| Username | User's login name |
| ID | Unique user identifier |
| Email | User's email address |
| Name | Full name (first + last) |
| Status | Enabled or Disabled (color coded) |

#### users show

Display detailed information for a specific user.

```bash
cwms-admin users show <id>
```

| Argument | Description |
|----------|-------------|
| id | User ID or username to display |

Example:

```bash
cwms-admin users show m5hectest
```

Output fields:

| Field | Description |
|-------|-------------|
| Username | User's login name |
| ID | Unique identifier |
| Email | Email address (if set) |
| First Name | First name (if set) |
| Last Name | Last name (if set) |
| Status | Enabled or Disabled |

### Roles Commands

#### roles list

Display all roles in a formatted table.

```bash
cwms-admin roles list
```

Output columns:

| Column | Description |
|--------|-------------|
| Name | Role name |
| ID | Unique role identifier |
| Description | Role description |

#### roles show

Display detailed information for a specific role.

```bash
cwms-admin roles show <id>
```

| Argument | Description |
|----------|-------------|
| id | Role ID to display |

Example:

```bash
cwms-admin roles show cwms_user
```

Output fields:

| Field | Description |
|-------|-------------|
| Name | Role name |
| ID | Unique identifier |
| Description | Role description (if set) |

### Policies Commands

#### policies list

Display all authorization policies in a formatted table.

```bash
cwms-admin policies list
```

Output columns:

| Column | Description |
|--------|-------------|
| Name | Policy name |
| ID | Unique policy identifier |
| Description | Policy description |

#### policies show

Display detailed information for a specific policy, including rule definitions.

```bash
cwms-admin policies show <id>
```

| Argument | Description |
|----------|-------------|
| id | Policy ID to display |

Example:

```bash
cwms-admin policies show office-restriction
```

Output fields:

| Field | Description |
|-------|-------------|
| Name | Policy name |
| ID | Unique identifier |
| Description | Policy description |
| Rules | JSON-formatted policy rules |

## Output Formats

### Table Output

List commands display data in formatted tables with box-drawing characters:

```
Found 3 users
+-----------+------+-----------------+------------+---------+
| Username  | ID   | Email           | Name       | Status  |
+-----------+------+-----------------+------------+---------+
| m5hectest | 001  | m5@test.com     | M5 Test    | Enabled |
| l2hectest | 002  | l2@test.com     | L2 Test    | Enabled |
| l1hectest | 003  | -               | L1 Test    | Disabled|
+-----------+------+-----------------+------------+---------+
```

### Detail Output

Show commands display key-value pairs with aligned labels:

```
User Details
Username:    m5hectest
ID:          001
Email:       m5@test.com
First Name:  M5
Last Name:   Test
Status:      Enabled
```

### Status Colors

| Status | Color |
|--------|-------|
| Enabled | Green |
| Disabled | Red |
| Loading | Cyan |
| Warning | Yellow |
| Error | Red |

## Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 1 | General error (authentication failure, API error, validation error) |

## Building from Source

### Prerequisites

- Node.js 24+
- pnpm 10+
- Access to the cwms-access-management monorepo

### Build Steps

```bash
cd cwms-access-management

pnpm install

pnpm nx build management-cli --configuration=production
```

Output location: `dist/apps/cli/management-cli/index.js`

### Development Mode

Run with hot reload during development:

```bash
pnpm nx serve management-cli
```

Or using tsx directly:

```bash
cd apps/cli/management-cli
pnpm dev
```

### Create Distribution Package

```bash
./apps/cli/management-cli/scripts/build-executable.sh
```

Output in `./release/` directory:

- Platform-specific tarballs (darwin-arm64, darwin-x64, linux-x64)
- Cross-platform portable ZIP archive

## Troubleshooting

### Command not found

If you see "command not found: cwms-admin" after npm installation:

```bash
export PATH="$PATH:$(npm bin -g)"
```

Add this line to `~/.bashrc` or `~/.zshrc` for persistence.

### Permission denied

For npm permission errors during global installation:

```bash
npm config set prefix ~/.npm-global
export PATH=~/.npm-global/bin:$PATH
npm install -g @usace/cwms-admin
```

### Cannot connect to API

Verify your configuration:

```bash
cat ~/.cwms-admin/config.json
```

Re-authenticate with the correct API URL:

```bash
cwms-admin login -u admin -p password -a http://correct-api-url:3002
```

### Authentication required error

If commands fail with "Not authenticated. Please run: cwms-admin login":

```bash
cwms-admin login -u admin -p password
```

### Table rendering issues

Ensure your terminal supports Unicode:

```bash
echo $LANG
export LANG=en_US.UTF-8
```

## API Integration

The CLI communicates with the Management API service (default port 3002). All requests include the stored authentication token in the Authorization header.

### Endpoints Used

| Command | Method | Endpoint |
|---------|--------|----------|
| users list | GET | /users |
| users show | GET | /users/:id |
| roles list | GET | /roles |
| roles show | GET | /roles/:id |
| policies list | GET | /policies |
| policies show | GET | /policies/:id |
| login | POST | /login |

### Request Timeout

All API requests have a 10-second timeout. For slow network connections, ensure the Management API is accessible and responsive.

## Project Structure

```
apps/cli/management-cli/
├── src/
│   ├── commands/           # Command implementations
│   │   ├── login.ts        # Authentication commands
│   │   ├── users.tsx       # User management commands
│   │   ├── roles.tsx       # Role management commands
│   │   └── policies.tsx    # Policy management commands
│   ├── ink/
│   │   ├── components/     # Reusable UI components
│   │   │   ├── ink-table.tsx      # Table component
│   │   │   └── status-message.tsx # Status display
│   │   ├── screens/        # Command output screens
│   │   │   ├── users-list.tsx
│   │   │   ├── user-details.tsx
│   │   │   ├── roles-list.tsx
│   │   │   ├── role-details.tsx
│   │   │   ├── policies-list.tsx
│   │   │   └── policy-details.tsx
│   │   └── render.ts       # Ink rendering utilities
│   ├── services/
│   │   └── api.service.ts  # API client
│   ├── utils/
│   │   ├── config.ts       # Configuration management
│   │   ├── error.ts        # Error handling utilities
│   │   ├── logger.ts       # Pino logger setup
│   │   └── version.ts      # Version utilities
│   └── index.ts            # CLI entry point
├── scripts/
│   ├── build-executable.sh     # Distribution build script
│   ├── install-from-archive.sh # User installation script
│   └── prepare-dist.sh         # Distribution preparation
├── docs/
│   ├── installation.md     # End-user installation guide
│   └── distribution.md     # Build and distribution guide
├── package.json
└── tsconfig.json
```

## Related Documentation

- [Management UI](management-ui.md) - Web-based management interface
- [Architecture Overview](../architecture/index.md) - System architecture
- [Proxy API Reference](../proxy-api/index.md) - Authorization proxy API documentation
