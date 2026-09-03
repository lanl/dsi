# Installation Requirements

## Python Dependencies Only!

Both versions use **pure Python** libraries - no external tools required.

### Install Python packages
```bash
cd tools/federated
pip install -r requirements.txt
```

The `requirements.txt` includes:
- Flask==3.0.0
- Werkzeug==3.0.1

### DSI Package Dependencies

The parent DSI package already includes:
- **asyncssh** - Used for SSH/SFTP operations with password support
- All other required dependencies

## Authentication Options

The interactive version (`app_inter.py`) supports multiple authentication methods:

1. **Password Authentication** (via asyncssh)
   - Enter password in the UI
   - Pure Python, no external tools
   - Works with any SSH server

2. **SSH Key Authentication**
   - Leave password field blank
   - Uses your default SSH keys (~/.ssh/id_rsa, etc.)

3. **Kerberos Authentication**
   - Leave password field blank
   - Requires valid Kerberos ticket (`klist` to check)

## Quick Start

```bash
# Standard version (terminal prompts)
python app.py
# Access at http://localhost:5000

# Interactive version (UI prompts)
python app_inter.py
# Access at http://localhost:5001
```

No additional installation required! 🎉
