import paramiko
import getpass
from pathlib import Path
import sys


def sftp_connect(remote, password=None, verbose=False):
    """
    Connect via SFTP with keyboard-interactive authentication.
    Supports MFA/2FA for HPC systems.
    
    Args:
        remote: string in format "username@hostname"
        password: optional password string
        verbose: print debug information
        
    Returns:
        tuple: (connection, sftp_client)
    """
    username, hostname = remote.split("@")
    
    if verbose:
        print(f" -- DEBUG: Connecting to {hostname} as {username}")
    
    # Create transport
    transport = paramiko.Transport((hostname, 22))
    
    try:
        transport.connect()
        
        if verbose:
            print(f" -- DEBUG: Transport connected")
            print(f" -- DEBUG: Server supports: {transport.get_security_options().key_types}")
        
        # Get available authentication methods
        try:
            transport.auth_none(username)
        except paramiko.BadAuthenticationType as e:
            allowed_methods = e.allowed_types
            if verbose:
                print(f" -- DEBUG: Allowed auth methods: {allowed_methods}")
        
        # Define handler for keyboard-interactive prompts
        prompt_count = [0]  # Use list to make it mutable in nested function
        
        def auth_handler(title, instructions, prompt_list):
            """
            Handler for keyboard-interactive authentication.
            Handles MFA (password + token).
            """
            if verbose:
                print(f" -- DEBUG: Auth prompt #{prompt_count[0] + 1}")
                if title:
                    print(f" -- DEBUG: Title: {title}")
                if instructions:
                    print(f" -- DEBUG: Instructions: {instructions}")
            
            responses = []
            for prompt, echo in prompt_list:
                prompt_count[0] += 1
                
                if verbose:
                    print(f" -- DEBUG: Prompt: {prompt} (echo={echo})")
                
                # First prompt is usually password
                if prompt_count[0] == 1 and password:
                    if verbose:
                        print(f" -- DEBUG: Using provided password")
                    responses.append(password)
                else:
                    # Subsequent prompts (like MFA token) or if no password provided
                    if echo:
                        response = input(f" -- {prompt}")
                    else:
                        response = getpass.getpass(f" -- {prompt}")
                    responses.append(response)
            
            return responses
        
        # Try keyboard-interactive authentication
        try:
            if verbose:
                print(f" -- DEBUG: Attempting keyboard-interactive auth")
            
            transport.auth_interactive(username, auth_handler)
            
            if verbose:
                print(f" -- DEBUG: Authentication successful!")
            
            # Create SFTP client
            sftp = paramiko.SFTPClient.from_transport(transport)
            return transport, sftp
            
        except paramiko.AuthenticationException as e:
            if verbose:
                print(f" -- DEBUG: Keyboard-interactive failed: {e}")
            raise
        
    except Exception as e:
        transport.close()
        raise Exception(f"Connection failed: {e}")


def sftp_remote_size_bytes(remote, remote_path, password=None, verbose=False):
    """Get the size of a remote file via SFTP."""
    connection, sftp = sftp_connect(remote, password, verbose)
    
    try:
        stat = sftp.stat(remote_path)
        return stat.st_size
    except FileNotFoundError:
        raise FileNotFoundError(f"Remote file not found: {remote_path}")
    finally:
        sftp.close()
        if hasattr(connection, 'close'):
            connection.close()


def compute_remote_md5_sftp(remote, remote_path, password=None, verbose=False):
    """Compute MD5 hash of remote file via SFTP."""
    import hashlib
    
    connection, sftp = sftp_connect(remote, password, verbose)
    
    try:
        md5_hash = hashlib.md5()
        with sftp.open(remote_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                md5_hash.update(chunk)
        return md5_hash.hexdigest()
    finally:
        sftp.close()
        if hasattr(connection, 'close'):
            connection.close()


def should_download_sftp(remote, remote_path, stored_md5, password=None, verbose=False):
    """Check if file needs to be downloaded by comparing MD5 hashes."""
    try:
        remote_md5 = compute_remote_md5_sftp(remote, remote_path, password, verbose)
        return remote_md5 != stored_md5
    except Exception as e:
        print(f" -- Warning: Could not compute remote MD5: {e}")
        return True


def sftp_download_file(remote, remote_path, local_path, password=None, verbose=False):
    """Download a file via SFTP."""
    connection, sftp = sftp_connect(remote, password, verbose)
    
    try:
        # Create local directory if needed
        local_dir = Path(local_path)
        local_dir.mkdir(parents=True, exist_ok=True)
        
        # Get filename from remote path
        filename = Path(remote_path).name
        local_file = local_dir / filename
        
        # Download the file
        print(f" -- Downloading {filename}...")
        sftp.get(remote_path, str(local_file))
        print(f" -- Downloaded successfully")
        
        return str(local_file)
        
    finally:
        sftp.close()
        if hasattr(connection, 'close'):
            connection.close()