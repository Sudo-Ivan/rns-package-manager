import os
import sys
import time
import threading
import argparse
import RNS
import json
import hashlib
import math

from common import APP_NAME, DEFAULT_TIMEOUT, PACKAGE_METADATA_ASPECT, PACKAGE_DOWNLOAD_ASPECT

# Globals for client state
reticulum = None
server_metadata_dest = None
server_download_dest = None
server_metadata = None
metadata_link = None # Link specifically for metadata requests
server_link = None # Link for downloads
current_download = None
current_package_info = None
current_package_name = None # Add global for package name
exit_event = threading.Event()

# Download state tracking
download_started = 0
download_finished = 0
download_time = 0
transfer_size = 0
download_target_path = None

# ---- Utility Functions ----
def size_str(num, suffix='B'):
    # (Same as in server.py and filetransfer example)
    units = ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']
    last_unit = 'Yi'
    if suffix == 'b':
        num *= 8
        units = ['','K','M','G','T','P','E','Z']
        last_unit = 'Y'
    for unit in units:
        if abs(num) < 1024.0:
            return "%3.2f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.2f %s%s" % (num, last_unit, suffix)

def clear_screen():
    os.system('cls' if os.name=='nt' else 'clear')

def format_time(seconds):
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    return "{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),secs)

def parse_destination(dest_hash_hex):
    """Parses and validates a destination hash string."""
    try:
        dest_len = (RNS.Reticulum.TRUNCATED_HASHLENGTH//8)*2
        if len(dest_hash_hex) != dest_len:
            raise ValueError(
                f"Destination length is invalid, must be {dest_len} hexadecimal characters ({dest_len//2} bytes)."
            )
        return bytes.fromhex(dest_hash_hex)
    except ValueError as e:
        RNS.log(f"Invalid destination hash: {dest_hash_hex}. {e}", RNS.LOG_CRITICAL)
        return None
    except Exception as e:
        RNS.log(f"Error parsing destination hash '{dest_hash_hex}': {e}", RNS.LOG_CRITICAL)
        return None

def resolve_path(destination_hash, dest_name="destination"):
    """Resolves path to a destination, waiting if necessary."""
    if not RNS.Transport.has_path(destination_hash):
        RNS.log(f"{dest_name.capitalize()} is not yet known. Requesting path...")
        RNS.Transport.request_path(destination_hash)
        wait_start = time.time()
        while not RNS.Transport.has_path(destination_hash):
            if time.time() - wait_start > DEFAULT_TIMEOUT:
                RNS.log(f"Timed out waiting for path to {dest_name}.", RNS.LOG_ERROR)
                return False
            time.sleep(0.1)
        RNS.log(f"Path to {dest_name} obtained.")
    return True

# ---- Metadata Handling ----
def request_metadata():
    """Establishes a link and requests metadata from the server."""
    global server_metadata_dest, metadata_link
    if not server_metadata_dest:
        RNS.log("Server metadata destination not configured.", RNS.LOG_ERROR)
        return

    if metadata_link and metadata_link.status != RNS.Link.CLOSED:
        RNS.log("Metadata link already exists or is establishing.", RNS.LOG_INFO)
        # If already established, maybe resend request?
        if metadata_link.status == RNS.Link.ACTIVE:
             send_metadata_request_over_link(metadata_link)
        return

    RNS.log("Establishing link for metadata request...")
    metadata_link = RNS.Link(server_metadata_dest)
    metadata_link.set_link_established_callback(metadata_link_established)
    metadata_link.set_link_closed_callback(metadata_link_closed)
    # Set a watchdog for the metadata link
    metadata_link.watchdog_timeout = DEFAULT_TIMEOUT / 2

def metadata_link_established(link):
    """Callback when the metadata link is active. Sends the request."""
    RNS.log("Metadata link established. Sending request...")
    send_metadata_request_over_link(link)

def send_metadata_request_over_link(link):
    """Sends the actual metadata request packet over the provided link."""
    # Change the path to match exactly what's registered on the server
    link.request(
        path="/",  # Ensure this matches the server's registered path
        data=None, # No data needed for this request
        response_callback=metadata_response_handler,
        failed_callback=metadata_request_failed,
        timeout = DEFAULT_TIMEOUT / 2
    )

def metadata_response_handler(request_receipt):
    """Callback for successful metadata response received over the link."""
    global server_metadata, metadata_link
    RNS.log("Received metadata response from server.")
    try:
        metadata = json.loads(request_receipt.response.decode('utf-8'))
        if isinstance(metadata, dict):
            server_metadata = metadata
            RNS.log(f"Successfully parsed metadata for {len(server_metadata)} packages.")
        else:
            RNS.log("Received invalid metadata format (not a dictionary).", RNS.LOG_ERROR)
            server_metadata = {} # Indicate failure
    except json.JSONDecodeError:
        RNS.log("Failed to decode metadata response (invalid JSON).", RNS.LOG_ERROR)
        server_metadata = {} # Indicate failure
    except Exception as e:
        RNS.log(f"Error processing metadata response: {e}", RNS.LOG_ERROR)
        server_metadata = {} # Indicate failure
    finally:
        # Metadata request done, close the link
        if metadata_link:
            RNS.log("Closing metadata link.")
            metadata_link.teardown()
            metadata_link = None

def metadata_request_failed(request_receipt):
    """Callback for failed metadata request over the link."""
    global server_metadata, metadata_link
    RNS.log(f"Request for server metadata failed (status {request_receipt.status}) or timed out.", RNS.LOG_ERROR)
    server_metadata = {} # Indicate failure
    # Metadata request failed, close the link
    if metadata_link:
        RNS.log("Closing metadata link after failure.")
        metadata_link.teardown()
        metadata_link = None

def metadata_link_closed(link):
     global metadata_link
     RNS.log(f"Metadata link closed (Reason: {link.teardown_reason}).")
     if metadata_link == link:
          metadata_link = None
     # If metadata is still None here, it means the request likely failed entirely
     if server_metadata is None:
          RNS.log("Metadata link closed before receiving data.", RNS.LOG_WARNING)
          # Set metadata to {} to unblock the main loop waiting state
          # Otherwise client_main_loop could hang forever
          metadata_request_failed(None) # Simulate failure to set server_metadata to {}

# ---- Download Handling ----
def start_download(package_name, version_str, download_dir):
    """Initiates the download process for a specific package."""
    global server_metadata, server_download_dest, current_package_info, download_target_path, current_package_name

    if not server_metadata:
        RNS.log("Cannot start download: Server metadata not available.", RNS.LOG_ERROR)
        return

    if package_name not in server_metadata:
        RNS.log(f"Package '{package_name}' not found in server metadata.", RNS.LOG_ERROR)
        return

    versions = server_metadata[package_name]
    target_version_info = None
    if version_str.lower() == 'latest':
        if versions: # Already sorted descending by server
             target_version_info = versions[0]
        else:
             RNS.log(f"No versions found for package '{package_name}'.", RNS.LOG_ERROR)
             return
    else:
        for v_info in versions:
            if v_info['version'] == version_str:
                target_version_info = v_info
                break

    if not target_version_info:
        RNS.log(f"Version '{version_str}' for package '{package_name}' not found.", RNS.LOG_ERROR)
        return

    current_package_info = target_version_info
    current_package_name = package_name # Store the name
    filename = current_package_info['filename']
    download_target_path = os.path.join(download_dir, filename)

    # Check if file already exists
    if os.path.exists(download_target_path):
        # Simple check: if size matches, assume it's okay (could add hash check)
        if os.path.getsize(download_target_path) == current_package_info['size']:
             RNS.log(f"File '{filename}' already exists with correct size. Skipping download.")
             # Maybe verify hash here if desired
             return
        else:
             RNS.log(f"File '{filename}' already exists but size differs. Will overwrite.", RNS.LOG_WARNING)

    RNS.log(f"Preparing to download {package_name} version {current_package_info['version']} ({filename})...")
    establish_download_link()

def establish_download_link():
    """Creates the RNS Link to the server's download destination."""
    global server_download_dest, server_link
    if not server_download_dest:
        RNS.log("Cannot establish link: Server download destination not configured.", RNS.LOG_ERROR)
        return

    if server_link and server_link.status != RNS.Link.CLOSED:
        RNS.log("Download link already exists or is establishing.", RNS.LOG_INFO)
        # If already established, send the request packet
        if server_link.status == RNS.Link.ACTIVE:
            send_download_request_packet()
        return

    RNS.log("Establishing download link with server...")
    server_link = RNS.Link(server_download_dest)
    server_link.set_link_established_callback(download_link_established)
    server_link.set_link_closed_callback(download_link_closed)
    server_link.set_resource_callback(resource_received) # Generic resource callback
    server_link.set_resource_started_callback(download_began)
    server_link.set_resource_concluded_callback(download_concluded)
    server_link.set_remote_identified_callback(server_identified)
    # Set the link to accept incoming resources automatically
    server_link.set_resource_strategy(RNS.Link.ACCEPT_ALL)
    # Set a watchdog for the link itself (This seems to be unsupported or named differently)
    # server_link.watchdog_timeout = DEFAULT_TIMEOUT * 1.5 # Longer timeout for link

def server_identified(link, identity):
     # Optional: Could verify identity here if known/expected
     RNS.log(f"Server identity identified: {RNS.prettyhexrep(identity.hash)}")

def download_link_established(link):
    """Callback when the download link is active."""
    RNS.log("Download link established.")
    send_download_request_packet()

def send_download_request_packet():
    """Sends the packet requesting the specific package file."""
    global server_link, current_package_info, current_package_name
    if not server_link or server_link.status != RNS.Link.ACTIVE:
        RNS.log("Cannot send download request: Link not active.", RNS.LOG_ERROR)
        return
    if not current_package_info:
        RNS.log("Cannot send download request: No package selected.", RNS.LOG_ERROR)
        return

    request_data = {
        "name": current_package_name,
        "version": current_package_info['version']
    }
    try:
        request_json = json.dumps(request_data).encode('utf-8')
        RNS.log(f"Sending download request for {current_package_name}-{current_package_info['version']}")
        # Create a packet for the link and send it
        packet = RNS.Packet(server_link, request_json)
        receipt = packet.send() 
        if receipt:
            receipt.set_delivery_callback(download_request_delivered)
            receipt.set_timeout_callback(download_request_timeout)
            receipt.set_timeout(DEFAULT_TIMEOUT) # Set explicit timeout for this packet
        else:
            RNS.log("Failed to get packet receipt for download request.", RNS.LOG_ERROR)
            if server_link:
                server_link.teardown()

    except Exception as e:
        RNS.log(f"Error sending download request packet: {e}", RNS.LOG_ERROR)
        if server_link:
             server_link.teardown()

def download_request_delivered(receipt):
    RNS.log("Download request delivered to server. Waiting for resource transfer...")

def download_request_timeout(receipt):
    RNS.log("Download request timed out. Closing link.", RNS.LOG_ERROR)
    if server_link:
        server_link.teardown()

def resource_received(resource):
     # This callback is generally useful if the server advertises multiple resources
     # For our case, we mostly rely on started/concluded callbacks
     RNS.log(f"Resource advertised/received: {resource.file_path if hasattr(resource, 'file_path') else 'unknown'}")

def download_began(resource):
    """Callback when the resource transfer (download) starts."""
    global current_download, download_started, transfer_size
    current_download = resource
    download_started = time.time()
    transfer_size = resource.total_size # Should be total size of the resource
    RNS.log(f"Download started for {current_package_info['filename']} ({size_str(transfer_size)})...")

def download_concluded(resource):
    """Callback when the resource transfer finishes (successfully or not)."""
    global download_finished, download_time, current_download, current_package_info, download_target_path, server_link, current_package_name
    download_finished = time.time()
    download_time = download_finished - download_started

    if resource.status == RNS.Resource.COMPLETE:
        RNS.log(f"Resource transfer complete in {format_time(download_time)}.")
        expected_hash = current_package_info['hash']
        expected_size = current_package_info['size']
        filename = current_package_info['filename']

        try:
            # Verify hash and size
            verifier = hashlib.sha256()
            downloaded_size = 0
            resource.data.seek(0) # Ensure we read from the beginning
            while True:
                chunk = resource.data.read(4096)
                if not chunk:
                    break
                verifier.update(chunk)
                downloaded_size += len(chunk)
            resource.data.seek(0) # Reset seek for potential saving
            actual_hash = verifier.hexdigest()

            RNS.log(f"Verifying download: Size (Expected: {expected_size}, Got: {downloaded_size}), Hash (Expected: {expected_hash[:8]}..., Got: {actual_hash[:8]}...)")

            if downloaded_size != expected_size:
                RNS.log(f"Download failed: Size mismatch for '{filename}'.", RNS.LOG_ERROR)
                resource.status = RNS.Resource.FAILED # Mark as failed
            elif actual_hash != expected_hash:
                RNS.log(f"Download failed: Hash mismatch for '{filename}'. Potential corruption or tampering.", RNS.LOG_ERROR)
                resource.status = RNS.Resource.FAILED # Mark as failed
            else:
                RNS.log(f"Download verified successfully for '{filename}'.")
                # Save the file
                try:
                    os.makedirs(os.path.dirname(download_target_path), exist_ok=True)
                    with open(download_target_path, "wb") as f:
                        while True:
                            chunk = resource.data.read(65536) # Read in larger chunks for saving
                            if not chunk:
                                break
                            f.write(chunk)
                    RNS.log(f"Successfully saved package to: {download_target_path}")
                    print(f"\nPackage '{filename}' downloaded and verified.")
                except Exception as e:
                    RNS.log(f"Error saving downloaded file '{download_target_path}': {e}", RNS.LOG_ERROR)
                    print(f"\nError saving file: {e}")
                    resource.status = RNS.Resource.FAILED

        except Exception as e:
            RNS.log(f"Error during verification or saving: {e}", RNS.LOG_ERROR)
            resource.status = RNS.Resource.FAILED

    elif resource.status == RNS.Resource.FAILED:
        RNS.log("Download failed.", RNS.LOG_ERROR)
        print("\nDownload failed!")
    elif resource.status == RNS.Resource.TIMEOUT:
        RNS.log("Download timed out.", RNS.LOG_WARNING)
        print("\nDownload timed out!")
    else:
        RNS.log(f"Download concluded with unexpected status: {resource.status}", RNS.LOG_WARNING)
        print(f"\nDownload finished with status: {resource.status}")

    # Reset state and close link
    current_download = None
    current_package_info = None
    current_package_name = None # Reset name
    if server_link:
        server_link.teardown()
    # Signal the main loop to potentially redisplay menu
    exit_event.set() # Use event to break input loop

def download_link_closed(link):
    global server_link
    # Map known reason codes
    reason_map = { 
        0: "locally closed", # Assuming 0 might be normal close
        1: "timed out", # Guess based on usage elsewhere
        2: "closed by server", # Guess based on usage elsewhere
        # Add more known codes here if discovered
    }
    reason_str = reason_map.get(link.teardown_reason, f"reason code {link.teardown_reason}")
    RNS.log(f"Download link closed ({reason_str}).")
    if server_link == link:
        server_link = None # Clear the global link reference
    # If download wasn't finished, signal main loop
    if not current_download or current_download.status != RNS.Resource.COMPLETE:
         if not exit_event.is_set(): # Avoid setting multiple times if already concluding
              print(f"\nDownload link closed unexpectedly ({reason_str}).")
              exit_event.set()

# ---- User Interface ----
def display_packages():
    global server_metadata
    clear_screen()
    print("Available Packages:")
    print("-------------------")
    if server_metadata is None:
        print("  (Waiting for metadata from server...)")
        return False
    if not server_metadata:
        print("  (Metadata received, but no packages found or error occurred)")
        return False # Treat empty/error dict as nothing to show yet

    pkg_list = sorted(server_metadata.keys())
    for i, pkg_name in enumerate(pkg_list):
        versions = server_metadata[pkg_name]
        latest_version = versions[0] if versions else {'version': 'N/A', 'size': 0}
        print(f" {i+1:2d}. {pkg_name}")
        print(f"      Latest: {latest_version['version']} ({size_str(latest_version['size'])}) Hash: {latest_version.get('hash', 'N/A')[:8]}...")
        # Optionally list all versions:
        # for v_info in versions:
        #     print(f"      - {v_info['version']} ({size_str(v_info['size'])}) Hash: {v_info['hash'][:8]}...")
    print("-------------------")
    return True

def client_main_loop(download_dir):
    global server_metadata, exit_event

    # Wait for initial metadata retrieval (or failure)
    RNS.log("Waiting for initial metadata...")
    print("Requesting package list from server...")
    metadata_wait_start = time.time()
    while server_metadata is None:
        if time.time() - metadata_wait_start > (DEFAULT_TIMEOUT / 2) + 5: # Add grace period to link timeout
             RNS.log("Timed out waiting for initial metadata response.", RNS.LOG_CRITICAL)
             print("\nError: Could not retrieve package list from server (timeout).")
             # Set metadata to {} so loop can exit cleanly
             server_metadata = {}
             # No need to call exit_event.set() here, just break the wait
             break
        if exit_event.is_set(): # Check if reticulum exited early
             RNS.log("Exiting loop due to exit event during metadata wait.")
             return
        time.sleep(0.2)

    # Check if metadata failed (is empty dict)
    if not server_metadata:
         print("Failed to retrieve package list. Exiting.")
         return

    while not exit_event.is_set():
        if not display_packages(): # Should now return True if metadata dict exists
            # This case should ideally not happen after the initial wait
            print("Error displaying packages. Retrying metadata request...")
            server_metadata = None
            request_metadata() # Re-request
            time.sleep(2)
            continue

        print("\nEnter package number or name to view versions, or name@version to download.")
        print("Type 'refresh' to update metadata, 'q' to quit.")
        try:
            exit_event.clear() # Clear event before waiting for input
            user_input = input("> ").strip()

            if exit_event.is_set(): # Check if event was set during input (e.g., link closed)
                continue

            if not user_input:
                continue

            if user_input.lower() == 'q':
                RNS.log("User requested quit.")
                break

            if user_input.lower() == 'refresh':
                RNS.log("User requested metadata refresh.")
                server_metadata = None # Clear cache
                print("Refreshing metadata...")
                request_metadata() # Initiate new request
                metadata_wait_start = time.time()
                while server_metadata is None:
                    if time.time() - metadata_wait_start > (DEFAULT_TIMEOUT / 2) + 5:
                         RNS.log("Timed out waiting for refresh response.", RNS.LOG_ERROR)
                         print("\nError: Refresh timed out.")
                         server_metadata = {} # Mark as failed
                         break # Break inner wait loop
                    if exit_event.is_set(): break
                    time.sleep(0.2)
                continue # Go back to display/input loop

            selected_package = None
            selected_version = 'latest' # Default to latest if no version specified

            if '@' in user_input:
                parts = user_input.split('@', 1)
                name_part = parts[0]
                selected_version = parts[1] if parts[1] else 'latest'
            else:
                name_part = user_input

            try:
                # Check if input is a number (index)
                num_index = int(name_part) - 1
                pkg_list = sorted(server_metadata.keys())
                if 0 <= num_index < len(pkg_list):
                    selected_package = pkg_list[num_index]
                else:
                    print(f"Invalid package number: {name_part}")
                    time.sleep(1)
                    continue
            except ValueError:
                # Input is not a number, assume it's a package name
                pkg_list = sorted(server_metadata.keys())
                # Simple prefix matching or exact match
                matches = [p for p in pkg_list if p.startswith(name_part)]
                if len(matches) == 1:
                     selected_package = matches[0]
                elif len(matches) > 1:
                     print(f"Ambiguous package name '{name_part}'. Matches: {', '.join(matches)}")
                     time.sleep(2)
                     continue
                elif name_part in pkg_list:
                     selected_package = name_part
                else:
                    print(f"Package '{name_part}' not found.")
                    time.sleep(1)
                    continue
            except Exception as e:
                 print(f"Error processing input: {e}")
                 time.sleep(1)
                 continue

            if selected_package:
                # Start download logic (no changes)
                print(f"\nAttempting to download {selected_package}@{selected_version}...")
                start_download(selected_package, selected_version, download_dir)
                # Wait for download process (no changes)
                print("Waiting for download process... (Press Ctrl+C to attempt abort)")
                progress_update_time = time.time()
                try:
                     while not exit_event.is_set():
                         if current_download:
                             if time.time() - progress_update_time > 0.5:
                                 progress = current_download.get_progress()
                                 rate = current_download.get_rate()
                                 eta = current_download.get_eta()
                                 eta_str = format_time(eta) if eta is not None else "N/A" 
                                 print(f"\rDownloading: {progress*100:.1f}% ({size_str(rate)}/s, ETA: {eta_str})   ", end="")
                                 progress_update_time = time.time()
                         time.sleep(0.1)
                     print("\nDownload process finished or interrupted.") # Clear progress line
                except KeyboardInterrupt:
                     RNS.log("User interrupt during download wait.")
                     print("\nDownload interrupted by user.")
                     if server_link:
                         server_link.teardown()
                     # Allow loop to continue

        except KeyboardInterrupt:
            RNS.log("User interrupted main loop.")
            break
        except EOFError:
            RNS.log("EOF detected, exiting.")
            break # Exit loop if input stream closed

    RNS.log("Client main loop finished.")
    # Teardown any remaining links
    if server_link:
        server_link.teardown()
    if metadata_link:
        metadata_link.teardown()

# ---- Main Execution ----
def client(configpath, metadata_hash_hex, download_hash_hex, download_dir):
    global reticulum, server_metadata_dest, server_download_dest, exit_event

    metadata_hash = parse_destination(metadata_hash_hex)
    download_hash = parse_destination(download_hash_hex)

    if not metadata_hash or not download_hash:
        return # Error already logged

    reticulum = RNS.Reticulum(configpath)

    # Ensure download directory exists
    try:
        os.makedirs(download_dir, exist_ok=True)
        RNS.log(f"Using download directory: {download_dir}")
    except Exception as e:
        RNS.log(f"Could not create or access download directory '{download_dir}': {e}", RNS.LOG_CRITICAL)
        print(f"Error: Cannot use download directory '{download_dir}'. Check permissions.")
        reticulum = None # Prevent RNS from running further
        return

    # Resolve paths
    if not resolve_path(metadata_hash, "metadata server") or not resolve_path(download_hash, "download server"):
        reticulum = None # Prevent RNS from running further
        return

    # Create OUT destinations
    # We need the server's identity, which we get from recalling the hash
    try:
        # Assuming both destinations share the same underlying Identity
        server_identity = RNS.Identity.recall(metadata_hash)
        if not server_identity:
             # Try recalling from download hash if first failed (unlikely if paths resolved)
             server_identity = RNS.Identity.recall(download_hash)

        if not server_identity:
             RNS.log("Could not recall server Identity from provided hashes.", RNS.LOG_CRITICAL)
             print("Error: Failed to identify the server.")
             reticulum = None
             return

        RNS.log(f"Recalled server identity: {RNS.prettyhexrep(server_identity.hash)}")

        server_metadata_dest = RNS.Destination(
            server_identity,
            RNS.Destination.OUT,
            RNS.Destination.SINGLE,
            APP_NAME,
            PACKAGE_METADATA_ASPECT
        )

        server_download_dest = RNS.Destination(
            server_identity,
            RNS.Destination.OUT,
            RNS.Destination.SINGLE,
            APP_NAME,
            PACKAGE_DOWNLOAD_ASPECT
        )
    except Exception as e:
         RNS.log(f"Error creating destinations or recalling identity: {e}", RNS.LOG_CRITICAL)
         print(f"Error setting up communication destinations: {e}")
         reticulum = None
         return

    # Start metadata request by initiating the link establishment
    request_metadata()

    # Enter main loop
    client_main_loop(download_dir)

    # Shutdown Reticulum gracefully
    RNS.log("Shutting down client...")
    exit_event.set() # Signal any waiting threads
    # if reticulum: # Reticulum object has no shutdown method
    #      reticulum.shutdown()
    # Wait briefly for threads? Might not be necessary with reticulum.shutdown()

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="Reticulum Network Stack Package Manager Client")

        parser.add_argument(
            "metadata_hash",
            help="Hexadecimal hash of the server's metadata destination"
        )
        parser.add_argument(
            "download_hash",
            help="Hexadecimal hash of the server's download destination"
        )
        parser.add_argument(
            "-d", "--dir",
            default=".", # Default to current directory
            help="Directory to download packages into (default: current directory)"
        )
        parser.add_argument(
            "--config",
            action="store",
            default=None,
            help="Path to alternative Reticulum config directory",
            type=str
        )

        args = parser.parse_args()

        # Basic validation of download directory
        download_directory = os.path.abspath(args.dir)

        client(args.config, args.metadata_hash, args.download_hash, download_directory)

    except KeyboardInterrupt:
        print("\nClient interrupted by user.")
        if exit_event:
            exit_event.set() # Signal shutdown
        # Let shutdown proceed in client() or main loop handler
        time.sleep(0.5) # Give a moment for logs/shutdown
        sys.exit(0)
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}", file=sys.stderr)
        RNS.log(f"Client failed with unhandled exception: {e}", RNS.LOG_CRITICAL)
        # Log traceback here if desired for debugging
        if exit_event:
            exit_event.set()
        sys.exit(2) 