import os
import sys
import time
import threading
import argparse
import RNS
import RNS.vendor.umsgpack as umsgpack
import hashlib
import json

from common import APP_NAME, DEFAULT_TIMEOUT, PACKAGE_METADATA_ASPECT, PACKAGE_DOWNLOAD_ASPECT, METADATA_FILENAME

serve_path = None
package_metadata_cache = {}

def generate_package_metadata(package_dir):
    """Generates metadata (name, version, hash) for packages in the directory."""
    metadata = {}
    if not os.path.isdir(package_dir):
        RNS.log(f"Package directory '{package_dir}' not found or is not a directory.", RNS.LOG_ERROR)
        return metadata

    for filename in os.listdir(package_dir):
        filepath = os.path.join(package_dir, filename)
        if os.path.isfile(filepath):
            # Basic naming convention: package_name-version.ext
            # You might want a more robust way to handle versions
            base, ext = os.path.splitext(filename)
            parts = base.split('-')
            if len(parts) >= 2:
                name = '-'.join(parts[:-1])
                version = parts[-1]
            else:
                name = base
                version = "unknown" # Or handle files without version differently

            hasher = hashlib.sha256()
            try:
                with open(filepath, 'rb') as f:
                    while True:
                        chunk = f.read(4096)
                        if not chunk:
                            break
                        hasher.update(chunk)
                file_hash = hasher.hexdigest()

                if name not in metadata:
                    metadata[name] = []
                metadata[name].append({
                    "version": version,
                    "filename": filename,
                    "hash": file_hash,
                    "size": os.path.getsize(filepath)
                })
            except OSError as e:
                RNS.log(f"Error reading file {filename}: {e}", RNS.LOG_ERROR)
            except Exception as e:
                 RNS.log(f"Error processing file {filename}: {e}", RNS.LOG_CRITICAL)

    # Sort versions for each package (optional but good practice)
    for name in metadata:
        metadata[name].sort(key=lambda x: x['version'], reverse=True) # Basic version sort

    return metadata

def load_metadata_from_file():
    """Loads metadata from the JSON file into the cache."""
    global package_metadata_cache
    if os.path.exists(METADATA_FILENAME):
        try:
            with open(METADATA_FILENAME, 'r') as f:
                package_metadata_cache = json.load(f)
            RNS.log(f"Loaded package metadata from {METADATA_FILENAME}")
        except Exception as e:
            RNS.log(f"Error loading metadata from {METADATA_FILENAME}: {e}. Will regenerate.", RNS.LOG_ERROR)
            package_metadata_cache = {} # Reset cache on load error
    else:
        RNS.log(f"{METADATA_FILENAME} not found. Will generate initial metadata.")
        package_metadata_cache = {}

def save_metadata_to_file():
    """Saves the current in-memory cache to the JSON file."""
    global package_metadata_cache
    try:
        with open(METADATA_FILENAME, 'w') as f:
            json.dump(package_metadata_cache, f, indent=4) # Use indent for readability
        RNS.log(f"Saved package metadata to {METADATA_FILENAME}")
    except Exception as e:
        RNS.log(f"Error saving metadata to {METADATA_FILENAME}: {e}", RNS.LOG_ERROR)

def update_metadata_cache():
    """Generates, updates the in-memory cache, and saves metadata to file."""
    global package_metadata_cache, serve_path
    RNS.log("Updating package metadata cache...")
    package_metadata_cache = generate_package_metadata(serve_path)
    save_metadata_to_file() # Save after generating
    RNS.log(f"Metadata cache updated. Found metadata for {len(package_metadata_cache)} packages.")

def server(configpath, path):
    global serve_path
    serve_path = os.path.abspath(path)
    RNS.log(f"Serving packages from: {serve_path}")

    reticulum = RNS.Reticulum(configpath)

    # Create a unique identity for the package server
    # Consider persisting this identity for long-term use
    server_identity = RNS.Identity()
    RNS.log(f"Generated server identity: {server_identity}")

    # Load existing metadata or generate if needed
    load_metadata_from_file()
    if not package_metadata_cache: # If loading failed or file didn't exist
        update_metadata_cache() # Generate and save initial metadata

    # Destination for clients to request package metadata
    metadata_destination = RNS.Destination(
        server_identity,
        RNS.Destination.IN,
        RNS.Destination.SINGLE, # Expect requests on this aspect
        APP_NAME,
        PACKAGE_METADATA_ASPECT
    )
    metadata_destination.register_request_handler("/", response_generator=metadata_request_handler, allow=RNS.Destination.ALLOW_ALL)
    metadata_destination.set_link_established_callback(metadata_link_established_log)
    RNS.log(f"Listening for metadata requests on {RNS.prettyhexrep(metadata_destination.hash)}")

    # Destination for clients to establish download links
    download_destination = RNS.Destination(
        server_identity,
        RNS.Destination.IN,
        RNS.Destination.SINGLE,
        APP_NAME,
        PACKAGE_DOWNLOAD_ASPECT
    )
    download_destination.set_link_established_callback(client_connected)
    RNS.log(f"Listening for download connections on {RNS.prettyhexrep(download_destination.hash)}")

    # Announce both destinations
    announceLoop(metadata_destination, download_destination)

def announceLoop(metadata_dest, download_dest):
    RNS.log(f"Package server {RNS.prettyhexrep(metadata_dest.hash)} (metadata) / {RNS.prettyhexrep(download_dest.hash)} (download) running")
    RNS.log("Serving packages from: "+serve_path)
    RNS.log("Hit enter to manually send announces (Ctrl-C to quit)")

    while True:
        try:
            entered = input()
            if entered.lower() == 'update': # Add command to refresh cache
                update_metadata_cache() # This now saves to file too
            else:
                metadata_dest.announce()
                RNS.log(f"Sent announce for metadata destination {RNS.prettyhexrep(metadata_dest.hash)}")
                time.sleep(0.1) # Small delay between announces
                download_dest.announce()
                RNS.log(f"Sent announce for download destination {RNS.prettyhexrep(download_dest.hash)}")
        except EOFError:
            # Handle case where input stream is closed (e.g., piping)
            time.sleep(1)
        except KeyboardInterrupt:
            RNS.log("Shutting down server...")
            break

def metadata_request_handler(path, data, request_id, link_id, remote_identity, requested_at):
    """Handles incoming requests for the package list from the cache."""
    remote_id_str = "Unknown"
    if remote_identity:
        remote_id_str = RNS.prettyhexrep(remote_identity.hash)
    else:
        RNS.log("Received metadata request but remote_identity was None.", RNS.LOG_WARNING)

    RNS.log(f"Received metadata request {request_id[:6]}... from {remote_id_str}")
    global package_metadata_cache # Read from the global cache
    try:
        # Check if cache is populated (should be after startup logic)
        if not package_metadata_cache:
             RNS.log("Metadata cache is empty, attempting to update...", RNS.LOG_WARNING)
             update_metadata_cache() # Try to regenerate/load if empty

        if package_metadata_cache:
            metadata_json = json.dumps(package_metadata_cache).encode('utf-8')
            # RNS.Transport.respond(request_id, metadata_json) # Not needed for link requests
            RNS.log(f"Sending metadata response (for link request {request_id[:6]}...)...")
            return metadata_json # Return data directly for link requests
        else:
            # If still empty after trying to update, respond with error/empty
             RNS.log("Metadata cache remains empty after update attempt.", RNS.LOG_ERROR)
             # RNS.Transport.respond(request_id, b'{}') # Not needed for link requests
             return b'{}' # Return empty JSON for link request
    except Exception as e:
        RNS.log(f"Error handling metadata request {request_id[:6]}...: {e}", RNS.LOG_ERROR)
        # Optionally try sending an error response
        try:
             error_msg = json.dumps({"error": "Failed to process metadata request"}).encode('utf-8')
             # RNS.Transport.respond(request_id, error_msg) # Not needed for link requests
             return error_msg # Return error message for link request
        except Exception as resp_e:
             RNS.log(f"Failed to send error response for request {request_id[:6]}...: {resp_e}", RNS.LOG_ERROR)
             return None # Return None on catastrophic failure

def metadata_link_established_log(link):
    """Logs when a link is established to the metadata destination."""
    RNS.log(f"Link established to METADATA destination by {RNS.prettyhexrep(link.hash)}. Expecting request...")
    # We don't need to set packet/closed callbacks here, as the request handler handles the interaction

def client_connected(link):
    RNS.log(f"Client {RNS.prettyhexrep(link.hash)} connected for download.")
    if not os.path.isdir(serve_path):
        RNS.log("Served path no longer exists! Tearing down link.", RNS.LOG_ERROR)
        link.teardown()
        return

    link.set_link_closed_callback(client_disconnected)
    # Expect packet with requested filename and version
    link.set_packet_callback(client_download_request)
    # Set a timeout for the initial request from the client
    # link.set_link_watchdog_callback(link_watchdog_timeout) # This method doesn't exist on Link
    # link.watchdog_timeout = DEFAULT_TIMEOUT # This attribute doesn't exist without watchdog
    RNS.log(f"Waiting for download request from {RNS.prettyhexrep(link.hash)}")

# def link_watchdog_timeout(link): # No longer needed
#     RNS.log(f"Link {RNS.prettyhexrep(link.hash)} timed out waiting for download request. Tearing down.", RNS.LOG_WARNING)
#     link.teardown()

def client_disconnected(link):
    reason_str = {
        # Map known reason codes based on observed behavior or documentation
        # These might be guesses and need adjustment based on RNS internal constants
        0: "locally closed", 
        1: "timed out",
        2: "closed by remote",
        # RNS.Link.TIMEOUT: "timed out", # Attribute doesn't exist
        # RNS.Link.DESTINATION_CLOSED: "closed by remote", # Attribute doesn't exist
        # RNS.Link.CONNECTION_FAILED: "connection failed", # Attribute doesn't exist
        # RNS.Link.PACKET_TIMEOUT: "packet timeout", # Attribute doesn't exist
        # RNS.Link.RESOURCE_TIMEOUT: "resource timeout", # Attribute doesn't exist
        # RNS.Link.FAILED: "failed", # Attribute doesn't exist
        # RNS.Link.CLOSED: "closed" # Attribute doesn't exist
    }.get(link.teardown_reason, f"unknown reason code {link.teardown_reason}")
    RNS.log(f"Client {RNS.prettyhexrep(link.hash)} disconnected ({reason_str}).")

def client_download_request(message, packet):
    global serve_path, package_metadata_cache
    try:
        request_data = json.loads(message.decode('utf-8'))
        pkg_name = request_data.get('name')
        pkg_version = request_data.get('version') # Can be 'latest' or specific version
        RNS.log(f"Client {RNS.prettyhexrep(packet.link.hash)} requested package: {pkg_name}, version: {pkg_version}")
    except Exception as e:
        RNS.log(f"Invalid download request format from {RNS.prettyhexrep(packet.link.hash)}: {e}. Tearing down link.", RNS.LOG_ERROR)
        packet.link.teardown()
        return

    if not pkg_name or not pkg_version:
        RNS.log(f"Malformed download request (missing name or version) from {RNS.prettyhexrep(packet.link.hash)}. Tearing down link.", RNS.LOG_WARNING)
        packet.link.teardown()
        return

    package_info = None
    # Use the currently loaded cache, not regenerate on the fly
    if pkg_name in package_metadata_cache:
        versions = package_metadata_cache[pkg_name]
        if pkg_version == 'latest':
            if versions:
                package_info = versions[0]
        else:
            for v_info in versions:
                if v_info['version'] == pkg_version:
                    package_info = v_info
                    break

    if package_info:
        filename = package_info['filename']
        filepath = os.path.join(serve_path, filename)
        if os.path.isfile(filepath):
            try:
                RNS.log(f"Sending \"{filename}\" ({size_str(package_info['size'])}) to {RNS.prettyhexrep(packet.link.hash)}")
                file = open(filepath, "rb")

                file_resource = RNS.Resource(
                    file,
                    packet.link,
                    callback=resource_sending_concluded,
                )
                file_resource.package_info = package_info
            except Exception as e:
                RNS.log(f"Error opening or creating resource for \"{filename}\": {e}. Tearing down link.", RNS.LOG_ERROR)
                packet.link.teardown()
        else:
            RNS.log(f"File '{filename}' for requested package {pkg_name}-{pkg_version} not found on disk (metadata mismatch?). Tearing down link.", RNS.LOG_ERROR)
            packet.link.teardown()
    else:
        RNS.log(f"Client {RNS.prettyhexrep(packet.link.hash)} requested unknown package/version: {pkg_name}-{pkg_version}. Tearing down link.", RNS.LOG_WARNING)
        packet.link.teardown()

def resource_sending_concluded(resource):
    pkg_info = getattr(resource, 'package_info', {'filename': 'unknown file', 'name': 'unknown', 'version': 'unknown'})
    filename = pkg_info.get('filename', 'unknown file')
    link_hash = RNS.prettyhexrep(resource.link.hash) if resource.link else 'N/A'

    if resource.status == RNS.Resource.COMPLETE:
        RNS.log(f"Successfully sent \"{filename}\" to client {link_hash}.")
    elif resource.status == RNS.Resource.FAILED:
        RNS.log(f"Sending \"{filename}\" to client {link_hash} failed.", RNS.LOG_ERROR)
    elif resource.status == RNS.Resource.TIMEOUT:
         RNS.log(f"Sending \"{filename}\" to client {link_hash} timed out.", RNS.LOG_WARNING)
    else:
        RNS.log(f"Sending \"{filename}\" to client {link_hash} concluded with status {resource.status}", RNS.LOG_WARNING)

def size_str(num, suffix='B'):
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

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="Reticulum Network Stack Package Manager Server")

        parser.add_argument(
            "packagedir",
            action="store",
            metavar="directory",
            help="Directory containing package files to serve"
        )

        parser.add_argument(
            "--config",
            action="store",
            default=None,
            help="Path to alternative Reticulum config directory",
            type=str
        )

        args = parser.parse_args()

        configarg = args.config # Can be None

        if os.path.isdir(args.packagedir):
            server(configarg, args.packagedir)
        else:
            print(f"Error: The specified package directory '{args.packagedir}' does not exist or is not a directory.", file=sys.stderr)
            sys.exit(1)

    except KeyboardInterrupt:
        print("\nServer stopped by user.")
        sys.exit(0)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        # Consider more specific error handling/logging
        sys.exit(2) 