import os
import sys
import time
import argparse
import RNS
import hashlib
import json
import tarfile
import threading
import colors

from common import (
    APP_NAME,
    METADATA_ASPECT,
    FETCH_ASPECT,
    METADATA_FILENAME,
    METADATA_FORMAT_VERSION
)

serve_path = None
package_metadata_cache = {}
auto_announce_interval = None
auto_announce_thread = None
exit_event_host = threading.Event()

def calculate_content_hash(filepath):
    """Calculates a hash based on the content of a file."""
    hasher = hashlib.sha256()
    try:
        with open(filepath, 'rb') as f:
            while True:
                chunk = f.read(4096)
                if not chunk:
                    break
                hasher.update(chunk)
        return hasher.hexdigest()
    except OSError as e:
        RNS.log(f"Error reading file {filepath} for content hashing: {e}", RNS.LOG_ERROR)
        return None
    except Exception as e:
        RNS.log(f"Unexpected error hashing file {filepath}: {e}", RNS.LOG_ERROR)
        return None

def determine_package_type(filename):
    """Determine package type based on filename extension."""
    if filename.endswith(".tar.gz") or filename.endswith(".tgz"):
        return "tar.gz"
    if filename.endswith(".whl"):
        return "wheel"
    if filename.endswith(".apk"):
        return "apk"
    if '.' not in os.path.basename(filename):
        return "binary"
    return "unknown"

def generate_package_metadata(package_dir):
    """Generates enhanced metadata for packages in the directory."""
    metadata_v1 = {
        "format_version": METADATA_FORMAT_VERSION,
        "packages": {}
    }
    if not os.path.isdir(package_dir):
        RNS.log(f"Package directory '{package_dir}' not found or is not a directory.", RNS.LOG_ERROR)
        return metadata_v1

    for filename in os.listdir(package_dir):
        filepath = os.path.join(package_dir, filename)
        if not os.path.isfile(filepath):
            continue

        pkg_type = determine_package_type(filename)
        if pkg_type == "unknown":
             RNS.log(f"Skipping {filename}: Unknown package type.", RNS.LOG_VERBOSE)
             continue

        base, _ = os.path.splitext(filename)
        if pkg_type == "tar.gz":
            base = filename[:-len(".tar.gz")]
        elif pkg_type == "wheel":
             parts = filename.split('-')
             if len(parts) < 5:
                 RNS.log(f"Skipping wheel {filename}: Cannot parse standard name parts.", RNS.LOG_VERBOSE)
                 continue
             name = parts[0]
             version = parts[1]
        elif pkg_type == "apk":
             base = filename[:-len(".apk")]
             parts = base.split('-')
             if len(parts) < 2:
                  name = base
                  version = "unknown"
             else:
                  name = '-'.join(parts[:-1])
                  version = parts[-1]
        elif pkg_type == "binary":
             parts = filename.split('-')
             if len(parts) >= 2:
                  name = '-'.join(parts[:-1])
                  version = parts[-1]
             else:
                  name = filename
                  version = "unknown"
        else:
             continue

        try:
            archive_hasher = hashlib.sha256()
            file_size = os.path.getsize(filepath)

            with open(filepath, 'rb') as f:
                while True:
                    chunk = f.read(4096)
                    if not chunk:
                        break
                    archive_hasher.update(chunk)
            archive_hash = archive_hasher.hexdigest()

            content_hash = calculate_content_hash(filepath)
            if not content_hash:
                continue

            dependencies = []

            if name not in metadata_v1["packages"]:
                metadata_v1["packages"][name] = []

            metadata_v1["packages"][name].append({
                "version": version,
                "package_type": pkg_type,
                "archive_filename": filename,
                "archive_hash": archive_hash,
                "archive_size": file_size,
                "store_hash": content_hash,
                "dependencies": dependencies
            })
        except OSError as e:
            RNS.log(f"Error reading file {filename}: {e}", RNS.LOG_ERROR)
        except tarfile.TarError as e:
             RNS.log(f"Error processing tar archive {filename}: {e}", RNS.LOG_ERROR)
        except Exception as e:
             RNS.log(f"Error processing file {filename}: {e}", RNS.LOG_CRITICAL)

    for name in metadata_v1["packages"]:
        metadata_v1["packages"][name].sort(key=lambda x: x['version'], reverse=True)

    return metadata_v1

def load_metadata_from_file():
    """Loads metadata from the JSON file into the cache."""
    global package_metadata_cache
    if os.path.exists(METADATA_FILENAME):
        try:
            with open(METADATA_FILENAME, 'r') as f:
                loaded_data = json.load(f)
                if isinstance(loaded_data, dict) and loaded_data.get("format_version") == METADATA_FORMAT_VERSION:
                    package_metadata_cache = loaded_data
                    RNS.log(f"Loaded package metadata (v{METADATA_FORMAT_VERSION}) from {METADATA_FILENAME}")
                else:
                    RNS.log(f"Metadata file {METADATA_FILENAME} has incompatible format. Regenerating.", RNS.LOG_WARNING)
                    package_metadata_cache = {}
        except json.JSONDecodeError as e:
            RNS.log(f"Error decoding JSON from {METADATA_FILENAME}: {e}. Will regenerate.", RNS.LOG_ERROR)
            package_metadata_cache = {}
        except OSError as e:
            RNS.log(f"OS error loading metadata from {METADATA_FILENAME}: {e}. Will regenerate.", RNS.LOG_ERROR)
            package_metadata_cache = {}
        except Exception as e:
            RNS.log(f"Unexpected error loading metadata from {METADATA_FILENAME}: {e}. Will regenerate.", RNS.LOG_ERROR)
            package_metadata_cache = {}
    else:
        RNS.log(f"{METADATA_FILENAME} not found. Will generate initial metadata.")
        package_metadata_cache = {}

def save_metadata_to_file():
    """Saves the current in-memory cache to the JSON file."""
    global package_metadata_cache
    try:
        with open(METADATA_FILENAME, 'w') as f:
            json.dump(package_metadata_cache, f, indent=4)
        RNS.log(f"Saved package metadata to {METADATA_FILENAME}")
    except OSError as e:
        RNS.log(f"OS error saving metadata to {METADATA_FILENAME}: {e}", RNS.LOG_ERROR)
    except Exception as e:
        RNS.log(f"Unexpected error saving metadata to {METADATA_FILENAME}: {e}", RNS.LOG_ERROR)

def update_metadata_cache():
    """Generates, updates the in-memory cache, and saves metadata to file."""
    global package_metadata_cache, serve_path
    RNS.log("Updating package metadata cache...")
    package_metadata_cache = generate_package_metadata(serve_path)
    save_metadata_to_file()
    RNS.log(f"Metadata cache updated. Found metadata for {len(package_metadata_cache.get('packages', {}))} packages.")

def host(configpath, path, announce_interval=None):
    """Sets up the host and announces destinations."""
    global serve_path, auto_announce_interval
    serve_path = os.path.abspath(path)
    auto_announce_interval = announce_interval
    RNS.log(f"Serving packages from: {serve_path}")
    if auto_announce_interval:
         RNS.log(f"Automatic announcing enabled every {auto_announce_interval} seconds.")

    # Initialize Reticulum - needed for Identity, Destination, Transport, etc.
    RNS.Reticulum(configpath)
    # The instance is stored globally by RNS, no need to assign here if not used directly
    # reticulum = RNS.Reticulum(configpath) 

    # Load or create a persistent identity for the host
    identity_path = os.path.join(RNS.Reticulum.configdir, APP_NAME, "identity")
    host_identity = None
    if os.path.exists(identity_path):
        try:
            host_identity = RNS.Identity.from_file(identity_path)
            if host_identity:
                 RNS.log(f"Loaded host identity {host_identity} from {identity_path}")
            else:
                 RNS.log(f"Failed to load host identity from {identity_path}, file might be corrupt?", RNS.LOG_ERROR)
                 try:
                    os.remove(identity_path)
                 except OSError as del_e:
                    RNS.log(f"Could not remove corrupt identity file: {del_e}", RNS.LOG_ERROR)

        except Exception as e:
            RNS.log(f"Could not load identity from {identity_path}: {e}", RNS.LOG_ERROR)
            try:
                os.remove(identity_path)
            except OSError as del_e:
                RNS.log(f"Could not remove corrupt identity file: {del_e}", RNS.LOG_ERROR)

    if not host_identity:
        RNS.log("Generating new host identity...")
        try:
            os.makedirs(os.path.dirname(identity_path), exist_ok=True)
            host_identity = RNS.Identity()
            if host_identity.to_file(identity_path):
                 RNS.log(f"Created new host identity {host_identity} and saved to {identity_path}")
            else:
                 RNS.log(f"Failed to save new host identity to {identity_path}", RNS.LOG_ERROR)
                 host_identity = RNS.Identity()
                 RNS.log(f"Warning: Using ephemeral host identity {host_identity} as saving failed.")
        except Exception as e:
            RNS.log(f"Could not create/save host identity file {identity_path}: {e}", RNS.LOG_CRITICAL)
            host_identity = RNS.Identity()
            RNS.log(f"Warning: Using ephemeral host identity {host_identity} due to creation error.")

    if not host_identity:
         RNS.log("CRITICAL: Could not load or create host identity. Exiting.", RNS.LOG_CRITICAL)
         print(colors.error("Error: Failed to initialize host identity. Check permissions and configuration."), file=sys.stderr)
         sys.exit(1)

    load_metadata_from_file()
    if not package_metadata_cache:
        update_metadata_cache()

    metadata_destination = RNS.Destination(
        host_identity,
        RNS.Destination.IN,
        RNS.Destination.SINGLE,
        APP_NAME,
        METADATA_ASPECT
    )
    metadata_destination.register_request_handler("/", response_generator=metadata_request_handler, allow=RNS.Destination.ALLOW_ALL)
    metadata_destination.set_link_established_callback(metadata_link_established_log)
    RNS.log(f"Listening for metadata requests (v{METADATA_FORMAT_VERSION}) on {RNS.prettyhexrep(metadata_destination.hash)}")

    download_destination = RNS.Destination(
        host_identity,
        RNS.Destination.IN,
        RNS.Destination.SINGLE,
        APP_NAME,
        FETCH_ASPECT
    )
    download_destination.set_link_established_callback(client_connected)
    RNS.log(f"Listening for fetch connections (v{METADATA_FORMAT_VERSION}) on {RNS.prettyhexrep(download_destination.hash)}")

    announceLoop(metadata_destination, download_destination)

def periodic_announcer(metadata_dest, download_dest, interval):
    """Thread target to send announces periodically."""
    RNS.log("Starting periodic announcer thread...")
    while not exit_event_host.is_set():
        try:
            metadata_dest.announce()
            RNS.log(f"[Auto] Sent announce for metadata destination {RNS.prettyhexrep(metadata_dest.hash)}", RNS.LOG_VERBOSE)
            exit_event_host.wait(0.2)
            if exit_event_host.is_set(): 
                break

            download_dest.announce()
            RNS.log(f"[Auto] Sent announce for fetch destination {RNS.prettyhexrep(download_dest.hash)}", RNS.LOG_VERBOSE)
            exit_event_host.wait(interval - 0.2) 
        except Exception as e:
            RNS.log(f"Error in periodic announcer thread: {e}", RNS.LOG_ERROR)
            exit_event_host.wait(interval)

    RNS.log("Periodic announcer thread stopped.")

def announceLoop(metadata_dest, download_dest):
    """Handles announcing and user interaction."""
    global auto_announce_thread
    RNS.log(f"Package host {RNS.prettyhexrep(metadata_dest.hash)} (metadata) / {RNS.prettyhexrep(download_dest.hash)} (fetch) running")
    RNS.log("Serving packages from: "+serve_path)

    if auto_announce_interval:
         print(f"Auto-announcing every {colors.info(str(auto_announce_interval))} seconds. {colors.prompt('Press Ctrl+C to quit.')}")
         auto_announce_thread = threading.Thread(
             target=periodic_announcer,
             args=(metadata_dest, download_dest, auto_announce_interval),
             daemon=True
         )
         auto_announce_thread.start()
    else:
        print(colors.prompt("Manual announces enabled. Hit enter to announce, Ctrl+C to quit."))

    while not exit_event_host.is_set():
        try:
            if auto_announce_interval:
                time.sleep(1)
            else:
                entered = input()
                if entered.lower() == 'update':
                    update_metadata_cache()
                else:
                    metadata_dest.announce()
                    RNS.log(f"[Manual] Sent announce for metadata destination {RNS.prettyhexrep(metadata_dest.hash)}")
                    time.sleep(0.1)
                    download_dest.announce()
                    RNS.log(f"[Manual] Sent announce for fetch destination {RNS.prettyhexrep(download_dest.hash)}")
        except EOFError:
            if not auto_announce_interval:
                 RNS.log("Input closed, entering passive mode (Ctrl+C to quit).", RNS.LOG_NOTICE)
                 while not exit_event_host.is_set():
                     time.sleep(1)
            else:
                time.sleep(1)
        except KeyboardInterrupt:
            RNS.log("Shutdown requested by user...")
            exit_event_host.set()
            break
        except Exception as e:
            RNS.log(f"Error in main announce loop: {e}", RNS.LOG_ERROR)
            time.sleep(1)

    if auto_announce_thread and auto_announce_thread.is_alive():
        RNS.log("Waiting for announcer thread to stop...")
        auto_announce_thread.join(timeout=2)

    RNS.log("Host shutdown complete.")

def metadata_request_handler(path, data, request_id, link_id, remote_identity, requested_at):
    """Handles incoming metadata requests."""
    remote_id_str = "Unknown"
    if remote_identity:
        remote_id_str = RNS.prettyhexrep(remote_identity.hash)
    else:
        RNS.log("Received metadata request but remote_identity was None.", RNS.LOG_WARNING)

    RNS.log(f"Received metadata request {request_id[:6]}... from {remote_id_str}")
    global package_metadata_cache
    try:
        if not package_metadata_cache or package_metadata_cache.get("format_version") != METADATA_FORMAT_VERSION:
             RNS.log("Metadata cache empty or wrong format, attempting to update...", RNS.LOG_WARNING)
             update_metadata_cache()

        if package_metadata_cache and package_metadata_cache.get("format_version") == METADATA_FORMAT_VERSION:
            metadata_json = json.dumps(package_metadata_cache).encode('utf-8')
            RNS.log(f"Sending metadata response (v{METADATA_FORMAT_VERSION}, size {len(metadata_json)}) for {request_id[:6]}...")
            return metadata_json
        msg = "Metadata cache empty or invalid after update attempt."
        RNS.log(msg, RNS.LOG_ERROR)
        return json.dumps({"error": msg}).encode('utf-8')
    except json.JSONDecodeError as e:
        msg = f"Metadata cache file {METADATA_FILENAME} is corrupt."
        RNS.log(f"{msg} Error: {e}", RNS.LOG_ERROR)
        return json.dumps({"error": msg}).encode('utf-8')
    except Exception as e:
        msg = "Internal server error processing metadata request."
        RNS.log(f"Error handling metadata request {request_id[:6]}...: {e}", RNS.LOG_ERROR)
        try:
             error_msg = json.dumps({"error": msg}).encode('utf-8')
             return error_msg
        except Exception as resp_e:
             RNS.log(f"Failed to send error response for request {request_id[:6]}...: {resp_e}", RNS.LOG_ERROR)
             return None

def metadata_link_established_log(link):
    """Logs when a link is established to the metadata destination."""
    RNS.log(f"Link established to METADATA destination by {RNS.prettyhexrep(link.hash)}. Expecting request...")

def client_connected(link):
    """Callback when a client connects to the FETCH destination."""
    RNS.log(f"Client {RNS.prettyhexrep(link.hash)} connected for fetch.")
    if not os.path.isdir(serve_path):
        RNS.log("Served path no longer exists! Tearing down link.", RNS.LOG_ERROR)
        link.teardown()
        return

    link.set_link_closed_callback(client_disconnected)
    link.set_packet_callback(client_fetch_request)
    RNS.log(f"Waiting for fetch request from {RNS.prettyhexrep(link.hash)}")

def client_disconnected(link):
    """Logs client disconnections."""
    reason_str = {
        0: "locally closed",
        1: "timed out",
        2: "closed by remote",
    }.get(link.teardown_reason, f"unknown reason code {link.teardown_reason}")
    RNS.log(f"Client {RNS.prettyhexrep(link.hash)} disconnected ({reason_str}).")

def client_fetch_request(message, packet):
    """Handles incoming package archive requests."""
    global serve_path, package_metadata_cache
    try:
        request_data = json.loads(message.decode('utf-8'))
        pkg_name = request_data.get('name')
        pkg_version = request_data.get('version')
        RNS.log(f"Client {RNS.prettyhexrep(packet.link.hash)} requested package: {pkg_name}, version: {pkg_version}")
    except Exception as e:
        RNS.log(f"Invalid fetch request format from {RNS.prettyhexrep(packet.link.hash)}: {e}. Tearing down link.", RNS.LOG_ERROR)
        packet.link.teardown()
        return

    if not pkg_name or not pkg_version:
        RNS.log(f"Malformed fetch request (missing name or version) from {RNS.prettyhexrep(packet.link.hash)}. Tearing down link.", RNS.LOG_WARNING)
        packet.link.teardown()
        return

    package_info = None
    if package_metadata_cache and pkg_name in package_metadata_cache.get("packages", {}):
        versions = package_metadata_cache["packages"][pkg_name]
        for v_info in versions:
            if v_info['version'] == pkg_version:
                package_info = v_info
                break

    if package_info:
        filename = package_info['archive_filename']
        filepath = os.path.join(serve_path, filename)
        expected_hash = package_info['archive_hash']
        file_size = package_info['archive_size']

        if os.path.isfile(filepath):
            try:
                hasher = hashlib.sha256()
                with open(filepath, 'rb') as f_verify:
                    while True:
                        chunk = f_verify.read(4096)
                        if not chunk:
                            break
                        hasher.update(chunk)
                disk_hash = hasher.hexdigest()

                if disk_hash != expected_hash:
                    RNS.log(f"Archive hash mismatch for {filename} on disk! Expected {expected_hash[:8]}, got {disk_hash[:8]}. Aborting transfer.", RNS.LOG_ERROR)
                    packet.link.teardown()
                    return
                if os.path.getsize(filepath) != file_size:
                    RNS.log(f"Archive size mismatch for {filename} on disk! Expected {file_size}, got {os.path.getsize(filepath)}. Aborting transfer.", RNS.LOG_ERROR)
                    packet.link.teardown()
                    return

            except Exception as e_verify:
                RNS.log(f"Error verifying {filename} on disk before sending: {e_verify}. Aborting transfer.", RNS.LOG_ERROR)
                packet.link.teardown()
                return

            # Send the file as a resource
            file = None
            try:
                RNS.log(f"Sending archive \"{filename}\" ({size_str(file_size)}) to {RNS.prettyhexrep(packet.link.hash)}")
                file = open(filepath, "rb")

                file_resource = RNS.Resource(
                    file,
                    packet.link,
                    callback=resource_sending_concluded,
                )
                file_resource.package_info = package_info
            except OSError as e:
                 RNS.log(f"OS Error opening file \"{filename}\": {e}. Tearing down link.", RNS.LOG_ERROR)
                 if file and not file.closed: 
                     file.close()
                 packet.link.teardown()
            except Exception as e:
                RNS.log(f"Error creating resource for \"{filename}\": {e}. Tearing down link.", RNS.LOG_ERROR)
                if file and not file.closed: 
                    file.close()
                packet.link.teardown()
        else:
            RNS.log(f"Archive '{filename}' for requested package {pkg_name}-{pkg_version} not found on disk. Tearing down link.", RNS.LOG_ERROR)
            packet.link.teardown()
    else:
        RNS.log(f"Client {RNS.prettyhexrep(packet.link.hash)} requested unknown package/version: {pkg_name}-{pkg_version}. Tearing down link.", RNS.LOG_WARNING)
        packet.link.teardown()

def resource_sending_concluded(resource):
    """Handles resource sending completion."""
    pkg_info = getattr(resource, 'package_info', {'archive_filename': 'unknown file', 'name': 'unknown', 'version': 'unknown'})
    filename = pkg_info.get('archive_filename', 'unknown file')
    link_hash = RNS.prettyhexrep(resource.link.hash) if resource.link else 'N/A'

    if resource.status == RNS.Resource.COMPLETE:
        RNS.log(f"Successfully sent '{filename}' to client {link_hash}.")
    elif resource.status == RNS.Resource.FAILED:
        RNS.log(f"Sending '{filename}' to client {link_hash} failed.", RNS.LOG_ERROR)
    elif resource.status == RNS.Resource.TIMEOUT:
         RNS.log(f"Sending '{filename}' to client {link_hash} timed out.", RNS.LOG_WARNING)
    else:
        RNS.log(f"Sending '{filename}' to client {link_hash} concluded with status {resource.status}", RNS.LOG_WARNING)

    if hasattr(resource, 'data') and hasattr(resource.data, 'close') and not resource.data.closed:
        try:
            resource.data.close()
            RNS.log(f"Closed file handle for '{filename}'", RNS.LOG_VERBOSE)
        except Exception as e:
            RNS.log(f"Error closing file handle for resource '{filename}': {e}", RNS.LOG_WARNING)

def size_str(num, suffix='B'):
    """Converts bytes to human-readable string."""
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
        parser = argparse.ArgumentParser(description="Reticulum Package Host")

        parser.add_argument(
            "packagedir",
            action="store",
            metavar="directory",
            help="Directory containing package archive files (.tar.gz) to serve"
        )

        parser.add_argument(
            "--config",
            action="store",
            default=None,
            help="Path to alternative Reticulum config directory",
            type=str
        )

        parser.add_argument(
            "-aa", "--auto-announce",
            metavar='SECONDS',
            type=int,
            nargs='?',
            const=600,
            default=None,
            help="Enable automatic announcing every SECONDS (default: 600 if specified without value)"
        )

        args = parser.parse_args()

        configarg = args.config

        if os.path.isdir(args.packagedir):
            host(configarg, args.packagedir, args.auto_announce)
        else:
            print(colors.error(f"Error: The specified package directory '{args.packagedir}' does not exist or is not a directory."), file=sys.stderr)
            sys.exit(1)

    except KeyboardInterrupt:
        print(f"\n{colors.warning('Host stopped by user.')}")
        sys.exit(0)
    except Exception as e:
        print(f"\n{colors.error('An unexpected error occurred:')} {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(2)
