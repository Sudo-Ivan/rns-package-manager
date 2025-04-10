import os
import sys
import time
import threading
import argparse
import RNS
import json
import hashlib
import tarfile
import zipfile
import shutil
from . import colors

from .common import (
    APP_NAME, 
    DEFAULT_TIMEOUT, 
    METADATA_ASPECT, 
    FETCH_ASPECT,
    RNS_PKG_BASE_DIR, 
    RNS_STORE_DIR, 
    RNS_CACHE_DIR, 
    LOCAL_STATE_DB,
    HOST_PROFILES_DB,
    METADATA_FORMAT_VERSION
)

reticulum = None
host_metadata_dest = None
host_fetch_dest = None
host_metadata = None
metadata_link = None
fetch_link = None
current_fetch_resource = None
current_fetch_package_info = None
current_fetch_package_name = None
exit_event = threading.Event()

fetch_started = 0
fetch_finished = 0
fetch_time = 0
fetch_transfer_size = 0
fetch_target_path = None

def size_str(num, suffix='B'):
    """Return a human-readable string representation of a size."""
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

def format_time(seconds):
    """Format seconds into HH:MM:SS.SS string."""
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    return "{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),secs)

def parse_destination(dest_hash_hex):
    """Parse and validate a destination hash string."""
    try:
        dest_len = (RNS.Reticulum.TRUNCATED_HASHLENGTH//8)*2
        if len(dest_hash_hex) != dest_len:
            raise ValueError(
                f"Destination length is invalid, must be {dest_len} hexadecimal characters ({dest_len//2} bytes)."
            )
        return bytes.fromhex(dest_hash_hex)
    except ValueError as e:
        RNS.log(f"Invalid destination hash: {dest_hash_hex}. {e}", RNS.LOG_CRITICAL)
        print(f"Error: Invalid destination hash: {dest_hash_hex}. {e}", file=sys.stderr)
        return None
    except Exception as e:
        RNS.log(f"Error parsing destination hash '{dest_hash_hex}': {e}", RNS.LOG_CRITICAL)
        print(f"Error: Could not parse destination hash '{dest_hash_hex}': {e}", file=sys.stderr)
        return None

def resolve_path(destination_hash, dest_name="destination"):
    """Resolve path to a destination, waiting if necessary."""
    if not RNS.Transport.has_path(destination_hash):
        RNS.log(f"{dest_name.capitalize()} is not yet known. Requesting path...")
        RNS.Transport.request_path(destination_hash)
        wait_start = time.time()
        while not RNS.Transport.has_path(destination_hash):
            if time.time() - wait_start > DEFAULT_TIMEOUT:
                RNS.log(f"Timed out waiting for path to {dest_name}.", RNS.LOG_ERROR)
                print(f"Error: Timed out waiting for path to {dest_name}.", file=sys.stderr)
                return False
            time.sleep(0.1)
        RNS.log(f"Path to {dest_name} obtained.")
    return True

def ensure_local_dirs():
    """Create base, cache, and store directories if they don't exist."""
    try:
        os.makedirs(RNS_PKG_BASE_DIR, exist_ok=True)
        os.makedirs(RNS_STORE_DIR, exist_ok=True)
        os.makedirs(RNS_CACHE_DIR, exist_ok=True)
        RNS.log(f"Ensured local directories exist: {RNS_PKG_BASE_DIR}")
        return True
    except Exception as e:
        RNS.log(f"Could not create local directories in {RNS_PKG_BASE_DIR}: {e}", RNS.LOG_CRITICAL)
        print(f"Error: Could not create local directories: {e}", file=sys.stderr)
        return False

def load_local_state():
    """Load installed packages from the local state file."""
    if not os.path.exists(LOCAL_STATE_DB):
        return {}
    try:
        with open(LOCAL_STATE_DB, 'r') as f:
            state = json.load(f)
            return state
    except json.JSONDecodeError as e:
        RNS.log(f"Error decoding JSON from {LOCAL_STATE_DB}: {e}", RNS.LOG_ERROR)
        print(f"Warning: Local package state file is corrupt: {e}", file=sys.stderr)
        return {}
    except OSError as e:
        RNS.log(f"OS error loading local state from {LOCAL_STATE_DB}: {e}", RNS.LOG_ERROR)
        print(f"Warning: Could not read local package state: {e}", file=sys.stderr)
        return {}

def save_local_state(state):
    """Save installed packages to the local state file."""
    ensure_local_dirs()
    try:
        with open(LOCAL_STATE_DB, 'w') as f:
            json.dump(state, f, indent=4)
    except OSError as e:
        RNS.log(f"OS error saving local state to {LOCAL_STATE_DB}: {e}", RNS.LOG_CRITICAL)
        print(f"Error: Could not write local package state: {e}", file=sys.stderr)
    except Exception as e:
        RNS.log(f"Error saving local state to {LOCAL_STATE_DB}: {e}", RNS.LOG_CRITICAL)
        print(f"Error: Could not save local package state: {e}", file=sys.stderr)

def load_host_profiles():
    """Load known hosts from the profiles file."""
    if not os.path.exists(HOST_PROFILES_DB):
        return {} 
    try:
        with open(HOST_PROFILES_DB, 'r') as f:
            profiles = json.load(f)
            if not isinstance(profiles, dict):
                RNS.log(f"Host profiles file {HOST_PROFILES_DB} is corrupt (not a dict).", RNS.LOG_ERROR)
                print("Warning: Host profiles file is corrupt.", file=sys.stderr)
                return {}
            return profiles
    except json.JSONDecodeError as e:
        RNS.log(f"Error decoding JSON from {HOST_PROFILES_DB}: {e}", RNS.LOG_ERROR)
        print(f"Warning: Host profiles file is corrupt: {e}", file=sys.stderr)
        return {} 
    except OSError as e:
        RNS.log(f"OS error loading host profiles from {HOST_PROFILES_DB}: {e}", RNS.LOG_ERROR)
        print(f"Warning: Could not read host profiles: {e}", file=sys.stderr)
        return {} 
    except Exception as e:
        RNS.log(f"Unexpected error loading host profiles from {HOST_PROFILES_DB}: {e}", RNS.LOG_ERROR)
        print(f"Warning: Unexpected error loading host profiles: {e}", file=sys.stderr)
        return {}

def save_host_profiles(profiles):
    """Save known hosts to the profiles file."""
    ensure_local_dirs()
    try:
        with open(HOST_PROFILES_DB, 'w') as f:
            json.dump(profiles, f, indent=4)
    except OSError as e:
        RNS.log(f"OS error saving host profiles to {HOST_PROFILES_DB}: {e}", RNS.LOG_CRITICAL)
        print(f"Error: Could not write host profiles: {e}", file=sys.stderr)
    except Exception as e:
        RNS.log(f"Unexpected error saving host profiles: {e}", RNS.LOG_CRITICAL)
        print(f"Error: Could not save host profiles: {e}", file=sys.stderr)

class HostAnnounceHandler:
    """Handle incoming Announce packets for package hosts."""
    aspect_filter = APP_NAME

    def __init__(self):
        self.found_hosts = {}

    def received_announce(self, destination_hash, announced_identity, app_data):
        """Callback for received announces."""
        dest_hash_hex = RNS.prettyhexrep(destination_hash)
        id_hash_hex = RNS.prettyhexrep(announced_identity.hash)

        RNS.log(f"Received announce from {dest_hash_hex} (ID: {id_hash_hex[:8]}...), app_data len: {len(app_data) if app_data else 0}", RNS.LOG_VERBOSE)

        if id_hash_hex not in self.found_hosts:
            print(f"\n{colors.prompt('Found potential host identity:')} {id_hash_hex}")
            print(f"  {colors.prompt('Announced destination:')} {dest_hash_hex}")
            self.found_hosts[id_hash_hex] = set()

        if dest_hash_hex not in self.found_hosts[id_hash_hex]:
             self.found_hosts[id_hash_hex].add(dest_hash_hex)
             print(f"  {colors.prompt('Associated destination:')} {dest_hash_hex}")

def request_metadata(force_refresh=False):
    """Establish a link and request metadata from the host."""
    global host_metadata_dest, metadata_link, host_metadata
    if not host_metadata_dest:
        RNS.log("Host metadata destination not configured.", RNS.LOG_ERROR)
        return False

    if metadata_link and metadata_link.status != RNS.Link.CLOSED:
        RNS.log("Metadata link already exists or is establishing.", RNS.LOG_INFO)
        return True

    RNS.log("Establishing link for metadata request...")
    host_metadata = None
    try:
        metadata_link = RNS.Link(host_metadata_dest)
        metadata_link.set_link_established_callback(metadata_link_established)
        metadata_link.set_link_closed_callback(metadata_link_closed)
        return True
    except Exception as e:
        RNS.log(f"Unexpected error establishing metadata link: {e}", RNS.LOG_CRITICAL)
        print(f"{colors.error('Error: Unexpected error establishing metadata link:')} {e}", file=sys.stderr)
        return False

def metadata_link_established(link):
    """Callback when the metadata link is active."""
    RNS.log("Metadata link established. Sending request...")
    send_metadata_request_over_link(link)

def send_metadata_request_over_link(link):
    """Send the actual metadata request over the provided link."""
    try:
        link.request(
            path="/", 
            data=None, 
            response_callback=metadata_response_handler,
            failed_callback=metadata_request_failed,
            timeout = DEFAULT_TIMEOUT / 2
        )
    except Exception as e:
        RNS.log(f"RNS error sending metadata request: {e}", RNS.LOG_ERROR)
        print(f"{colors.error('Error: Network error sending metadata request:')} {e}", file=sys.stderr)
        metadata_request_failed(None)
    except Exception as e:
        RNS.log(f"Unexpected error sending metadata request: {e}", RNS.LOG_CRITICAL)
        print(f"{colors.error('Error: Unexpected error sending metadata request:')} {e}", file=sys.stderr)
        metadata_request_failed(None)

def metadata_response_handler(request_receipt):
    """Callback for successful metadata response."""
    global host_metadata, metadata_link
    RNS.log("Received metadata response from host.")
    try:
        metadata = json.loads(request_receipt.response.decode('utf-8'))
        if isinstance(metadata, dict) and metadata.get("format_version") == METADATA_FORMAT_VERSION:
            host_metadata = metadata
            RNS.log(f"Successfully parsed metadata for {len(host_metadata.get('packages', {}))} packages (v{METADATA_FORMAT_VERSION}).")
        elif isinstance(metadata, dict) and "error" in metadata:
            RNS.log(f"Host responded with error: {metadata['error']}", RNS.LOG_ERROR)
            print(f"{colors.error('Error from host:')} {metadata['error']}", file=sys.stderr)
            host_metadata = {}
        else:
            RNS.log(f"Received invalid metadata format (version {metadata.get('format_version')}, expected {METADATA_FORMAT_VERSION}).", RNS.LOG_ERROR)
            print(f"{colors.error('Error: Received incompatible metadata format from host.')}", file=sys.stderr)
            host_metadata = {}
    except json.JSONDecodeError:
        RNS.log("Failed to decode metadata response (invalid JSON).", RNS.LOG_ERROR)
        print(f"{colors.error('Error: Failed to decode metadata response from host.')}", file=sys.stderr)
        host_metadata = {}
    except Exception as e:
        RNS.log(f"Error processing metadata response: {e}", RNS.LOG_ERROR)
        print(f"{colors.error('Error: Could not process metadata response:')} {e}", file=sys.stderr)
        host_metadata = {}
    finally:
        if metadata_link:
            RNS.log("Closing metadata link.")
            metadata_link.teardown()
            metadata_link = None
        exit_event.set()

def metadata_request_failed(request_receipt):
    """Callback for failed metadata request."""
    global host_metadata, metadata_link
    reason_str = f"status {request_receipt.status}" if request_receipt else "link closed/timeout"
    RNS.log(f"Request for host metadata failed ({reason_str}).", RNS.LOG_ERROR)
    print(f"{colors.error('Error: Failed to retrieve metadata from host')} ({reason_str}).", file=sys.stderr)
    host_metadata = {}
    if metadata_link:
        RNS.log("Closing metadata link after failure.")
        metadata_link.teardown()
        metadata_link = None
    exit_event.set()

def metadata_link_closed(link):
    """Callback when the metadata link is closed."""
    global metadata_link, host_metadata
    reason_map = { 0: "locally closed", 1: "timed out", 2: "closed by host" }
    reason_str = reason_map.get(link.teardown_reason, f"reason code {link.teardown_reason}")
    RNS.log(f"Metadata link closed ({reason_str}).")
    if metadata_link == link:
        metadata_link = None
    if host_metadata is None:
        RNS.log("Metadata link closed before receiving data.", RNS.LOG_WARNING)
        metadata_request_failed(None)

def start_fetch(package_name, version_str):
    """Initiate the fetch process for a specific package archive."""
    global host_metadata, host_fetch_dest, current_fetch_package_info, current_fetch_package_name

    if not host_metadata or not host_metadata.get("packages"):
        RNS.log("Cannot start fetch: Host metadata not available or empty.", RNS.LOG_ERROR)
        print(f"{colors.error('Error: Package metadata not available. Try fetching metadata first.')}", file=sys.stderr)
        return False

    if package_name not in host_metadata["packages"]:
        RNS.log(f"Package '{package_name}' not found in host metadata.", RNS.LOG_ERROR)
        print(f"{colors.error(f'Error: Package {package_name} not found.')}", file=sys.stderr)
        return False

    versions = host_metadata["packages"][package_name]
    target_version_info = None
    if version_str.lower() == 'latest':
        if versions:
             target_version_info = versions[0]
        else:
             RNS.log(f"No versions found for package '{package_name}'.", RNS.LOG_ERROR)
             print(f"{colors.warning(f'Warning: No versions listed for {package_name}')}", file=sys.stderr)
             return False
    else:
        for v_info in versions:
            if v_info['version'] == version_str:
                target_version_info = v_info
                break

    if not target_version_info:
        RNS.log(f"Version '{version_str}' for package '{package_name}' not found.", RNS.LOG_ERROR)
        print(f"{colors.error(f'Error: Version {version_str} for package {package_name} not found.')}", file=sys.stderr)
        return False

    current_fetch_package_info = target_version_info
    current_fetch_package_name = package_name
    archive_filename = current_fetch_package_info['archive_filename']
    archive_hash = current_fetch_package_info['archive_hash']
    content_hash = current_fetch_package_info['store_hash']
    store_pkg_name = f"{content_hash}-{package_name}-{version_str}"
    store_path = os.path.join(RNS_STORE_DIR, store_pkg_name)

    if os.path.exists(store_path):
         RNS.log(f"Package {package_name}-{version_str} already in store: {store_path}")
         print(f"{colors.success(f'Package {package_name}-{version_str} already installed.')}")
         return True

    ensure_local_dirs()
    cache_path = os.path.join(RNS_CACHE_DIR, archive_filename)
    if os.path.exists(cache_path):
        RNS.log(f"Found archive {archive_filename} in cache. Verifying hash...")
        try:
            hasher = hashlib.sha256()
            with open(cache_path, 'rb') as f_verify:
                while True:
                    chunk = f_verify.read(4096)
                    if not chunk:
                        break
                    hasher.update(chunk)
            disk_hash = hasher.hexdigest()
            if disk_hash == archive_hash:
                RNS.log(f"Cache hash matches for {archive_filename}. Skipping fetch.")
                install_from_cache(cache_path, store_path, package_name, version_str, content_hash)
                return True
            RNS.log(f"Cache hash mismatch for {archive_filename}! Expected {archive_hash[:8]}, got {disk_hash[:8]}. Will fetch again.", RNS.LOG_WARNING)
            os.remove(cache_path)
        except OSError as e:
            RNS.log(f"OS error verifying cache file {cache_path}: {e}. Will fetch again.", RNS.LOG_WARNING)
            try: os.remove(cache_path) 
            except OSError as del_e: RNS.log(f"Failed to remove corrupt cache file {cache_path}: {del_e}", RNS.LOG_WARNING)
        except Exception as e:
            RNS.log(f"Unexpected error verifying cache file {cache_path}: {e}. Will fetch again.", RNS.LOG_WARNING)
            try: os.remove(cache_path) 
            except OSError as del_e: RNS.log(f"Failed to remove corrupt cache file {cache_path}: {del_e}", RNS.LOG_WARNING)

    RNS.log(f"Preparing to fetch {package_name} version {current_fetch_package_info['version']} ({archive_filename})...")
    global fetch_target_path
    fetch_target_path = cache_path
    return establish_fetch_link()

def establish_fetch_link():
    """Create the RNS Link to the host's fetch destination."""
    global host_fetch_dest, fetch_link
    if not host_fetch_dest:
        RNS.log("Cannot establish link: Host fetch destination not configured.", RNS.LOG_ERROR)
        return False

    if fetch_link and fetch_link.status != RNS.Link.CLOSED:
        RNS.log("Fetch link already exists or is establishing.", RNS.LOG_INFO)
        if fetch_link.status == RNS.Link.ACTIVE:
            send_fetch_request_packet()
        return True

    RNS.log("Establishing fetch link with host...")
    try:
        fetch_link = RNS.Link(host_fetch_dest)
        fetch_link.set_link_established_callback(fetch_link_established)
        fetch_link.set_link_closed_callback(fetch_link_closed)
        fetch_link.set_resource_callback(resource_received)
        fetch_link.set_resource_started_callback(fetch_began)
        fetch_link.set_resource_concluded_callback(fetch_concluded)
        fetch_link.set_resource_strategy(RNS.Link.ACCEPT_ALL)
        return True
    except Exception as e:
        RNS.log(f"RNS error establishing fetch link: {e}", RNS.LOG_ERROR)
        print(f"{colors.error('Error: Network error establishing fetch link:')} {e}", file=sys.stderr)
        return False

def fetch_link_established(link):
    """Callback when the fetch link is active."""
    RNS.log("Fetch link established.")
    send_fetch_request_packet()

def send_fetch_request_packet():
    """Send the packet requesting the specific package archive."""
    global fetch_link, current_fetch_package_info, current_fetch_package_name
    if not fetch_link or fetch_link.status != RNS.Link.ACTIVE:
        RNS.log("Cannot send fetch request: Link not active.", RNS.LOG_ERROR)
        return
    if not current_fetch_package_info:
        RNS.log("Cannot send fetch request: No package selected.", RNS.LOG_ERROR)
        return

    request_data = {
        "name": current_fetch_package_name,
        "version": current_fetch_package_info['version']
    }
    try:
        request_json = json.dumps(request_data).encode('utf-8')
        RNS.log(f"Sending fetch request for {current_fetch_package_name}-{current_fetch_package_info['version']}")
        packet = RNS.Packet(fetch_link, request_json)
        receipt = packet.send()
        if receipt:
            receipt.set_delivery_callback(fetch_request_delivered)
            receipt.set_timeout_callback(fetch_request_timeout)
            receipt.set_timeout(DEFAULT_TIMEOUT)
        else:
            RNS.log("Failed to get packet receipt for fetch request.", RNS.LOG_ERROR)
            if fetch_link: fetch_link.teardown()
    except Exception as e:
        RNS.log(f"RNS error sending fetch request packet: {e}", RNS.LOG_ERROR)
        print(f"{colors.error('Error: Network error sending fetch request:')} {e}", file=sys.stderr)
        if fetch_link: fetch_link.teardown()
        exit_event.set()

def fetch_request_delivered(receipt):
    """Callback when the fetch request is delivered."""
    RNS.log("Fetch request delivered to host. Waiting for resource transfer...")

def fetch_request_timeout(receipt):
    """Callback when the fetch request times out."""
    RNS.log("Fetch request timed out. Closing link.", RNS.LOG_ERROR)
    print(f"{colors.error('Error: Fetch request timed out.')}", file=sys.stderr)
    if fetch_link: fetch_link.teardown()
    exit_event.set()

def resource_received(resource):
    """Callback when a resource is advertised/received."""
    RNS.log(f"Resource advertised/received: hash {resource.get_hash()[:8]}..., size {size_str(resource.get_transfer_size())}")

def fetch_began(resource):
    """Callback when the resource transfer (fetch) starts."""
    global current_fetch_resource, fetch_started, fetch_transfer_size
    current_fetch_resource = resource
    fetch_started = time.time()
    fetch_transfer_size = resource.total_size
    RNS.log(f"Fetch started for {current_fetch_package_info['archive_filename']} ({size_str(fetch_transfer_size)})...")
    print(f"{colors.info('Fetching:')} {current_fetch_package_info['archive_filename']} ({size_str(fetch_transfer_size)}) ")

def fetch_concluded(resource):
    """Callback when the resource transfer finishes."""
    global fetch_finished, fetch_time, current_fetch_resource, current_fetch_package_info, fetch_target_path, fetch_link, current_fetch_package_name
    fetch_finished = time.time()
    fetch_time = fetch_finished - fetch_started
    print()

    if resource.status == RNS.Resource.COMPLETE:
        RNS.log(f"Resource transfer complete in {format_time(fetch_time)}.")
        expected_hash = current_fetch_package_info['archive_hash']
        expected_size = current_fetch_package_info['archive_size']
        filename = current_fetch_package_info['archive_filename']

        try:
            verifier = hashlib.sha256()
            downloaded_size = 0
            resource.data.seek(0)
            while True:
                chunk = resource.data.read(4096)
                if not chunk:
                    break
                verifier.update(chunk)
                downloaded_size += len(chunk)
            resource.data.seek(0)
            actual_hash = verifier.hexdigest()

            RNS.log(f"Verifying fetch: Size (Expected: {expected_size}, Got: {downloaded_size}), Hash (Expected: {expected_hash[:8]}..., Got: {actual_hash[:8]}...)")

            if downloaded_size != expected_size:
                RNS.log(f"Fetch failed: Size mismatch for '{filename}'.", RNS.LOG_ERROR)
                print(f"{colors.error('Error: Download size mismatch for')} '{filename}'.", file=sys.stderr)
                resource.status = RNS.Resource.FAILED 
            elif actual_hash != expected_hash:
                RNS.log(f"Fetch failed: Hash mismatch for '{filename}'. Potential corruption or tampering.", RNS.LOG_ERROR)
                print(f"{colors.error('Error: Download hash mismatch for')} '{filename}'. Potential corruption.", file=sys.stderr)
                resource.status = RNS.Resource.FAILED
            else:
                RNS.log(f"Fetch verified successfully for '{filename}'. Saving to cache...")
                try:
                    ensure_local_dirs()
                    with open(fetch_target_path, "wb") as f:
                        while True:
                            chunk = resource.data.read(65536) 
                            if not chunk:
                                break
                            f.write(chunk)
                    RNS.log(f"Successfully saved archive to cache: {fetch_target_path}")
                    print(f"{colors.success('Fetched')} '{filename}' to cache.")
                    content_hash = current_fetch_package_info['store_hash']
                    store_pkg_name = f"{content_hash}-{current_fetch_package_name}-{current_fetch_package_info['version']}"
                    store_path = os.path.join(RNS_STORE_DIR, store_pkg_name)
                    install_from_cache(fetch_target_path, store_path, current_fetch_package_name, current_fetch_package_info['version'], content_hash)

                except OSError as e:
                    RNS.log(f"OS error saving fetched file '{fetch_target_path}': {e}", RNS.LOG_ERROR)
                    print(f"\n{colors.error('Error saving file to cache:')} {e}")
                    resource.status = RNS.Resource.FAILED
                except Exception as e:
                    RNS.log(f"Error saving fetched file '{fetch_target_path}': {e}", RNS.LOG_ERROR)
                    print(f"\n{colors.error('Error saving file to cache:')} {e}")
                    resource.status = RNS.Resource.FAILED

        except OSError as e:
             RNS.log(f"OS error during verification of fetched data: {e}", RNS.LOG_ERROR)
             print(f"\n{colors.error('Error verifying downloaded file:')} {e}")
             resource.status = RNS.Resource.FAILED

    elif resource.status == RNS.Resource.FAILED:
        RNS.log("Fetch failed.", RNS.LOG_ERROR)
        print(f"\n{colors.error('Fetch failed!')}")
    elif resource.status == RNS.Resource.TIMEOUT:
        RNS.log("Fetch timed out.", RNS.LOG_WARNING)
        print(f"\n{colors.warning('Fetch timed out!')}")
    else:
        RNS.log(f"Fetch concluded with unexpected status: {resource.status}", RNS.LOG_WARNING)
        print(f"\n{colors.warning(f'Fetch finished with status: {resource.status}')}")

    current_fetch_resource = None
    current_fetch_package_info = None
    current_fetch_package_name = None
    if fetch_link:
        fetch_link.teardown()
        fetch_link = None
    exit_event.set()

def fetch_link_closed(link):
    """Callback when the fetch link is closed."""
    global fetch_link
    reason_map = { 0: "locally closed", 1: "timed out", 2: "closed by host" }
    reason_str = reason_map.get(link.teardown_reason, f"reason code {link.teardown_reason}")
    RNS.log(f"Fetch link closed ({reason_str}).")
    if fetch_link == link:
        fetch_link = None
    if current_fetch_resource and current_fetch_resource.status != RNS.Resource.COMPLETE:
         if not exit_event.is_set():
              print(f"\n{colors.warning(f'Fetch link closed unexpectedly ({reason_str}).')}")
              exit_event.set()

# Helper function to prevent path traversal
def is_within_directory(directory, target):
    """Checks if a target path is safely contained within a directory."""
    abs_directory = os.path.abspath(directory)
    abs_target = os.path.abspath(target)
    prefix = os.path.commonprefix([abs_directory, abs_target])
    return prefix == abs_directory

def install_from_cache(cache_path, store_path, pkg_name, pkg_version, content_hash):
    """Install a package from the cached archive into the store."""
    RNS.log(f"Installing {pkg_name}-{pkg_version} from {cache_path} to {store_path}")
    print(f"{colors.info(f'Installing {pkg_name}-{pkg_version}...')}")

    pkg_type = determine_package_type(os.path.basename(cache_path))
    # Resolve temp_extract_dir to an absolute path for reliable checking
    temp_extract_dir = os.path.abspath(store_path + "-extracting")

    try:
        if os.path.exists(temp_extract_dir):
            shutil.rmtree(temp_extract_dir)
        os.makedirs(temp_extract_dir)

        if pkg_type == "tar.gz":
            try:
                with tarfile.open(cache_path, "r:gz") as tar:
                    for member in tar.getmembers():
                        member_path = os.path.join(temp_extract_dir, member.name)
                        # Ensure the member path is within the extraction directory
                        if not is_within_directory(temp_extract_dir, member_path):
                            RNS.log(f"Skipping potentially unsafe tar member: {member.name}", RNS.LOG_WARNING)
                            print(f"{colors.warning(f'Warning: Skipping potentially unsafe tar member: {member.name}')}", file=sys.stderr)
                            continue
                        # Check for directories explicitly before extracting to avoid creating them based on file paths
                        if member.isdir():
                             # Ensure directory path is safe before creating
                             safe_dir_path = os.path.join(temp_extract_dir, member.name)
                             if is_within_directory(temp_extract_dir, safe_dir_path):
                                 os.makedirs(safe_dir_path, exist_ok=True)
                        elif member.isfile():
                             # Extract only regular files after path check
                             tar.extract(member, path=temp_extract_dir)
                        else:
                             RNS.log(f"Skipping non-file/non-directory tar member: {member.name}", RNS.LOG_VERBOSE)

                RNS.log(f"Extracted tar.gz archive to {temp_extract_dir}", RNS.LOG_VERBOSE)
            except tarfile.TarError as e:
                RNS.log(f"Failed to extract archive {cache_path}: {e}", RNS.LOG_ERROR)
                print(f"{colors.error(f'Error: Failed to extract package archive {os.path.basename(cache_path)}:')} {e}", file=sys.stderr)
                raise
            except Exception as e: # Catch other potential errors during extraction loop
                 RNS.log(f"Error during tar extraction for {cache_path}: {e}", RNS.LOG_ERROR)
                 print(f"{colors.error('Error: Problem during tar extraction:')} {e}", file=sys.stderr)
                 raise
        elif pkg_type == "wheel" or pkg_type == "apk": # Zip based
            try:
                with zipfile.ZipFile(cache_path, 'r') as zip_ref:
                    for member_name in zip_ref.namelist():
                        member_path = os.path.join(temp_extract_dir, member_name)
                        # Ensure the member path is within the extraction directory
                        if not is_within_directory(temp_extract_dir, member_path):
                            RNS.log(f"Skipping potentially unsafe zip member: {member_name}", RNS.LOG_WARNING)
                            print(f"{colors.warning(f'Warning: Skipping potentially unsafe zip member: {member_name}')}", file=sys.stderr)
                            continue

                        # Handle directories explicitly first
                        if member_name.endswith('/'):
                             # Ensure directory path is safe before creating
                             safe_dir_path = os.path.join(temp_extract_dir, member_name)
                             if is_within_directory(temp_extract_dir, safe_dir_path):
                                 os.makedirs(safe_dir_path, exist_ok=True)
                        else:
                             # Create parent directory if needed before extracting file
                             parent_dir = os.path.dirname(member_path)
                             if not os.path.exists(parent_dir):
                                 # Double-check parent is safe too
                                 if is_within_directory(temp_extract_dir, parent_dir):
                                     os.makedirs(parent_dir, exist_ok=True)
                                 else:
                                     # This case should theoretically be caught by the member check, but safety first
                                     RNS.log(f"Skipping zip member due to unsafe parent directory: {member_name}", RNS.LOG_WARNING)
                                     continue
                             # Extract the file
                             zip_ref.extract(member_name, path=temp_extract_dir)

                RNS.log(f"Extracted {pkg_type} archive to {temp_extract_dir}", RNS.LOG_VERBOSE)
            except zipfile.BadZipFile as e:
                RNS.log(f"Failed to extract zip archive {cache_path}: {e}", RNS.LOG_ERROR)
                print(f"{colors.error(f'Error: Failed to extract package archive {os.path.basename(cache_path)} (corrupt zip?):')} {e}", file=sys.stderr)
                raise
            except Exception as e: # Catch other potential errors during extraction loop
                 RNS.log(f"Error during zip extraction for {cache_path}: {e}", RNS.LOG_ERROR)
                 print(f"{colors.error('Error: Problem during zip extraction:')} {e}", file=sys.stderr)
        elif pkg_type == "binary":
            # For binary, still extract to temp dir first for consistency
            bin_dir = os.path.join(temp_extract_dir, "bin")
            os.makedirs(bin_dir, exist_ok=True)
            target_bin_path = os.path.join(bin_dir, os.path.basename(cache_path))
            # Ensure the target path is within the extraction directory (defence in depth)
            if not is_within_directory(temp_extract_dir, target_bin_path):
                RNS.log(f"Binary target path {target_bin_path} seems outside extraction dir {temp_extract_dir}. Aborting install.", RNS.LOG_ERROR)
                raise OSError(f"Invalid target path for binary installation: {target_bin_path}")

            shutil.copy2(cache_path, target_bin_path)
            # Use more restrictive permissions: owner rwx, group -, other -
            os.chmod(target_bin_path, 0o700)
            RNS.log(f"Copied binary to {target_bin_path} with mode 0o700", RNS.LOG_VERBOSE)
        else:
            raise ValueError(f"Unsupported package type '{pkg_type}' for installation.")

        try:
            # Atomically move the completed extraction to the final store path
            # Ensure the final store_path exists before trying to rename over it if needed
            if os.path.exists(store_path):
                # This case might indicate a race condition or prior failed cleanup.
                # For safety, remove the existing destination before renaming.
                RNS.log(f"Destination store path {store_path} already exists. Removing before rename.", RNS.LOG_WARNING)
                shutil.rmtree(store_path)

            os.rename(temp_extract_dir, store_path)
            RNS.log(f"Atomically moved installation to {store_path}")
        except OSError as e:
            RNS.log(f"OS error moving installation to store {store_path}: {e}", RNS.LOG_ERROR)
            print(f"{colors.error('Error: Could not move package to final store location:')} {e}")
            # If rename fails, temp dir might still exist, cleanup handled in outer except block
            raise

        # Update local state only after successful installation and move
        local_state = load_local_state()
        if pkg_name not in local_state:
            local_state[pkg_name] = []

        # Consider adding archive_hash and store_hash here too?
        local_state[pkg_name].append({
            "version": pkg_version,
            "store_path": store_path,
            "installed_at": time.time()
            # Add dependencies here later?
        })
        save_local_state(local_state)
        RNS.log(f"Updated local state database for {pkg_name}-{pkg_version}")
        print(f"{colors.success(f'Successfully installed {pkg_name}-{pkg_version} to {store_path}')}")
        return True

    except Exception as e:
        # General exception handler for installation process
        RNS.log(f"Installation failed for {pkg_name}-{pkg_version}: {e}", RNS.LOG_ERROR)
        # Avoid printing redundant error messages if already printed in specific handlers
        if not isinstance(e, (tarfile.TarError, zipfile.BadZipFile, OSError, ValueError)):
             print(f"{colors.error('Error: Installation failed:')} {e}", file=sys.stderr)

        # Cleanup: remove potentially incomplete extraction directory and final store path if it exists
        if os.path.exists(temp_extract_dir):
             try:
                 shutil.rmtree(temp_extract_dir)
                 RNS.log(f"Cleaned up temporary directory {temp_extract_dir}", RNS.LOG_VERBOSE)
             except Exception as clean_e:
                 RNS.log(f"Cleanup error for temp directory {temp_extract_dir}: {clean_e}", RNS.LOG_WARNING)
        if os.path.exists(store_path):
             pass # Added pass to satisfy indentation requirements
             # Only remove store_path if it was created by *this* failed attempt.
             # If os.rename failed, store_path shouldn't exist *yet* unless there was a race condition.
             # The check inside the try block handles the pre-existing case.
             # It might be safer *not* to remove store_path here unless we are certain it's from this failed run.
             # Let's comment out the store_path cleanup here for now to avoid deleting potentially good installs on error.
             # try:
             #     shutil.rmtree(store_path)
             #     RNS.log(f"Cleaned up store path {store_path} after failure", RNS.LOG_VERBOSE)
             # except Exception as clean_e:
             #     RNS.log(f"Cleanup error for store path {store_path}: {clean_e}", RNS.LOG_WARNING)
        return False

# Need determine_package_type also in client
def determine_package_type(filename):
    """Determine package type based on filename extension."""
    if filename.endswith(".tar.gz") or filename.endswith(".tgz"):
        return "tar.gz"
    if filename.endswith(".whl"):
        return "wheel"
    if filename.endswith(".apk"):
        return "apk"
    if '.' not in os.path.basename(filename): # Heuristic for plain binary
        return "binary"
    return "unknown"

# --- Command Functions ----
def handle_search(args):
    """Handles the 'search' command."""
    global host_metadata
    if not host_metadata or not host_metadata.get("packages"):
        print(f"{colors.info('No package metadata loaded. Fetching...')}")
        if not request_metadata():
            return 1
        exit_event.clear()
        exit_event.wait(DEFAULT_TIMEOUT) # Wait for metadata response
        if not host_metadata or not host_metadata.get("packages"):
            print(f"{colors.error('Failed to retrieve metadata.')}", file=sys.stderr)
            return 1

    print(f"{colors.prompt('Available Packages:')}")
    print(f"{colors.prompt('-------------------')}")
    found = False
    term = args.package_term.lower() if args.package_term else None
    packages = host_metadata.get("packages", {})
    pkg_list = sorted(packages.keys())

    for pkg_name in pkg_list:
        if term and term not in pkg_name.lower():
            continue
        found = True
        versions = packages[pkg_name]
        latest_version = versions[0] if versions else None
        if latest_version:
             print(f"  {colors.colorize(pkg_name, colors.BOLD)}")
             print(f"      Latest: {colors.info(latest_version['version'])} ({colors.detail(size_str(latest_version['archive_size']))}) StoreHash: {colors.detail(latest_version['store_hash'][:8]+'...')}")
             # TODO: Add description field to metadata
             # Potential improvement: Show description if available
             # if 'description' in latest_version:
             #    print(f"      Desc: {colors.detail(latest_version['description'])}")
        else:
             print(f"  {colors.colorize(pkg_name, colors.BOLD)} {colors.warning('(No versions available)')}")

    if not found:
        print(f"{colors.warning(f'No packages found matching {args.package_term}' if term else 'No packages available.')}")
    print(f"{colors.prompt('-------------------')}")
    return 0

def handle_install(args):
    """Handles the 'install' command."""
    global host_metadata
    pkg_specifiers = args.packages

    if not host_metadata or not host_metadata.get("packages"):
        print(f"{colors.info('No package metadata loaded. Fetching...')}")
        if not request_metadata():
            return 1
        exit_event.clear()
        exit_event.wait(DEFAULT_TIMEOUT) # Wait for metadata response
        if not host_metadata or not host_metadata.get("packages"):
            print(f"{colors.error('Failed to retrieve metadata.')}", file=sys.stderr)
            return 1

    install_results = {}
    success_count = 0
    fail_count = 0

    for spec in pkg_specifiers:
        # Basic parsing: name or name==version
        # TODO: Add more robust spec parsing (>, <, etc.)
        pkg_name = spec
        pkg_version = "latest"
        if "==" in spec:
            parts = spec.split("==", 1)
            pkg_name = parts[0]
            pkg_version = parts[1]

        print(f"\n{colors.prompt(f'Attempting to install {pkg_name} version {pkg_version}...')}")
        exit_event.clear()
        result = start_fetch(pkg_name, pkg_version)
        install_results[spec] = {"status": None, "message": ""}

        if result is False: # Explicit check for False indicating immediate failure (e.g., metadata error)
             print(f"{colors.error(f'Failed to initiate fetch for {pkg_name}@{pkg_version}')}", file=sys.stderr)
             install_results[spec]["status"] = "failed"
             install_results[spec]["message"] = "Could not start fetch (check metadata?)"
             fail_count += 1
             continue
        elif result is True: # Explicit check for True indicating already installed or cached & installed
            # Message already printed by start_fetch or install_from_cache
            install_results[spec]["status"] = "success"
            install_results[spec]["message"] = "Package already installed or installed from cache."
            # success_count += 1 # Counted later based on final status
            continue

        # If result is None (or implicitly), means fetch started, need to wait
        print(f"{colors.info('Waiting for fetch/install process... (Press Ctrl+C to attempt abort)')}")
        progress_update_time = time.time()
        try:
             while not exit_event.is_set():
                 if current_fetch_resource:
                     if time.time() - progress_update_time > 0.5:
                         progress = current_fetch_resource.get_progress()
                         rate = current_fetch_resource.get_rate()
                         eta = current_fetch_resource.get_eta()
                         eta_str = format_time(eta) if eta is not None else "N/A"
                         print(f"\r{colors.info('Fetching:')} {progress*100:.1f}% ({colors.detail(size_str(rate))}/s, ETA: {colors.detail(eta_str)})   ", end="")
                         progress_update_time = time.time()
                 time.sleep(0.1)
             print("\n") # Clear progress line

             # Check the actual outcome after wait finishes
             # Assume install_from_cache (called within fetch_concluded) sets the status
             # Re-check local state for final confirmation
             local_state_final = load_local_state()
             found_in_state = False
             if pkg_name in local_state_final:
                 for installed_version in local_state_final[pkg_name]:
                     # We need to compare against the *specific* version we tried to install
                     # The version might be 'latest', so we need the actual version fetched.
                     # This info is lost after fetch_concluded clears current_fetch_package_info
                     # TODO: fetch_concluded needs to pass back the *actual* installed version info
                     # For now, just checking if *any* version is present after waiting isn't reliable enough
                     # Let's assume fetch_concluded updated install_results[spec]["status"] correctly
                     # If not updated, assume failure for now.
                     pass # Placeholder logic, see TODO above

             if install_results[spec]["status"] is None: # If status wasn't set by fetch_concluded (e.g., timeout?)
                 RNS.log(f"Fetch/Install process for {spec} ended without explicit status.", RNS.LOG_WARNING)
                 install_results[spec]["status"] = "unknown"
                 install_results[spec]["message"] = "Process finished with unknown status (timeout or interruption?)"
                 # fail_count += 1 # Counted later

        except KeyboardInterrupt:
             RNS.log("User interrupt during fetch/install wait.")
             print(f"\n{colors.warning('Fetch/Install interrupted by user.')}")
             if fetch_link: fetch_link.teardown()
             if current_fetch_resource: current_fetch_resource.cancel()
             install_results[spec]["status"] = "interrupted"
             install_results[spec]["message"] = "Installation interrupted by user."
             # Don't increment fail_count, just report interruption
             # Stop processing further specs on interrupt
             break

    # Summary
    print(f"\n{colors.prompt('Installation Summary:')}")
    final_success_count = 0
    final_fail_count = 0
    interrupted_count = 0
    for spec, result in install_results.items():
        status = result.get("status", "unknown")
        message = result.get("message", "")
        if status == "success":
            print(f"- {spec}: {colors.success('Success')}{f' ({message})' if message else ''}")
            final_success_count += 1
        elif status == "failed" or status == "unknown":
            print(f"- {spec}: {colors.error(status.capitalize())}{f' ({message})' if message else ''}")
            final_fail_count += 1
        elif status == "interrupted":
            print(f"- {spec}: {colors.warning('Interrupted')}")
            interrupted_count += 1
        else: # Should not happen
            print(f"- {spec}: {status}")
            final_fail_count += 1 # Count unexpected as failure

    if final_fail_count > 0 or interrupted_count > 0:
         return 1 # Indicate partial/total failure or interruption
    return 0 # Indicate full success

def handle_list(args):
    """Handles the 'list' command."""
    local_state = load_local_state()
    if not local_state:
        print(f"{colors.info('No packages installed.')}")
        return 0

    print(f"{colors.prompt('Installed Packages:')}")
    print(f"{colors.prompt('-------------------')}")
    pkg_list = sorted(local_state.keys())
    for pkg_name in pkg_list:
        # TODO: Handle multiple installed versions if that becomes supported
        if local_state[pkg_name]:
            # Assume the last entry is the 'active' one for now
            install_info = local_state[pkg_name][-1]
            version = install_info.get("version", "unknown")
            store_path = install_info.get("store_path", "unknown path")
            hash_part = os.path.basename(store_path).split('-')[0] if store_path != "unknown path" and '-' in os.path.basename(store_path) else "unknown_hash"
            install_time_unix = install_info.get("installed_at")
            install_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(install_time_unix)) if install_time_unix else "unknown time"

            print(f"  {colors.colorize(pkg_name, colors.BOLD)}=={colors.info(version)}")
            print(f"      Store Path: {colors.detail(store_path)}")
            # print(f"      Store Hash: {colors.detail(hash_part)}") # Maybe redundant if in path
            print(f"      Installed:  {colors.detail(install_time_str)}")
        else:
            print(f"  {colors.colorize(pkg_name, colors.BOLD)} {colors.warning('(installation record invalid)')}")
    print(f"{colors.prompt('-------------------')}")
    return 0

def handle_uninstall(args):
    """Handles the 'uninstall' command."""
    packages_to_remove = args.packages
    local_state = load_local_state()
    removed_count = 0
    not_found_count = 0
    error_count = 0

    print(f"{colors.prompt('Attempting to uninstall:')} {', '.join(packages_to_remove)}")

    packages_actually_removed = []

    for pkg_spec in packages_to_remove:
        # TODO: Add version specification handling (e.g., pkg==version)
        # For now, just removes the latest/only installed version by name
        pkg_name = pkg_spec
        if pkg_name not in local_state or not local_state[pkg_name]:
            print(f"{colors.warning(f'Warning: Package {pkg_name} not found in local state.')}")
            not_found_count += 1
            continue

        # Assume last entry is the one to remove
        install_info = local_state[pkg_name][-1] # Get latest install record
        version = install_info.get("version", "unknown")
        store_path = install_info.get("store_path")

        if not store_path or not os.path.isdir(store_path):
            print(f"{colors.error(f'Error: Store path for {pkg_name}-{version} is invalid or missing:')} {store_path}", file=sys.stderr)
            print(f"{colors.warning(f'Warning: Removing entry for {pkg_name}-{version} from state, but directory may need manual cleanup.')}", file=sys.stderr)
            error_count += 1
            # Remove from state even if directory is bad
            local_state[pkg_name].pop()
            if not local_state[pkg_name]:
                 del local_state[pkg_name]
            packages_actually_removed.append(f"{pkg_name}-{version}")
            continue

        print(f"{colors.info('Removing directory:')} {store_path}")
        try:
            shutil.rmtree(store_path)
            RNS.log(f"Removed package directory {store_path}")
            # Remove the specific version entry from the state
            local_state[pkg_name].pop()
            # If no more versions of this package exist, remove the package key
            if not local_state[pkg_name]:
                 del local_state[pkg_name]
            removed_count += 1
            packages_actually_removed.append(f"{pkg_name}-{version}")
            print(f"{colors.success(f'Successfully removed {pkg_name}-{version}.')}")
        except OSError as e:
            print(f"{colors.error(f'Error removing directory {store_path}:')} {e}", file=sys.stderr)
            RNS.log(f"Error removing package directory {store_path}: {e}", RNS.LOG_ERROR)
            error_count += 1
        except Exception as e:
            print(f"{colors.error(f'Unexpected error removing {store_path}:')} {e}", file=sys.stderr)
            RNS.log(f"Unexpected error removing package directory {store_path}: {e}", RNS.LOG_CRITICAL)
            error_count += 1

    if packages_actually_removed:
        save_local_state(local_state)
        RNS.log(f"Updated local state after removing: {', '.join(packages_actually_removed)}")

    print(f"\n{colors.prompt('Uninstall Summary:')}")
    print(f"  Successfully removed: {removed_count}")
    print(f"  Not found: {not_found_count}")
    print(f"  Errors encountered: {error_count}")

    return 1 if error_count > 0 or not_found_count > 0 else 0

def handle_update(args):
    """Handles the 'update' command (currently checks for newer versions)."""
    global host_metadata
    profile_name = args.profile_name
    packages_to_check = args.packages # Optional list of packages

    profiles = load_host_profiles()
    if profile_name not in profiles:
        print(colors.error(f"Error: Host profile '{profile_name}' not found."), file=sys.stderr)
        return 1

    profile_data = profiles[profile_name]
    metadata_hash_str = profile_data.get("metadata_hash")
    if not metadata_hash_str:
        print(colors.error(f"Error: Profile '{profile_name}' is missing metadata hash."), file=sys.stderr)
        return 1

    print(f"{colors.info('Checking for updates using profile:')} {profile_name}")

    # We need to setup RNS and resolve path to get metadata
    # This duplicates logic from main(), consider refactoring later
    global reticulum, host_metadata_dest
    try:
        if not reticulum:
            reticulum = RNS.Reticulum(args.config if hasattr(args, 'config') else None) # Use default config if not specified globally
            print(f"{colors.info('rns-pkg using Reticulum config:')} {colors.detail(RNS.Reticulum.configdir)}")

        metadata_hash = parse_destination(metadata_hash_str)
        if not metadata_hash: return 1

        print(f"{colors.info('Resolving host metadata path...')}")
        if not resolve_path(metadata_hash, "metadata host"): return 1

        host_identity = RNS.Identity.recall(metadata_hash)
        if not host_identity:
            RNS.log("Could not recall host Identity from profile metadata hash.", RNS.LOG_CRITICAL)
            print(f"{colors.error('Error: Failed to identify the host from the profile.')}", file=sys.stderr)
            return 1

        host_metadata_dest = RNS.Destination(
            host_identity, RNS.Destination.OUT, RNS.Destination.SINGLE,
            APP_NAME, METADATA_ASPECT
        )

    except Exception as e:
        print(f"{colors.error('Error: Network error setting up for update check:')} {e}", file=sys.stderr)
        return 1

    print(f"{colors.info('Fetching latest metadata from host...')}")
    host_metadata = None # Clear previous metadata
    if not request_metadata():
        return 1
    exit_event.clear()
    exit_event.wait(DEFAULT_TIMEOUT) # Wait for metadata response

    if not host_metadata or not host_metadata.get("packages"):
        print(f"{colors.error('Failed to retrieve metadata from host.')}", file=sys.stderr)
        return 1

    print(f"{colors.info('Comparing with locally installed packages...')}")
    local_state = load_local_state()
    if not local_state:
        print(f"{colors.info('No packages installed locally to update.')}")
        return 0

    updates_available = {}
    host_packages = host_metadata.get("packages", {})

    packages_in_scope = packages_to_check if packages_to_check else sorted(local_state.keys())

    for pkg_name in packages_in_scope:
        if pkg_name not in local_state or not local_state[pkg_name]:
            if packages_to_check: # Only warn if user specifically asked for this package
                print(f"{colors.warning(f'Warning: Package {pkg_name} not installed locally.')}")
            continue

        if pkg_name not in host_packages or not host_packages[pkg_name]:
            # Package installed locally but not available on host anymore?
            print(f"{colors.warning(f'Warning: Installed package {pkg_name} not found on host {profile_name}.')}")
            continue

        installed_info = local_state[pkg_name][-1] # Check latest installed version
        installed_version = installed_info.get("version")
        if not installed_version: continue # Skip invalid entries

        latest_host_info = host_packages[pkg_name][0] # Host metadata is sorted latest first
        latest_host_version = latest_host_info.get("version")
        if not latest_host_version: continue

        # Basic version comparison (assumes comparable strings, ideally use packaging.version)
        # TODO: Implement proper version comparison (e.g., using 'packaging' library)
        if latest_host_version != installed_version:
             # Simplistic check: if they differ, assume host is newer if it's listed first
             # This breaks if versions aren't strictly ordered or comparable like this
             try:
                 from packaging.version import parse as parse_version
                 if parse_version(latest_host_version) > parse_version(installed_version):
                     updates_available[pkg_name] = {
                         "installed": installed_version,
                         "latest": latest_host_version
                     }
             except ImportError:
                  # Fallback to simple string comparison if packaging is not available
                  if latest_host_version > installed_version: # Very basic fallback
                      updates_available[pkg_name] = {
                         "installed": installed_version,
                         "latest": latest_host_version
                     }
             except Exception as e:
                 print(f"{colors.warning(f'Could not compare versions for {pkg_name} ({installed_version} vs {latest_host_version}): {e}')}")


    if not updates_available:
        print(f"{colors.success('All specified packages are up-to-date.')}")
    else:
        print(f"\n{colors.prompt('Updates available:')}")
        for pkg_name, versions in updates_available.items():
            print(f"  {colors.colorize(pkg_name, colors.BOLD)}: {colors.warning(versions['installed'])} -> {colors.success(versions['latest'])}")
        print(f"\n{colors.info('To install updates, run:')} {colors.colorize(f'rns-pkg install {profile_name} {" ".join([f"{p}=={v["latest"]}" for p,v in updates_available.items()])}', colors.BOLD)}")
        # Return 1 to indicate updates are available, even though none were installed
        return 1

    return 0


def handle_clean(args):
    """Handles the 'clean' command."""
    if args.target == "cache":
        if not os.path.isdir(RNS_CACHE_DIR):
            print(f"{colors.info('Cache directory does not exist:')} {RNS_CACHE_DIR}")
            return 0

        print(f"{colors.prompt('Cache directory:')} {RNS_CACHE_DIR}")
        cache_files = []
        total_size = 0
        try:
            for item in os.listdir(RNS_CACHE_DIR):
                item_path = os.path.join(RNS_CACHE_DIR, item)
                if os.path.isfile(item_path):
                     try:
                         size = os.path.getsize(item_path)
                         cache_files.append((item, size))
                         total_size += size
                     except OSError:
                         pass # Ignore files we can't access/stat
                # Could add handling for unexpected subdirs later if needed
        except OSError as e:
            print(f"{colors.error('Error listing cache directory contents:')} {e}", file=sys.stderr)
            return 1

        if not cache_files:
            print(f"{colors.info('Cache directory is empty.')}")
            return 0

        print(f"{colors.info(f'Found {len(cache_files)} file(s) totaling {size_str(total_size)}.')}")

        if not args.yes:
            try:
                confirm = input(f"{colors.warning('Proceed with cleaning the cache? (y/N): ')}").lower()
                if confirm != 'y':
                    print(f"{colors.info('Cache cleaning cancelled.')}")
                    return 0
            except EOFError:
                print(f"\n{colors.warning('Input closed, cancelling clean.')}")
                return 1
            except KeyboardInterrupt:
                print(f"\n{colors.warning('Cache cleaning cancelled by user.')}")
                return 130

        print(f"{colors.info('Cleaning cache...')}")
        removed_count = 0
        error_count = 0
        for filename, _ in cache_files:
            file_path = os.path.join(RNS_CACHE_DIR, filename)
            try:
                os.remove(file_path)
                removed_count += 1
                RNS.log(f"Removed cached file: {file_path}", RNS.LOG_VERBOSE)
            except OSError as e:
                print(f"{colors.error(f'Error removing cache file {filename}:')} {e}", file=sys.stderr)
                RNS.log(f"Error removing cache file {file_path}: {e}", RNS.LOG_ERROR)
                error_count += 1

        print(f"{colors.success(f'Cleaned {removed_count} file(s).')} Encountered {error_count} error(s).")
        return 1 if error_count > 0 else 0

    else:
        print(f"{colors.error(f'Unknown target for clean command: {args.target}')}", file=sys.stderr)
        return 1


# --- Host Profile Commands ---
def handle_host_add(args):
    """Handles the 'host add' command."""
    profile_name = args.name
    metadata_hash_hex = args.metadata_hash
    fetch_hash_hex = args.fetch_hash

    # Validate hashes
    metadata_hash = parse_destination(metadata_hash_hex)
    fetch_hash = parse_destination(fetch_hash_hex)
    if not metadata_hash or not fetch_hash:
        print(f"{colors.error('Error: Invalid destination hash provided.')}", file=sys.stderr)
        return 1

    profiles = load_host_profiles()
    if profile_name in profiles and not args.force:
        print(f"{colors.error(f'Error: Profile {profile_name} already exists. Use --force to overwrite.')}", file=sys.stderr)
        return 1

    profiles[profile_name] = {
        "metadata_hash": metadata_hash_hex,
        "fetch_hash": fetch_hash_hex
    }
    save_host_profiles(profiles)
    print(f"{colors.success(f'Added host profile {profile_name}.')}")
    return 0

def handle_host_remove(args):
    """Handles the 'host remove' command."""
    profile_name = args.name
    profiles = load_host_profiles()
    if profile_name not in profiles:
        print(f"{colors.error(f'Error: Profile {profile_name} not found.')}", file=sys.stderr)
        return 1

    del profiles[profile_name]
    save_host_profiles(profiles)
    print(f"{colors.success(f'Removed host profile {profile_name}.')}")
    return 0

def handle_host_list(args):
    """Handles the 'host list' command."""
    profiles = load_host_profiles()
    if not profiles:
        print(f"{colors.info('No host profiles defined.')}")
        return 0

    print(f"{colors.prompt('Defined Host Profiles:')}")
    print(f"{colors.prompt('----------------------')}")
    for name, data in sorted(profiles.items()):
        print(f"  {colors.colorize(name, colors.BOLD)}:")
        print(f"    Metadata Hash: {colors.detail(data.get('metadata_hash', 'N/A'))}")
        print(f"    Fetch Hash   : {colors.detail(data.get('fetch_hash', 'N/A'))}")
    print(f"{colors.prompt('----------------------')}")
    return 0

def handle_host_find(args):
    """Handles the 'host find' command."""
    global reticulum # Ensure reticulum is initialized
    print(f"{colors.info(f'Listening for package host announces for {args.timeout} seconds... (Press Ctrl+C to stop early)')}")

    announce_handler = HostAnnounceHandler()
    RNS.Transport.register_announce_handler(announce_handler)

    try:
        # Keep the program alive while listening
        listen_end_time = time.time() + args.timeout
        while time.time() < listen_end_time:
            time.sleep(0.2)
            # TODO: Could potentially try Transport.recall() on found identity hashes here 
            #       to find the corresponding metadata/fetch destinations, but that adds complexity.

    except KeyboardInterrupt:
        print(f"\n{colors.warning('Stopped listening.')}")
    finally:
        RNS.Transport.deregister_announce_handler(announce_handler)
        print(f"{colors.info('Finished listening for hosts.')}")

    if not announce_handler.found_hosts:
        print(f"{colors.warning('No package hosts found during listening period.')}")
    else:
        print(f"\n{colors.prompt('Summary of potential hosts found:')}")
        for id_hash, dest_hashes in announce_handler.found_hosts.items():
            print(f" - Identity: {colors.info(id_hash)}")
            for dest_hash in dest_hashes:
                print(f"   - Destination: {colors.detail(dest_hash)}")

    return 0

# --- Main Execution ----
def main():
    global reticulum, host_metadata_dest, host_fetch_dest, exit_event

    parser = argparse.ArgumentParser(
        description="Reticulum Package Manager Client",
        prog="rns-pkg" # Explicitly set program name
    )
    parser.add_argument("--config", help="Path to alternative Reticulum config directory")
    subparsers = parser.add_subparsers(dest="command", required=True, help="Command to execute")

    # Search command
    parser_search = subparsers.add_parser(
        "search",
        aliases=['s'],
        help="Search for available packages on a host"
    )
    parser_search.add_argument("host_specifier", help="Host profile name or metadata destination hash")
    parser_search.add_argument("package_term", nargs='?', help="Optional search term for package name")
    parser_search.set_defaults(func=handle_search)

    # Install command
    parser_install = subparsers.add_parser(
        "install",
        aliases=['i'],
        help="Install packages from a host"
    )
    # Group for mutually exclusive host specification
    install_host_group = parser_install.add_mutually_exclusive_group(required=True)
    install_host_group.add_argument("--profile", dest="profile_name", help="Host profile name (must be added via 'host add')")
    install_host_group.add_argument("--metadata-hash", help="Direct host metadata destination hash")
    parser_install.add_argument("--fetch-hash", help="Direct host fetch destination hash (required if using --metadata-hash)")
    parser_install.add_argument("packages", nargs='+', help="Package(s) to install (e.g., pkgname or pkgname==version)")
    parser_install.set_defaults(func=handle_install)

    # Uninstall command
    parser_uninstall = subparsers.add_parser(
        "uninstall",
        aliases=['rm', 'remove'],
        help="Uninstall locally installed packages"
    )
    parser_uninstall.add_argument("packages", nargs='+', help="Package name(s) to uninstall")
    # parser_uninstall.add_argument("--yes", "-y", action="store_true", help="Automatically confirm removal") # TODO: Implement --yes flag
    parser_uninstall.set_defaults(func=handle_uninstall)

    # Update command
    parser_update = subparsers.add_parser(
        "update",
        aliases=['up'],
        help="Check for newer versions of installed packages on a host"
    )
    parser_update.add_argument("profile_name", help="Host profile name to check against")
    parser_update.add_argument("packages", nargs='*', help="Specific package(s) to check (default: all installed)")
    # parser_update.add_argument("--install", action="store_true", help="Automatically install available updates") # TODO: Implement install flag
    parser_update.set_defaults(func=handle_update)


    # List command
    parser_list = subparsers.add_parser(
        "list",
        aliases=['ls'],
        help="List locally installed packages"
    )
    # No host needed for list
    parser_list.set_defaults(func=handle_list)

    # Clean command
    parser_clean = subparsers.add_parser(
        "clean",
        help="Clean up local resources"
    )
    parser_clean.add_argument("target", choices=['cache'], help="What to clean (currently only 'cache')")
    parser_clean.add_argument("--yes", "-y", action="store_true", help="Automatically confirm cleaning")
    parser_clean.set_defaults(func=handle_clean)


    # Host command group
    parser_host = subparsers.add_parser("host", help="Manage package host profiles")
    host_subparsers = parser_host.add_subparsers(dest="host_command", required=True, help="Host management action")

    # Host add
    parser_host_add = host_subparsers.add_parser("add", help="Add a new host profile")
    parser_host_add.add_argument("name", help="Memorable name for the host profile")
    parser_host_add.add_argument("metadata_hash", help="Host metadata destination hash")
    parser_host_add.add_argument("fetch_hash", help="Host fetch destination hash")
    parser_host_add.add_argument("--force", action="store_true", help="Overwrite existing profile")
    parser_host_add.set_defaults(func=handle_host_add)

    # Host remove
    parser_host_rm = host_subparsers.add_parser("remove", help="Remove a host profile")
    parser_host_rm.add_argument("name", help="Name of the profile to remove")
    parser_host_rm.set_defaults(func=handle_host_remove)

    # Host list
    parser_host_list = host_subparsers.add_parser("list", help="List defined host profiles")
    parser_host_list.set_defaults(func=handle_host_list)

    # Host find
    parser_host_find = host_subparsers.add_parser("find", help="Listen for host announcements")
    parser_host_find.add_argument("-t", "--timeout", type=int, default=30, help="Seconds to listen for announces (default: 30)")
    parser_host_find.set_defaults(func=handle_host_find)

    args = parser.parse_args()

    # Validate install command direct hash usage
    if args.command == "install" and args.metadata_hash and not args.fetch_hash:
         parser.error("--fetch-hash is required when using --metadata-hash for install.")
    if args.command == "install" and args.fetch_hash and not args.metadata_hash:
         # This case is caught by the mutually exclusive group if profile_name is also missing
         # but add an explicit check just in case.
         parser.error("--metadata-hash is required when using --fetch-hash for install.")


    # Initialize Reticulum conditionally (needed for search, install, update, find)
    if args.command in ["search", "install", "update"] or (args.command == "host" and args.host_command == "find"):
        try:
            # Only create instance if it doesn't exist
            if not reticulum:
                reticulum = RNS.Reticulum(args.config)
                print(f"{colors.info('rns-pkg using Reticulum config:')} {colors.detail(RNS.Reticulum.configdir)}")
        except Exception as e:
             print(f"{colors.error('Error: Failed to initialize Reticulum:')} {e}", file=sys.stderr)
             print(f"{colors.warning('Check network interfaces and configuration.')}")
             sys.exit(1)

    # Load profiles if potentially needed
    profiles = None
    if args.command in ["search", "install", "update"] or (args.command == "host" and args.host_command == "add"):
        profiles = load_host_profiles()

    # Setup destinations only if needed (search, install, update)
    metadata_hash_str = None
    fetch_hash_str = None

    # Determine destination hashes based on command and args
    if args.command == "search":
        if args.host_specifier in profiles:
            metadata_hash_str = profiles[args.host_specifier].get("metadata_hash")
            if not metadata_hash_str:
                print(colors.error(f"Error: Profile '{args.host_specifier}' found but missing metadata hash."), file=sys.stderr)
                sys.exit(1)
            print(f"{colors.info('Using profile:')} {args.host_specifier}")
        else:
            # Assume it's a hash directly
            metadata_hash_str = args.host_specifier
            # Fetch hash is not used for search
    elif args.command == "install":
        if args.profile_name:
            if args.profile_name in profiles:
                profile_data = profiles[args.profile_name]
                metadata_hash_str = profile_data.get("metadata_hash")
                fetch_hash_str = profile_data.get("fetch_hash")
                if not metadata_hash_str or not fetch_hash_str:
                    print(colors.error(f"Error: Profile '{args.profile_name}' is incomplete (missing metadata or fetch hash)."), file=sys.stderr)
                    sys.exit(1)
                print(f"{colors.info('Using profile:')} {args.profile_name}")
            else:
                print(colors.error(f"Error: Host profile '{args.profile_name}' not found."), file=sys.stderr)
                sys.exit(1)
        elif args.metadata_hash and args.fetch_hash:
            metadata_hash_str = args.metadata_hash
            fetch_hash_str = args.fetch_hash
            print(f"{colors.info('Using direct hashes:')}")
            print(f"  Metadata: {colors.detail(metadata_hash_str)}")
            print(f"  Fetch:    {colors.detail(fetch_hash_str)}")
        else:
             # Should be caught by argparse, but defensive check
             print(colors.error("Error: Install requires either --profile or both --metadata-hash and --fetch-hash."), file=sys.stderr)
             sys.exit(1)
    elif args.command == "update":
        # Update command already checks profile existence in its handler
        # No destination setup needed here, handler does it.
        pass


    # Proceed with destination validation and setup if hashes were determined
    if metadata_hash_str:
        metadata_hash = parse_destination(metadata_hash_str)
        if not metadata_hash:
            sys.exit(1)

        fetch_hash = None
        if fetch_hash_str:
            fetch_hash = parse_destination(fetch_hash_str)
            if not fetch_hash:
                sys.exit(1)

        # Resolve paths if RNS is initialized
        if reticulum:
            print(f"{colors.info('Resolving host paths...')}")
            resolved_metadata = False
            resolved_fetch = False
            try:
                resolved_metadata = resolve_path(metadata_hash, "metadata host")
                # Only resolve fetch if we have the hash and need it (install)
                if fetch_hash and args.command == "install":
                    resolved_fetch = resolve_path(fetch_hash, "fetch host")
                else:
                    # If fetch not needed (e.g. search), consider it resolved for this step
                    resolved_fetch = True 

                if not resolved_metadata or not resolved_fetch:
                     print(colors.error("Error: Failed to resolve required host paths."), file=sys.stderr)
                     sys.exit(1)

            except Exception as e:
                print(f"{colors.error('Error: Network error resolving paths:')} {e}", file=sys.stderr)
                sys.exit(1)

            # Create OUT destinations - Move assignment outside the try block
            temp_metadata_dest = None
            temp_fetch_dest = None
            try:
                # Try recalling identity from metadata hash first
                host_identity = RNS.Identity.recall(metadata_hash)
                if not host_identity and fetch_hash:
                     # Try fetch hash if metadata didn't work or wasn't provided (e.g., future scenarios)
                     host_identity = RNS.Identity.recall(fetch_hash)

                if not host_identity:
                     RNS.log("Could not recall host Identity from provided hashes.", RNS.LOG_CRITICAL)
                     print(f"{colors.error('Error: Failed to identify the host from provided destination hashes.')}", file=sys.stderr)
                     # Add specific error for direct hash mode
                     if args.metadata_hash:
                          print(colors.error("Check if the provided metadata/fetch hashes correspond to a valid RNS Identity."), file=sys.stderr)
                     else:
                          print(f"{colors.error('Failed to identify the host from profile ')}'{args.profile_name if args.profile_name else args.host_specifier}'.", file=sys.stderr)
                     sys.exit(1)

                RNS.log(f"Recalled host identity: {RNS.prettyhexrep(host_identity.hash)}")

                # Create temporary destinations first
                temp_metadata_dest = RNS.Destination(
                    host_identity, RNS.Destination.OUT, RNS.Destination.SINGLE,
                    APP_NAME, METADATA_ASPECT
                )
                RNS.log(f"Prepared metadata destination: {temp_metadata_dest}", RNS.LOG_DEBUG)

                # Only create fetch destination if we have the hash and need it (install)
                if fetch_hash and args.command == "install":
                    temp_fetch_dest = RNS.Destination(
                        host_identity, RNS.Destination.OUT, RNS.Destination.SINGLE,
                        APP_NAME, FETCH_ASPECT
                    )
                    RNS.log(f"Prepared fetch destination: {temp_fetch_dest}", RNS.LOG_DEBUG)

            except Exception as e:
                RNS.log(f"Error creating destinations or recalling identity: {e}", RNS.LOG_CRITICAL)
                print(f"{colors.error('Error: Network error setting up communication destinations:')} {e}", file=sys.stderr)
                sys.exit(1)

            # Assign to globals only if creation succeeded
            if temp_metadata_dest:
                 host_metadata_dest = temp_metadata_dest
                 RNS.log(f"Assigned global host_metadata_dest: {host_metadata_dest}", RNS.LOG_DEBUG)
            if temp_fetch_dest:
                 host_fetch_dest = temp_fetch_dest
                 RNS.log(f"Assigned global host_fetch_dest: {host_fetch_dest}", RNS.LOG_DEBUG)

        else:
             # Should not happen if command requires reticulum, but defensive check
             if args.command in ["search", "install", "update"]:
                  print(colors.error("Error: Reticulum not initialized, cannot proceed with network operations."), file=sys.stderr)
                  sys.exit(1)


    # DEBUG: Check state before calling handler
    print(f"DEBUG: Before calling '{args.command}': host_metadata_dest is {host_metadata_dest}, host_fetch_dest is {host_fetch_dest}", file=sys.stderr)

    # Execute command function
    try:
        result = args.func(args)
        sys.exit(result)
    except KeyboardInterrupt:
        print(f"\n{colors.warning('Operation cancelled by user.')}")
        exit_event.set() # Signal threads
        # Teardown links if they exist
        if metadata_link: metadata_link.teardown()
        if fetch_link: fetch_link.teardown()
        if current_fetch_resource: current_fetch_resource.cancel()
        sys.exit(130) # Exit code for Ctrl+C
    except Exception as e:
        print(f"\n{colors.error('An unexpected error occurred:')} {e}", file=sys.stderr)
        RNS.log(f"rns-pkg failed with unhandled exception: {e}", RNS.LOG_CRITICAL)
        import traceback
        traceback.print_exc()
        exit_event.set()
        sys.exit(2)
    finally:
        # Ensure reticulum instance is cleaned up if it was created
        pass

if __name__ == "__main__":
    main() 
