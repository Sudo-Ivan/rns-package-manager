import os

"""
Constants and configurations for the RNS package manager.

This module defines various constants and file paths used throughout the
package manager for managing package metadata, fetching packages,
configuring timeouts, and managing local storage.
"""

APP_NAME = "rns_package_manager"
METADATA_FORMAT_VERSION = 1

METADATA_ASPECT = f"metadata/v{METADATA_FORMAT_VERSION}"
FETCH_ASPECT = f"package/fetch/v{METADATA_FORMAT_VERSION}"

DEFAULT_TIMEOUT = 120.0

RNS_PKG_BASE_DIR = os.path.expanduser("~/.local/share/rns-pkg")
RNS_STORE_DIR = os.path.join(RNS_PKG_BASE_DIR, "store")
RNS_CACHE_DIR = os.path.join(RNS_PKG_BASE_DIR, "cache")
LOCAL_STATE_DB = os.path.join(RNS_PKG_BASE_DIR, "installed.json")
HOST_PROFILES_DB = os.path.join(RNS_PKG_BASE_DIR, "hosts.json")

PACKAGE_METADATA_ASPECT = METADATA_ASPECT
PACKAGE_DOWNLOAD_ASPECT = FETCH_ASPECT
METADATA_FILENAME = "package_metadata.json"
