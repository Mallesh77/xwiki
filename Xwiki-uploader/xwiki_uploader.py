#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║            XWiki Unified Large-File Uploader  (Cloudflare Bypass)           ║
║  Runs on your LOCAL Mac. Copies files to EC2 via SCP, then uploads them     ║
║  to XWiki via localhost:8080 — completely bypassing Cloudflare's 100MB cap. ║
╚══════════════════════════════════════════════════════════════════════════════╝

USAGE:
  python3 xwiki_uploader.py            → show this help message
  python3 xwiki_uploader.py --dry-run  → preview + optional 1-file pipeline test
  python3 xwiki_uploader.py --upload   → live upload of all files

PREREQUISITES:
  pip3 install python-dotenv requests
  cp .env.example .env                 → then fill in your real values

WHAT THIS SCRIPT DOES (in order):
  1. Reads all config from .env (no hardcoded secrets)
  2. Interactively asks you to paste your XWiki page tree (sidebar/breadcrumb view)
     and pick the destination folder name from that tree
  3. Parses the tree to build the exact nested XWiki space path
  4. Walks LOCAL_SOURCE_PATH and maps each local file → XWiki page URL
  5. For each XWiki page, checks if it exists; creates it if not
  6. SCPs each file to EC2, then runs a remote Python snippet over SSH to PUT
     the file to XWiki via localhost:8080/xwiki (bypasses Cloudflare entirely)
  7. After all uploads, verifies every file exists on XWiki via the REST API
  8. Writes a verification report to logs/verification_YYYY-MM-DD.txt
  9. Writes a full upload log to logs/upload_YYYY-MM-DD_HH-MM-SS.log
"""

import os
import re
import sys
import subprocess
import shlex
import urllib.parse
import html
import time
import json
import csv
from typing import Dict, List, Optional, Set, Tuple, Union
import logging
import argparse
from datetime import datetime
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# 0.  DEPENDENCY CHECK – tell user clearly if a package is missing
# ─────────────────────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
except ImportError:
    print("ERROR: 'python-dotenv' is not installed.")
    print("  Fix: pip3 install python-dotenv requests")
    sys.exit(1)

try:
    import requests
except ImportError:
    print("ERROR: 'requests' is not installed.")
    print("  Fix: pip3 install python-dotenv requests")
    sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# 1.  LOGGING SETUP
#     Writes to console AND to a timestamped log file simultaneously.
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging() -> logging.Logger:
    """Create a logger that streams to stdout and to a timestamped log file."""
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file  = logs_dir / f"upload_{timestamp}.log"

    logger = logging.getLogger("xwiki_uploader")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File handler
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    logger.info(f"Log file: {log_file.resolve()}")
    return logger


LOG = setup_logging()


# ─────────────────────────────────────────────────────────────────────────────
# 2.  CONFIGURATION  (all values come from the .env file, never hardcoded)
# ─────────────────────────────────────────────────────────────────────────────

def load_config() -> dict:
    """
    Load and validate all required environment variables from the .env file.
    Exits with a clear error if any required variable is missing.

    TWO XWiki URLs are used intentionally:
      XWIKI_PUBLIC_URL  → e.g. https://msdocs.bootlabstech.com/xwiki
                          Used from your LOCAL Mac for page existence checks,
                          page creation, attachment checks, and verification.
                          These are small XML/HEAD requests — Cloudflare's
                          100 MB limit only applies to file uploads, not API calls.

      XWIKI_BASE_URL    → always http://localhost:8080/xwiki
                          Used ONLY inside the remote Python snippet that runs
                          ON EC2 via SSH. File bytes never touch Cloudflare.
    """
    load_dotenv()

    required = [
        "LOCAL_SOURCE_PATH",
        "EC2_HOST",
        "EC2_SSH_KEY",
        "EC2_TEMP_DIR",
        "XWIKI_PUBLIC_URL",   # public domain — for local REST checks/page creation
        "XWIKI_BASE_URL",     # localhost:8080  — used only inside EC2 remote snippet
        "JSESSIONID",
    ]

    config = {}
    missing = []
    for key in required:
        val = os.getenv(key, "").strip()
        if not val:
            missing.append(key)
        config[key] = val

    if missing:
        LOG.error("The following variables are missing from your .env file:")
        for m in missing:
            LOG.error(f"  • {m}")
        LOG.error("Copy .env.example → .env and fill in all values.")
        sys.exit(1)

    # Optional / boolean flags
    config["DRY_RUN"] = os.getenv("DRY_RUN", "false").strip().lower() == "true"

    return config



# ─────────────────────────────────────────────────────────────────────────────
# 3. DESTINATION PROMPT
# ─────────────────────────────────────────────────────────────────────────────

def prompt_destination_path() -> Tuple[List[str], str]:
    """
    Ask the user to paste the XWiki WebHome URL or type the destination path.
    """
    print("\n" + "=" * 70)
    print("  XWiki Destination Path")
    print("=" * 70)
    print(
        "\nEnter the destination path where files will be uploaded.\n"
        "Easiest option: Paste the browser URL of the target XWiki page.\n"
        "(e.g., https://your-xwiki.com/bin/view/Space/Subspace/Target/)\n\n"
        "Or enter the path manually separated by '/' (e.g., Space / Subspace / Target)\n"
    )

    raw = input("Destination URL or path: ").strip()

    if not raw:
        LOG.error("No destination entered. Exiting.")
        sys.exit(1)

    segments = []
    
    # Try parsing as URL
    if "/bin/view/" in raw:
        path_part = raw.split("/bin/view/")[-1]
        # Remove query params or trailing fragments
        path_part = path_part.split("?")[0].split("#")[0]
        # Remove trailing WebHome and slashes
        if path_part.endswith("/WebHome"):
            path_part = path_part[:-8]
        # Split by slash, decode url characters (spaces)
        segments = [urllib.parse.unquote(s).replace("+", " ").strip() for s in path_part.split("/") if s.strip()]
    else:
        # Fallback to manual path
        segments = [s.strip() for s in raw.split("/") if s.strip()]

    if not segments:
        LOG.error("Could not parse a valid destination from your input. Exiting.")
        sys.exit(1)

    dest_name = segments[-1]
    LOG.info(f"Destination path parsed → {' / '.join(segments)}")
    return segments, dest_name


# ─────────────────────────────────────────────────────────────────────────────
# 4.  XWIKI REST API HELPERS
#     All REST calls go to XWIKI_BASE_URL (localhost:8080 on EC2 via SSH
#     for uploads, but called from local for existence checks / page creation).
#
#     NOTE: The existence check and page creation use localhost via SSH tunnel
#     if needed, OR we use the public URL for those lightweight checks only.
#     Actual file bytes never cross Cloudflare — they go SCP → EC2 → localhost.
# ─────────────────────────────────────────────────────────────────────────────

def build_page_url(base_url: str, space_path: List[str], page_name: str = "WebHome") -> str:
    """
    Build a full XWiki REST API URL for a page.

    Example:
      base_url   = "http://localhost:8080/xwiki"
      space_path = ["Mirae Asset Cloud Managed Service", "MIrae Internal", "KT Recordings"]
      page_name  = "WebHome"
      → "http://localhost:8080/xwiki/rest/wikis/xwiki
           /spaces/Mirae%20Asset%20Cloud%20Managed%20Service
           /spaces/MIrae%20Internal
           /spaces/KT%20Recordings
           /pages/WebHome"
    """
    url = f"{base_url.rstrip('/')}/rest/wikis/xwiki"
    for segment in space_path:
        url += f"/spaces/{urllib.parse.quote(segment, safe='')}"
    url += f"/pages/{urllib.parse.quote(page_name, safe='')}"
    return url


def build_attachment_url(page_url: str, filename: str) -> str:
    """Build the REST URL for a specific attachment on a page."""
    return f"{page_url}/attachments/{urllib.parse.quote(filename, safe='')}"


def check_page_exists(page_url: str, cookies: dict, timeout: int = 15) -> bool:
    """Return True if the XWiki page exists (HTTP 200)."""
    try:
        r = requests.head(page_url, cookies=cookies, timeout=timeout)
        return r.status_code == 200
    except requests.RequestException as exc:
        LOG.warning(f"Could not check page existence ({exc}). Assuming it does not exist.")
        return False


def create_page(page_url: str, page_title: str, parent_spaces: List[str],
                cookies: dict, timeout: int = 30) -> bool:
    """
    Create a blank XWiki page using a PUT request with an XML body.

    The parent reference follows XWiki dot-notation:
      "Space1.Space2.WebHome"

    Uses html.escape() to safely embed the title in XML — prevents parse
    errors for titles with ampersands, angle brackets, etc.
    """
    safe_title  = html.escape(page_title)
    parent_ref  = ".".join(parent_spaces) + ".WebHome" if parent_spaces else "Main.WebHome"
    safe_parent = html.escape(parent_ref)

    xml_body = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<page xmlns="http://www.xwiki.org">\n'
        f'  <title>{safe_title}</title>\n'
        f'  <parent>{safe_parent}</parent>\n'
        f'  <content>Files from the {safe_title} folder.</content>\n'
        '</page>\n'
    )

    headers = {
        "Content-Type": "application/xml",
        "Accept":       "application/xml",
    }

    try:
        res = requests.put(page_url, data=xml_body.encode("utf-8"),
                           headers=headers, cookies=cookies, timeout=timeout)
        if res.status_code in (200, 201, 202):
            LOG.info(f"  [PAGE CREATED] '{page_title}'  (HTTP {res.status_code})")
            return True
        else:
            LOG.error(f"  [PAGE CREATE FAILED] '{page_title}'  "
                      f"HTTP {res.status_code}: {res.text[:200]}")
            return False
    except requests.RequestException as exc:
        LOG.error(f"  [PAGE CREATE ERROR] '{page_title}': {exc}")
        return False


def check_attachment_exists(page_url: str, filename: str, cookies: dict,
                             timeout: int = 15) -> bool:
    """Return True if the attachment already exists on the XWiki page."""
    attach_url = build_attachment_url(page_url, filename)
    try:
        r = requests.head(attach_url, cookies=cookies, timeout=timeout)
        return r.status_code == 200
    except requests.RequestException:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# 5.  SSH / SCP HELPERS
#     These run commands on the LOCAL machine that drive operations on EC2.
# ─────────────────────────────────────────────────────────────────────────────

def _ssh_base(cfg: dict) -> list[str]:
    """Return the common SSH invocation prefix with BatchMode and strict keys."""
    return [
        "ssh",
        "-i", cfg["EC2_SSH_KEY"],
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=no",
        cfg["EC2_HOST"],
    ]


def run_local(cmd: Union[List[str], str], capture: bool = False) -> subprocess.CompletedProcess:
    """
    Run a command on the LOCAL machine.
    Raises CalledProcessError on non-zero exit.
    """
    if isinstance(cmd, list):
        display = " ".join(cmd)
    else:
        display = cmd

    LOG.debug(f"LOCAL CMD: {display}")
    result = subprocess.run(
        cmd,
        shell=isinstance(cmd, str),
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
        text=True,
    )
    if result.returncode != 0:
        err = result.stderr.strip() if capture else "(see stderr above)"
        raise subprocess.CalledProcessError(result.returncode, display, output=result.stdout, stderr=err)
    return result


def ensure_remote_dir(cfg: dict) -> None:
    """Create the temporary staging directory on EC2 if it does not exist."""
    LOG.info(f"Ensuring remote temp directory exists: {cfg['EC2_TEMP_DIR']}")
    cmd = _ssh_base(cfg) + [f"mkdir -p '{cfg['EC2_TEMP_DIR']}'"]
    run_local(cmd)


def _safe_staging_name(filename: str) -> str:
    """
    Convert any filename into a remote-shell-safe staging name by replacing
    every character that is not alphanumeric, a dot, a hyphen, or an underscore
    with an underscore.

    Why this matters:
      Newer OpenSSH (9+) defaults to SFTP mode for SCP transfers. In SFTP mode
      the remote path is NOT passed through a shell — it goes straight to the
      SFTP server. This means shell quoting (e.g. shlex.quote) does NOT work;
      the single quotes become literal characters and the path is not found.

      By sanitising the staging name we guarantee a space-free and
      special-character-free path that works in ALL SCP/SFTP modes.

    The original filename is preserved for the XWiki attachment URL — this
    only affects the temporary name used while the file sits on EC2.

    Examples:
      'image (9) (1).png'  →  'image__9___1_.png'
      'KT Session 01.mp4'  →  'KT_Session_01.mp4'
    """
    return re.sub(r"[^a-zA-Z0-9.\-_]", "_", filename)


def scp_file_to_ec2(local_file: str, cfg: Dict) -> str:
    """
    Copy a single file from the local machine to EC2_TEMP_DIR via SCP.

    The destination uses a sanitised staging name (no spaces, no parens) to
    avoid SCP/SFTP path-interpretation issues. Returns the REMOTE path to the
    sanitised file so callers can open/delete it correctly on EC2.

    The original filename is NOT used on the EC2 side — only the XWiki
    attachment URL carries the real name.
    """
    safe_name   = _safe_staging_name(os.path.basename(local_file))
    remote_path = f"{cfg['EC2_TEMP_DIR']}/{safe_name}"

    cmd = [
        "scp",
        "-i", cfg["EC2_SSH_KEY"],   # key path may have spaces — safe as list element
        "-o", "StrictHostKeyChecking=no",
        local_file,                  # local path — safe as list element regardless of spaces
        f"{cfg['EC2_HOST']}:{remote_path}",  # remote path is now guaranteed space-free
    ]
    LOG.info(f"  SCP → EC2: {os.path.basename(local_file)} (staged as '{safe_name}')")
    run_local(cmd)
    return remote_path


def remote_upload_via_localhost(cfg: dict, remote_file: str, attach_url: str) -> bool:
    """
    SSH into EC2 and run a minimal Python snippet that PUTs the file to
    XWiki via http://localhost:8080 — this bypasses Cloudflare entirely.

    The snippet is passed as a heredoc to avoid shell quoting nightmares
    with filenames containing spaces or special characters.

    Returns True on success, False on failure.
    """
    # We build the Python snippet as a string and pass it over stdin via heredoc.
    # The filename and URL are JSON-encoded so they are safe regardless of
    # special characters (spaces, quotes, ampersands, etc.).
    json_url      = json.dumps(attach_url)
    json_path     = json.dumps(remote_file)
    json_session  = json.dumps(cfg["JSESSIONID"])

    python_snippet = f"""\
import subprocess, sys

url      = {json_url}
path     = {json_path}
session  = {json_session}

cmd = [
    "curl",
    "-s", "-S",           # silent progress, but show errors
    "-X", "PUT",
    "-T", path,           # Streams the file directly, minimal RAM
    "-H", f"Cookie: JSESSIONID={{session}}",
    "-H", "Accept: application/xml",
    "-w", "\\nHTTP_CODE:%{{http_code}}", # Append status code at the end
    url
]

try:
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    # Find HTTP_CODE in output
    out = res.stdout
    if "HTTP_CODE:200" in out or "HTTP_CODE:201" in out or "HTTP_CODE:202" in out:
        print('HTTP_STATUS:201')
    else:
        # It failed (like a 500 error). Print everything so we see the actual XWiki server error msg.
        print('HTTP_ERROR_DETAILS:')
        print(res.stdout)
        print(res.stderr)
        print('HTTP_ERROR:500')
        sys.exit(1)
except Exception as e:
    print('EXCEPTION:' + str(e))
    sys.exit(2)
"""

    # Pass the snippet via stdin so no quoting is needed in the ssh command.
    ssh_cmd = _ssh_base(cfg) + ["python3 -"]
    LOG.debug(f"  Remote Python snippet length: {len(python_snippet)} chars")

    try:
        result = subprocess.run(
            ssh_cmd,
            input=python_snippet,
            capture_output=True,
            text=True,
        )
        output = result.stdout.strip()
        LOG.debug(f"  Remote stdout: {output}")

        if result.returncode != 0:
            err = result.stderr.strip() or output
            LOG.error(f"  [REMOTE ERROR] {err}")
            return False

        if output.startswith("HTTP_STATUS:"):
            status = int(output.split(":")[1])
            if status in (200, 201, 202, 204):
                LOG.info(f"  [SUCCESS] Upload complete  (HTTP {status})")
                return True
            else:
                LOG.error(f"  [UPLOAD FAILED] XWiki returned HTTP {status}")
                return False
        elif output.startswith("HTTP_ERROR:"):
            LOG.error(f"  [UPLOAD FAILED] {output}")
            return False
        else:
            LOG.warning(f"  [UNEXPECTED OUTPUT] {output}")
            return False

    except Exception as exc:
        LOG.error(f"  [SSH EXCEPTION] {exc}")
        return False


def cleanup_remote_file(cfg: dict, remote_file: str) -> None:
    """Delete a single file from the EC2 staging directory.
    Remote path is always a safe sanitised name (no spaces), so no quoting needed.
    """
    cmd = _ssh_base(cfg) + [f"rm -f {remote_file}"]
    try:
        run_local(cmd, capture=True)
        LOG.debug(f"  Cleaned up remote file: {remote_file}")
    except subprocess.CalledProcessError as exc:
        LOG.warning(f"  Could not clean up remote file '{remote_file}': {exc.stderr}")


# ─────────────────────────────────────────────────────────────────────────────
# 6.  FILE DISCOVERY
#     Walk the local source path and build a list of (local_file, space_path)
#     tuples that map each file to its XWiki destination.
# ─────────────────────────────────────────────────────────────────────────────

def sanitize_segment(name: str) -> str:
    """
    Sanitize a folder name for use as an XWiki Space name.
    XWiki does not allow '&' in space names; replace with '_'.
    Strips leading/trailing whitespace.
    """
    return name.strip().replace("&", "_")


def collect_files(source_path: str, dest_space_path: List[str]) -> List[Dict]:
    """
    Walk source_path and build an ordered list of upload tasks.

    Each task is a dict:
      {
        "local_path":  "/Users/.../file.mp4",
        "filename":    "file.mp4",
        "size_bytes":  123456789,
        "size_mb":     117.7,
        "space_path":  ["Space1", "Space2", "SubFolder"],   # XWiki path for this file
      }

    If source_path is a single file, returns one task using dest_space_path.
    If source_path is a directory, recursively maps subdirectories to sub-spaces.
    """
    tasks = []

    if os.path.isfile(source_path):
        size = os.path.getsize(source_path)
        tasks.append({
            "local_path": source_path,
            "filename":   os.path.basename(source_path),
            "size_bytes": size,
            "size_mb":    size / (1024 * 1024),
            "space_path": dest_space_path,
        })

    elif os.path.isdir(source_path):
        for root, dirs, files in os.walk(source_path):
            # Skip hidden directories (e.g. .DS_Store parent dirs)
            dirs[:] = [d for d in dirs if not d.startswith(".")]

            rel = os.path.relpath(root, source_path)

            if rel == ".":
                current_space = dest_space_path
            else:
                sub_segments   = rel.split(os.sep)
                clean_segments = [sanitize_segment(s) for s in sub_segments]
                current_space  = dest_space_path + clean_segments

            visible_files = [f for f in files if not f.startswith(".")]
            for fname in visible_files:
                fpath = os.path.join(root, fname)
                try:
                    size = os.path.getsize(fpath)
                except OSError:
                    size = 0
                tasks.append({
                    "local_path": fpath,
                    "filename":   fname,
                    "size_bytes": size,
                    "size_mb":    size / (1024 * 1024),
                    "space_path": current_space,
                })
    else:
        LOG.error(f"LOCAL_SOURCE_PATH does not exist: {source_path}")
        sys.exit(1)

    return tasks


# ─────────────────────────────────────────────────────────────────────────────
# 7.  PAGE CREATION  (ensures all XWiki pages in the path exist)
# ─────────────────────────────────────────────────────────────────────────────

def ensure_pages_exist(space_path: List[str], cfg: Dict,
                        cookies: Dict, created_cache: Set) -> bool:
    """
    Walk every level of space_path and create any missing XWiki pages.
    All missing pages have already been confirmed by preflight_check_pages().
    This function only creates — no user interaction.
    """
    for depth in range(1, len(space_path) + 1):
        current   = space_path[:depth]
        cache_key = tuple(current)

        if cache_key in created_cache:
            continue  # Already confirmed/created this level

        page_url = build_page_url(cfg["XWIKI_PUBLIC_URL"], current)

        if check_page_exists(page_url, cookies):
            created_cache.add(cache_key)
            continue

        LOG.info(f"  Creating page: {' / '.join(current)}")
        parent  = current[:-1]
        success = create_page(page_url, current[-1], parent, cookies)

        if not success:
            LOG.error(f"  Failed to create page '{current[-1]}'. Cannot upload here.")
            return False

        created_cache.add(cache_key)

    return True


def preflight_check_pages(tasks: List[Dict], cfg: Dict, cookies: Dict) -> bool:
    """
    Pre-flight safety check: scan ALL XWiki pages needed by every task,
    show the user exactly what exists and what will be CREATED, then ask
    for explicit confirmation before any page is touched.

    Returns True if the user approves (or nothing needs creating).
    Returns False if the user cancels — caller must abort upload.

    Key safety rule:
      If the TOP-LEVEL space (the first segment of the path) does not exist
      in XWiki, it almost always means the destination path is WRONG — the
      user likely omitted parent spaces. A prominent warning is shown.
    """
    # Collect every ancestor level needed across ALL tasks
    all_paths: Set = set()
    for task in tasks:
        for depth in range(1, len(task["space_path"]) + 1):
            all_paths.add(tuple(task["space_path"][:depth]))

    sorted_paths = sorted(all_paths, key=lambda p: (len(p), p))

    LOG.info("")
    LOG.info("Pre-flight: checking XWiki destination pages...")

    existing = []
    missing  = []

    for path_tuple in sorted_paths:
        page_url = build_page_url(cfg["XWIKI_PUBLIC_URL"], list(path_tuple))
        if check_page_exists(page_url, cookies):
            existing.append(path_tuple)
            LOG.info(f"  EXISTS   ✓  {' / '.join(path_tuple)}")
        else:
            missing.append(path_tuple)
            LOG.warning(f"  MISSING  ✗  {' / '.join(path_tuple)}")

    # Nothing to create — all good, proceed silently
    if not missing:
        LOG.info("Pre-flight complete: all destination pages already exist. ✓")
        return True

    # ── Warn about what will be created ─────────────────────────────────
    LOG.warning("")
    LOG.warning("  ┌─ PAGES THAT WILL BE CREATED IN XWIKI ────────────────────────────")
    for m in missing:
        LOG.warning(f"  │  NEW  →  {' / '.join(m)}")
    LOG.warning("  └──────────────────────────────────────────────────────────────")

    # ── Critical warning: top-level space missing means path is likely wrong ──
    top_exists = {p[0] for p in existing}
    missing_tops = [m for m in missing if len(m) == 1 and m[0] not in top_exists]

    if missing_tops:
        LOG.warning("")
        LOG.warning("  ⚠️  DANGER: The following TOP-LEVEL XWiki space(s) do not exist:")
        for m in missing_tops:
            LOG.warning(f"         '{m[0]}'")
        LOG.warning("  ⚠️  This almost always means your destination PATH IS WRONG.")
        LOG.warning("  ⚠️  Example correct path:  Home / Managed Services / Projects / Mahindra Finance / New-MMFSL")
        LOG.warning("  ⚠️  If you proceed, a brand-new root space will be created in XWiki.")
        LOG.warning("  ⚠️  Type 'no' to cancel and re-run with the correct path.")

    LOG.warning("")
    confirm = input("  Create the above page(s) and continue? (yes/no): ").strip().lower()
    if confirm not in ("yes", "y"):
        LOG.info("  Cancelled. No pages were created and no files were uploaded.")
        return False

    return True


def batch_check_attachments_local(tasks: List[Dict], cfg: Dict, cookies: Dict) -> Dict:
    """
    Check existence of all attachments via local API GET requests.
    Querying /attachments endpoint for the parent page returns all attachments for that page.
    Returns dict mapping: (tuple(space_path), filename) -> True/False.
    """
    LOG.info("  Checking existing attachments via XWiki API locally...")
    result_map = {}
    
    # Group tasks by space_path so we only query each page once
    pages_to_check = {}
    for task in tasks:
        path_key = tuple(task["space_path"])
        if path_key not in pages_to_check:
            pages_to_check[path_key] = []
        pages_to_check[path_key].append(task["filename"])

    headers = {"Accept": "application/json"}
    
    for path_key, filenames in pages_to_check.items():
        page_url = build_page_url(cfg["XWIKI_PUBLIC_URL"], list(path_key))
        attachments_url = f"{page_url}/attachments"
        try:
            r = requests.get(attachments_url, headers=headers, cookies=cookies, timeout=15)
            if r.status_code == 200:
                data = r.json()
                existing_files = {att.get("name") for att in data.get("attachments", [])}
                for fname in filenames:
                    result_map[(path_key, fname)] = fname in existing_files
            else:
                for fname in filenames:
                    result_map[(path_key, fname)] = False
        except requests.RequestException as exc:
            LOG.warning(f"  Could not check attachments for {'/'.join(path_key)}: {exc}")
            for fname in filenames:
                result_map[(path_key, fname)] = False

    return result_map

# ─────────────────────────────────────────────────────────────────────────────
# 8.  SINGLE FILE UPLOAD  (SCP → remote PUT → cleanup)
# ─────────────────────────────────────────────────────────────────────────────

def upload_one_file(task: Dict, cfg: Dict, cookies: Dict,
                     dry_run: bool = False) -> str:
    """
    Upload a single file to XWiki.

    URL strategy:
      • pub_page_url  — built from XWIKI_PUBLIC_URL — used for local HEAD checks
                        (does the attachment already exist?) — runs on this Mac
      • loc_attach_url — built from XWIKI_BASE_URL (localhost:8080) — used only
                        inside the remote Python snippet that runs on EC2 via SSH

    Returns one of:
      "skipped"   — file already exists on XWiki
      "success"   — uploaded successfully
      "failed"    — upload failed after retries
    """
    local_path  = task["local_path"]
    filename    = task["filename"]
    space_path  = task["space_path"]
    size_mb     = task["size_mb"]

    # Public URL — used from local Mac for lightweight checks only
    pub_page_url   = build_page_url(cfg["XWIKI_PUBLIC_URL"], space_path)
    pub_attach_url = build_attachment_url(pub_page_url, filename)

    # Localhost URL — used only inside the EC2 remote snippet for the file PUT
    loc_page_url   = build_page_url(cfg["XWIKI_BASE_URL"], space_path)
    loc_attach_url = build_attachment_url(loc_page_url, filename)

    LOG.info(f"  File     : {filename}  ({size_mb:.2f} MB)")
    LOG.info(f"  Target   : {pub_page_url}")

    if dry_run:
        LOG.info("  DRY RUN  : WOULD UPLOAD")
        return "dry_upload"

    # ── SCP file to EC2 ─────────────────────────────────────────────────
    try:
        remote_file = scp_file_to_ec2(local_path, cfg)
    except subprocess.CalledProcessError as exc:
        LOG.error(f"  [FAILED] SCP failed for '{filename}': {exc.stderr}")
        return "failed"

    # ── Upload via localhost (runs on EC2, bypasses Cloudflare) ─────────
    MAX_RETRIES = 3
    for attempt in range(1, MAX_RETRIES + 1):
        if attempt > 1:
            LOG.info(f"  [RETRY {attempt}/{MAX_RETRIES}] Retrying '{filename}'...")
            time.sleep(3)  # Brief pause before retry

        # loc_attach_url uses localhost — only valid inside EC2
        success = remote_upload_via_localhost(cfg, remote_file, loc_attach_url)

        if success:
            cleanup_remote_file(cfg, remote_file)
            return "success"
        else:
            LOG.warning(f"  Attempt {attempt}/{MAX_RETRIES} failed for '{filename}'.")

    # All retries exhausted — clean up and report failure
    cleanup_remote_file(cfg, remote_file)
    LOG.error(f"  [FAILED] '{filename}' failed after {MAX_RETRIES} attempts.")
    return "failed"


# ─────────────────────────────────────────────────────────────────────────────
# 9.  DRY-RUN MODE
# ─────────────────────────────────────────────────────────────────────────────

def run_dry_run(tasks: List[Dict], dest_path: List[str], cfg: Dict, cookies: Dict) -> None:
    """
    Phase 1: Print a full preview of what would happen.
    Phase 2: Optionally run a real end-to-end test with the smallest file.
    """
    LOG.info("=" * 65)
    LOG.info("  DRY RUN MODE — No files will be uploaded in Phase 1")
    LOG.info("=" * 65)
    LOG.info(f"  Destination XWiki path : {' / '.join(dest_path)}")
    LOG.info(f"  Total files found      : {len(tasks)}")
    LOG.info("")

    exists_map = batch_check_attachments_local(tasks, cfg, cookies)

    # Sort for consistent display
    sorted_tasks = sorted(tasks, key=lambda t: t["local_path"])

    would_skip   = 0
    would_upload = 0

    for i, task in enumerate(sorted_tasks, 1):
        key = (tuple(task["space_path"]), task["filename"])
        exists = exists_map.get(key, False)
        
        page_url = build_page_url(cfg["XWIKI_PUBLIC_URL"], task["space_path"])
        action   = "SKIP (exists)" if exists else "UPLOAD"
        
        if exists:
            would_skip += 1
        else:
            would_upload += 1

        LOG.info(
            f"  [{i:>3}] {action:<18}  {task['filename']}  "
            f"({task['size_mb']:.2f} MB)  →  {page_url}"
        )

    LOG.info("")
    LOG.info(f"  Summary: {would_upload} would upload, {would_skip} would skip.")
    LOG.info("=" * 65)

    # Phase 2 — optional real pipeline test
    print("\nPhase 2 — Sample File Test")
    print("Do you want to test the full upload pipeline with the SMALLEST file?")
    print("This will SCP the file to EC2 and upload it via localhost:8080 to confirm")
    print("that SSH, SCP, and XWiki authentication all work end-to-end.")
    choice = input("Run sample test? (yes/no): ").strip().lower()

    if choice not in ("yes", "y"):
        LOG.info("Sample test skipped. Dry run complete.")
        return

    # Pick the smallest file
    smallest = min(tasks, key=lambda t: t["size_bytes"])
    LOG.info(f"\nSample file selected: '{smallest['filename']}' ({smallest['size_mb']:.2f} MB)")
    LOG.info("Running full pipeline on this one file...")

    # Step A: Pre-flight check — shows what pages exist/missing, asks confirmation
    if not preflight_check_pages([smallest], cfg, cookies):
        LOG.info("Sample test cancelled.")
        return

    # Step B: Create the EC2 staging directory (must happen before SCP)
    try:
        ensure_remote_dir(cfg)
    except subprocess.CalledProcessError as exc:
        LOG.error(f"Could not create staging directory on EC2: {exc}")
        LOG.error("Check your EC2_SSH_KEY path and EC2_HOST in .env")
        return

    # If it already exists, report it
    key = (tuple(smallest["space_path"]), smallest["filename"])
    if exists_map.get(key, False):
        LOG.info(f"  [SKIPPED] '{smallest['filename']}' already exists on XWiki. Pipeline confirmed OK!")
        return

    # Step C: Ensure the destination XWiki page exists (silent — preflight already confirmed)
    created_cache: set = set()
    page_ok = ensure_pages_exist(smallest["space_path"], cfg, cookies, created_cache)
    if not page_ok:
        LOG.error("Could not create destination page. Test aborted.")
        return

    # Step D: Run the full upload for just this one file
    result = upload_one_file(smallest, cfg, cookies, dry_run=False)

    if result == "success":
        LOG.info("[SAMPLE TEST PASSED] Full pipeline works! SSH ✓  SCP ✓  XWiki API ✓")
        LOG.info("You can now run: python3 xwiki_uploader.py --upload")
    elif result == "skipped":
        LOG.info("[SAMPLE TEST INFO] File already existed on XWiki — pipeline connectivity is confirmed OK.")
    else:
        LOG.error("[SAMPLE TEST FAILED] Something went wrong. Review the error messages above.")


# ─────────────────────────────────────────────────────────────────────────────
# 10.  POST-UPLOAD VERIFICATION & CSV EXPORT
# ─────────────────────────────────────────────────────────────────────────────

def verify_uploads(tasks: List[Dict], cfg: Dict, cookies: Dict) -> None:
    """
    After all uploads, verify each file exists on XWiki via the REST API.
    Saves a text verification report and a CSV summary data dump.
    """
    LOG.info("")
    LOG.info("=" * 65)
    LOG.info("  POST-UPLOAD VERIFICATION")
    LOG.info("=" * 65)

    total      = len(tasks)
    verified   = []
    missing    = []

    # Run a single batch check via public API
    exists_map = batch_check_attachments_local(tasks, cfg, cookies)

    for task in tasks:
        key = (tuple(task["space_path"]), task["filename"])
        exists = exists_map.get(key, False)
        task["exists_in_xwiki"] = exists  # Cache the result for CSV
        
        # for logging display, show the public page URL
        page_url = build_page_url(cfg["XWIKI_PUBLIC_URL"], task["space_path"])
        
        # Track items that were supposedly uploaded or skipped during this run
        if exists:
            verified.append(task)
            if task.get("status") == "success":
                LOG.info(f"  [VERIFIED]  {task['filename']}")
            elif task.get("status") == "skipped":
                LOG.info(f"  [CONFIRMED] {task['filename']} (Already existed)")
        else:
            missing.append(task)
            LOG.warning(f"  [MISSING]   {task['filename']}  →  {page_url}")

    LOG.info("")
    LOG.info(f"  Verification complete: {len(verified)}/{total} present on XWiki.")
    
    # Check if there are tasks that were marked successful but now missing
    missing_success = [t for t in missing if t.get("status") == "success"]
    if missing_success:
        LOG.error(f"  {len(missing_success)} file(s) uploaded successfully but could NOT be found on XWiki!")

    # ── Write verification text report & CSV ────────────────────────────────
    logs_dir  = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    date_str  = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report    = logs_dir / f"verification_{date_str}.txt"
    csv_file  = logs_dir / f"report_{date_str}.csv"

    with open(report, "w", encoding="utf-8") as fh:
        fh.write("XWiki Upload Verification Report\n")
        fh.write(f"Generated : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        fh.write("=" * 60 + "\n\n")
        fh.write(f"Total processed : {total}\n")
        fh.write(f"Verified found  : {len(verified)}\n")
        fh.write(f"Missing         : {len(missing)}\n\n")

        fh.write("VERIFIED FILES:\n")
        for t in verified:
            fh.write(f"  ✓  {t['filename']}\n")

        if missing:
            fh.write("\nMISSING FILES (not found on XWiki after upload):\n")
            for t in missing:
                page_url = build_page_url(cfg["XWIKI_PUBLIC_URL"], t["space_path"])
                fh.write(f"  ✗  {t['filename']}\n")
                fh.write(f"       Target URL: {page_url}\n")
                fh.write(f"       Operation Status: {t.get('status', 'Unknown')}\n")

    with open(csv_file, "w", newline="", encoding="utf-8") as cf:
        writer = csv.writer(cf)
        writer.writerow([
            "Filename", "Size_MB", "Destination_Path", 
            "Target_URL", "Operation_Status", "Verification_Status"
        ])
        for t in tasks:
            page_url = build_page_url(cfg["XWIKI_PUBLIC_URL"], t["space_path"])
            dest_str = " / ".join(t["space_path"])
            verify_str = "Found" if t.get("exists_in_xwiki") else "Missing"
            status_str = t.get("status", "pending").capitalize()
            writer.writerow([
                t["filename"], 
                f"{t.get('size_mb', 0):.2f}", 
                dest_str, 
                page_url, 
                status_str, 
                verify_str
            ])

    LOG.info(f"  Text report saved → {report.resolve()}")
    LOG.info(f"  CSV report saved  → {csv_file.resolve()}")


# ─────────────────────────────────────────────────────────────────────────────
# 11.  LIVE UPLOAD MODE
# ─────────────────────────────────────────────────────────────────────────────

def run_upload(tasks: List[Dict], dest_path: List[str], cfg: Dict, cookies: Dict) -> None:
    """Run the full live upload pipeline for all discovered files."""
    LOG.info("=" * 65)
    LOG.info("  LIVE UPLOAD MODE")
    LOG.info(f"  Destination : {' / '.join(dest_path)}")
    LOG.info(f"  Total files : {len(tasks)}")
    LOG.info("=" * 65)

    # ── Pre-flight: show what pages will be created, ask for confirmation ─
    if not preflight_check_pages(tasks, cfg, cookies):
        return  # User cancelled or path is wrong

    total    = len(tasks)
    success  = 0
    skipped  = 0
    failed   = []

    # Ensure remote staging directory is ready
    ensure_remote_dir(cfg)

    # Pre-check all attachments to easily skip existing ones
    exists_map = batch_check_attachments_local(tasks, cfg, cookies)

    # Cache of confirmed pages to avoid redundant API checks
    created_pages: set = set()

    for i, task in enumerate(tasks, 1):
        filename = task["filename"]
        LOG.info("")
        LOG.info(f"[{i}/{total}] Processing: {filename}")

        key = (tuple(task["space_path"]), filename)
        if exists_map.get(key, False):
            LOG.info(f"  [SKIPPED] '{filename}' already exists on XWiki.")
            skipped += 1
            continue

        # Ensure all ancestor pages exist before uploading
        page_ok = ensure_pages_exist(task["space_path"], cfg, cookies, created_pages)
        if not page_ok:
            LOG.error(f"  Skipping '{filename}' — could not create destination page.")
            failed.append({"filename": filename, "reason": "Page creation failed"})
            continue

        result = upload_one_file(task, cfg, cookies, dry_run=False)
        task["status"] = result

        if result == "success":
            success += 1
        elif result == "skipped":
            skipped += 1
        else:
            failed.append({"filename": filename, "reason": "Upload failed after 3 retries"})

    # ── Final summary ────────────────────────────────────────────────────
    LOG.info("")
    LOG.info("=" * 65)
    LOG.info("  UPLOAD SUMMARY")
    LOG.info("=" * 65)
    LOG.info(f"  Total files found       : {total}")
    LOG.info(f"  Successfully uploaded   : {success}")
    LOG.info(f"  Skipped (already exist) : {skipped}")
    LOG.info(f"  Failed                  : {len(failed)}")
    if failed:
        LOG.warning("  Failed files:")
        for f in failed:
            LOG.warning(f"    • {f['filename']}  —  {f['reason']}")
    LOG.info("=" * 65)

    # Ensure any tasks that were skipped explicitly also get a status for the CSV
    for t in tasks:
        if "status" not in t:
            # If they didn't get a status, they were likely failed or never matched
            fail_record = next((f for f in failed if f["filename"] == t["filename"]), None)
            if fail_record:
                t["status"] = "failed"
            else:
                t["status"] = "skipped"

    verify_uploads(tasks, cfg, cookies)


# ─────────────────────────────────────────────────────────────────────────────
# 12.  HELP / USAGE
# ─────────────────────────────────────────────────────────────────────────────

HELP_TEXT = """
╔══════════════════════════════════════════════════════════════════╗
║           XWiki Unified Uploader  —  Usage Guide                ║
╚══════════════════════════════════════════════════════════════════╝

 COMMANDS:
   python3 xwiki_uploader.py            Show this help message
   python3 xwiki_uploader.py --dry-run  Preview all uploads (safe, no data sent)
   python3 xwiki_uploader.py --upload   Live upload (SCP + REST API via localhost)

 SETUP (first time):
   1. pip3 install python-dotenv requests
   2. cp .env.example .env
   3. Edit .env and fill in all values (SSH key, EC2 host, JSESSIONID, etc.)
   4. Run --dry-run first to confirm the destination tree mapping is correct.
   5. Run --upload when you are ready.

 HOW IT WORKS:
   • Files are copied to EC2 via SCP (no Cloudflare, no size limit)
   • On EC2, a Python snippet uploads via http://localhost:8080 (bypasses CF)
   • Pages are auto-created in XWiki if they do not exist
   • After upload, each file is verified via the REST API
   • All output is logged to logs/upload_YYYY-MM-DD_HH-MM-SS.log

 GETTING YOUR JSESSIONID:
   1. Open XWiki in your browser and log in (SSO is fine)
   2. Press F12 → Application tab → Cookies
   3. Find JSESSIONID and copy its value into .env

 XWIKI DESTINATION PATH (what you type when prompted):
   Enter the path as slash-separated levels matching your XWiki sidebar.
   Examples:
     KT Recordings
     Mirae Asset Cloud Managed Service / MIrae Internal / KT Recordings
     Home / Managed Services / Projects / Mahindra Finance / New-MMFSL

   Tip: Copy the breadcrumb trail from your XWiki page and replace '>' with '/'
"""


# ─────────────────────────────────────────────────────────────────────────────
# 13.  ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="XWiki Unified Large-File Uploader (Cloudflare Bypass)",
        add_help=False,
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview mode")
    parser.add_argument("--upload",  action="store_true", help="Live upload mode")
    parser.add_argument("--help",    action="store_true",  help="Show help")
    args = parser.parse_args()

    # Show help if no arguments or --help
    if args.help or (not args.dry_run and not args.upload):
        print(HELP_TEXT)
        sys.exit(0)

    # ── Load config ──────────────────────────────────────────────────────
    cfg = load_config()

    # CLI --dry-run flag overrides .env DRY_RUN value
    is_dry_run = args.dry_run or cfg["DRY_RUN"]

    # ── Log startup ──────────────────────────────────────────────────────
    LOG.info("=" * 65)
    LOG.info("  XWiki Unified Uploader started")
    LOG.info(f"  Mode            : {'DRY RUN' if is_dry_run else 'LIVE UPLOAD'}")
    LOG.info(f"  Source path     : {cfg['LOCAL_SOURCE_PATH']}")
    LOG.info(f"  EC2 host        : {cfg['EC2_HOST']}")
    LOG.info(f"  Public URL      : {cfg['XWIKI_PUBLIC_URL']}  (local REST checks)")
    LOG.info(f"  Localhost URL   : {cfg['XWIKI_BASE_URL']}  (EC2 file upload only)")
    LOG.info(f"  Start time      : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    LOG.info("=" * 65)

    # ── Session cookie (used for page checks and page creation from local) ──
    cookies = {"JSESSIONID": cfg["JSESSIONID"]}

    # ── Destination path input ───────────────────────────────────────────
    dest_space_path, dest_name = prompt_destination_path()

    LOG.info(f"Destination confirmed:")
    LOG.info(f"  Name  : {dest_name}")
    LOG.info(f"  Path  : {' / '.join(dest_space_path)}")

    # ── Collect files ────────────────────────────────────────────────────
    tasks = collect_files(cfg["LOCAL_SOURCE_PATH"], dest_space_path)

    if not tasks:
        LOG.warning("No files found in LOCAL_SOURCE_PATH. Nothing to do.")
        sys.exit(0)

    LOG.info(f"Files discovered: {len(tasks)}")
    total_size_mb = sum(t["size_mb"] for t in tasks)
    LOG.info(f"Total size      : {total_size_mb:.2f} MB")

    # ── Execute chosen mode ──────────────────────────────────────────────
    if is_dry_run:
        run_dry_run(tasks, dest_space_path, cfg, cookies)
    else:
        run_upload(tasks, dest_space_path, cfg, cookies)

    LOG.info(f"\nScript finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
