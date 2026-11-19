#!/bin/sh
set -eu

if [ "$#" -ne 4 ]; then
  echo 'Usage: install.sh INSTALL_DIR HTTPS_API_URL TENANT API_KEY_FILE' >&2
  exit 2
fi

example=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
python3 - "$example" "$@" <<'PY'
from pathlib import Path
import re
import shlex
import shutil
import sys
from urllib.parse import urlsplit
import uuid

example, directory, url, tenant, key_path = sys.argv[1:]
address = urlsplit(url)
if (address.scheme != "https" or not address.hostname or address.username or address.password
        or address.query or address.fragment or any(char.isspace() for char in url)):
    raise SystemExit("Provide an HTTPS control-plane URL without credentials, query, or fragment.")
if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", tenant):
    raise SystemExit("Provide a tenant identifier.")
key_file = Path(key_path).resolve()
if not key_file.is_file() or key_file.stat().st_mode & 0o077 or not key_file.read_text().strip():
    raise SystemExit("Provide a private API key file, readable only by its owner.")

directory = Path(directory).resolve()
directory.mkdir(mode=0o700)
try:
    shutil.copyfile(Path(example) / "worker.py", directory / "worker.py")
    settings = {
        "ORCH8_URL": url,
        "ORCH8_TENANT_ID": tenant,
        "ORCH8_API_KEY_FILE": str(key_file),
        "ORCH8_WORKER_ID": str(uuid.uuid4()),
    }
    env = "\n".join(f"export {key}={shlex.quote(value)}" for key, value in settings.items())
    launcher = directory / "run.sh"
    launcher.write_text(
        "#!/bin/sh\nset -eu\n"
        'cd -- "$(dirname -- "$0")"\n'
        + env
        + '\nexec python3 worker.py "$@"\n',
        encoding="utf8",
    )
    launcher.chmod(0o700)
except OSError:
    shutil.rmtree(directory)
    raise

print(f"Private worker installed at {directory / 'run.sh'}")
PY
