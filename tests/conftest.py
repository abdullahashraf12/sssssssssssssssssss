import os
import sys
from pathlib import Path

# Make the project root importable.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Stop sync.config from blowing up unit tests by setting required vars.
os.environ.setdefault("ZOHO_CLIENT_ID", "test-client")
os.environ.setdefault("ZOHO_CLIENT_SECRET", "test-secret")
os.environ.setdefault("ZOHO_REFRESH_TOKEN", "test-refresh")
