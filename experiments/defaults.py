"""Convenient defaults for the two-gNB testbed; verify before any campaign."""
import os
from pathlib import Path
import pwd


def user_home():
    # sudo normally changes HOME; inventory and fixtures belong to the invoking user.
    name = os.environ.get('SUDO_USER')
    return Path(pwd.getpwnam(name).pw_dir) if name else Path.home()


HOME = user_home()
INVENTORY = HOME / '.config/5g-mec/ue-inventory.yaml'
IMAGE = Path(__file__).resolve().parent.parent / 'apps/face-detection/images'
NR_UE = HOME / 'ueransim/build/nr-ue'
SCHEDULE = '1:1:0,1:0:1'
PHASE_DURATION = 30.0
SLO_MS = 500.0
RESULTS = Path('results')
