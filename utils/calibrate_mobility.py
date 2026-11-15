"""Run one balanced mobility calibration phase per method."""
import sys

from experiments import campaign


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    original = campaign.MOBILITY9_SCHEDULE
    campaign.MOBILITY9_SCHEDULE = '9:3:3:3'
    try:
        campaign.main(['--scenario', 'mobility9', '--fixed-replicas', '6',
                       '--tc-ms', '10', *args])
    finally:
        campaign.MOBILITY9_SCHEDULE = original


if __name__ == '__main__':
    main()
