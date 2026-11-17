"""Run one balanced mobility calibration phase per method."""
import sys

from experiments import campaign


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    original = campaign.MOBILITY9_SCHEDULE
    campaign.MOBILITY9_SCHEDULE = '9:3:3:3'
    extra = ([] if any(a == '--long-run' or a == '--phase-duration' or a.startswith('--phase-duration=')
                       for a in args)
             else ['--phase-duration', '60'])
    try:
        campaign.main(['--scenario', 'mobility9', '--fixed-replicas', '6',
                       '--tc-ms', '10', *extra, *args])
    finally:
        campaign.MOBILITY9_SCHEDULE = original


if __name__ == '__main__':
    main()
