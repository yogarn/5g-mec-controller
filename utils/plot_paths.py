"""Shared locations for plot images and their separate tabular exports."""
from pathlib import Path


def default_csv_dir(output_dir):
    output_dir = Path(output_dir)
    if not output_dir.name:
        output_dir = output_dir.resolve()
    name = output_dir.name
    return output_dir.with_name('csv' + name[5:] if name == 'plots' or name.startswith('plots-') else name + '-csv')
