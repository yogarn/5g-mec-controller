"""Create reproducible, bounded JPEG payloads from licensed local road images."""
import argparse
import hashlib
import json
from pathlib import Path

from PIL import Image, ImageOps


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--source-url', required=True)
    parser.add_argument('--license', required=True, help='dataset license or permission reference')
    parser.add_argument('--max-side', type=int, default=640)
    parser.add_argument('--quality', type=int, default=85)
    args = parser.parse_args()
    if not 32 <= args.max_side <= 4096 or not 1 <= args.quality <= 95:
        parser.error('max-side must be 32..4096 and quality 1..95')
    files = sorted(p for p in args.source.iterdir() if p.suffix.lower() in ('.jpg', '.jpeg', '.png'))
    if not files:
        parser.error('no JPG/PNG source images found')
    if args.output.exists():
        parser.error('output directory must not already exist')
    args.output.mkdir(parents=True)
    records = []
    for index, source in enumerate(files):
        raw = source.read_bytes()
        with Image.open(source) as image:
            original_size = image.size
            image = ImageOps.exif_transpose(image).convert('RGB')
            image.thumbnail((args.max_side, args.max_side), Image.Resampling.LANCZOS)
            target = args.output / f'{index:04d}.jpg'
            image.save(target, quality=args.quality, optimize=True)
            data = target.read_bytes()
            records.append({'source_name': source.name, 'source_sha256': hashlib.sha256(raw).hexdigest(),
                            'source_size': original_size, 'filename': target.name,
                            'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data), 'size': image.size})
    (args.output / 'provenance.json').write_text(json.dumps({
        'source_url': args.source_url, 'license': args.license, 'max_side': args.max_side,
        'jpeg_quality': args.quality, 'images': records}, indent=2) + '\n')
    print(f'Prepared {len(records)} images: {args.output}')


if __name__ == '__main__':
    main()
