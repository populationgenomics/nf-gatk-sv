#!/usr/bin/env python3
"""
P3, new side (NEXTFLOW_PORT_PLAN.md §10): `nf-gatk-sv stage <Stage> --dump-inputs`, run in-process.

    uv run python parity/p3_new_side.py [--existing existing.json] -- \\
        stage GatherSampleEvidence --plan <plan_dir> --chunk <chunk.json> --ar-guid <guid> --dump-inputs new.json

Everything after `--` goes to the CLI unchanged. With --existing, gs:// existence checks answer from that JSON list
of paths instead of GCS, as the old side's --existing does, so a comparison can run offline. Without it, this is
the same as running the CLI.
"""

import argparse
import json
import sys
from pathlib import Path

from gatk_sv_nf import cli
from gatk_sv_nf import storage as gcs_storage


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--existing', help='JSON list of gs:// paths to treat as existing; default: check GCS')
    parser.add_argument('cli_args', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    cli_args = args.cli_args[1:] if args.cli_args[:1] == ['--'] else args.cli_args

    if args.existing:
        existing = set(json.loads(Path(args.existing).read_text()))
        real_exists = gcs_storage.Storage.exists

        def exists(self: gcs_storage.Storage, uri: str) -> bool:
            return uri in existing if gcs_storage.is_gcs(uri) else real_exists(self, uri)

        gcs_storage.Storage.exists = exists  # type: ignore[method-assign]

    return cli.main(cli_args)


if __name__ == '__main__':
    sys.exit(main())
