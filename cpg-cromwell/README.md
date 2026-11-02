# cpg-cromwell

A Hail-free client and CLI for the CPG Cromwell server. It submits a WDL, watches it, aborts it on
SIGTERM, re-attaches on relaunch and copies outputs to their destinations. It runs from a Nextflow
process, a GCP Batch task or plain Python. **Start with the [user guide](USER_GUIDE.md).** The full
specification is [`CROMWELL_CLI_DESIGN.md`](../CROMWELL_CLI_DESIGN.md).

It produces **the same workflow options** as `cpg_utils.cromwell.run_cromwell_workflow`, but never
imports that module. A fixture generated from the old function enforces this (see
[`parity/`](../parity)).

## Quick start

```bash
# 1. bundle the WDL once per commit (non-test access levels require the commit to be on main)
cpg-cromwell bundle --repo gatk-sv --commit <40-char sha> --cwd wdl \
    --dest gs://cpg-<dataset>-<ns>-tmp/gatk_sv_nf/bundles --access-level standard

# 2. describe the workflow in a spec file, check what will be sent
cpg-cromwell render --spec spec.json

# 3. run it
cpg-cromwell run --spec spec.json --reattach-root gs://.../cromwell_ids --outputs-json outputs.json
```

`spec.json`:

```json
{
  "name": "FormatVcfForGatk/my-dataset",
  "source": {"bundle": "gs://.../bundles/gatk-sv/<sha>/", "workflow": "FormatVcfForGatk.wdl"},
  "inputs": {"vcf": "gs://.../in.vcf.gz"},
  "input_prefix": "FormatVcfForGatk",
  "dataset": "my-dataset",
  "access_level": "test",
  "output_prefix": "gatk_sv/output/FormatVcfForGatk/my-dataset",
  "ar_guid": "0f8e...",
  "labels": {"stage": "formatvcfforgatk"},
  "collect": {"gatk_formatted_vcf": "gs://.../out.vcf.gz"},
  "poll": {"min_seconds": 30, "max_seconds": 140, "jitter": true, "time_limit_seconds": 86400}
}
```

Storage paths and the default `google_project` come from the cpg-utils config (`CPG_CONFIG_PATH`).
The AR GUID is **only** taken from the spec's `ar_guid`, never from the config or environment.

## Python API

```python
from cpg_cromwell import WorkflowSpec, run, run_many

result = run(spec, reattach_root='gs://.../cromwell_ids')
results = run_many(specs, reattach_root=..., results_dir=..., on_result=register)
```

`on_result` is called once per workflow, on the loop thread, as soon as its result is final (after
collection). If it raises, that workflow's result becomes a failure (exit 1). It is not called for
workflows stopped by a signal.

## Exit codes

| Code | Meaning | Retry? |
|---|---|---|
| 0 | Succeeded, outputs collected | – |
| 1 | Workflow Failed, or Aborted by someone else; strict collection failed | no |
| 2 | Spec/usage error, invalid labels, bundle not verified on main, secret or config missing, Cromwell rejected the submission | no |
| 75 | Transient: Cromwell unreachable, token minting failed, GCS 5xx | **yes** (re-attach makes it safe) |
| 124 | `time_limit_seconds` exceeded; workflow aborted | no |
| 143 / 130 | SIGTERM / SIGINT; unfinished workflows aborted first (left running with `--no-abort-on-exit`) | – |

`run-many` exits 0 if everything succeeded, otherwise the first of 143/130, 75, 2, 1, 124 that
occurred.

## Development

```bash
uv sync            # from the repo root (uv workspace)
uv run pytest cpg-cromwell/tests
```

The tests need neither Hail nor network access: they use a fake in-process Cromwell server and
local directories in place of GCS.
