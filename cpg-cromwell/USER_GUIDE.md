# cpg-cromwell user guide

`cpg-cromwell` runs WDL workflows on the CPG Cromwell server from anywhere Python runs: a Nextflow
process, a GCP Batch task, or a script. It submits the workflow, watches it, copies its outputs to
where you want them, and reports what happened through its exit code.

It does not need Hail. The workflow options it sends to Cromwell (service account, log
directories, GCS root and labels) are the same as those from `cpg_utils.cromwell.run_cromwell_workflow`, so
nothing changes on the server side.

This guide covers how to use it. For the reasons behind each choice, see
[`CROMWELL_CLI_DESIGN.md`](../CROMWELL_CLI_DESIGN.md).

- [Before you start](#before-you-start)
- [The workflow in three steps](#the-workflow-in-three-steps)
- [Step 1: bundle the WDL](#step-1-bundle-the-wdl)
- [Step 2: write a spec](#step-2-write-a-spec)
- [Step 3: run it](#step-3-run-it)
- [Running many workflows](#running-many-workflows)
- [Relaunching safely: re-attach](#relaunching-safely-re-attach)
- [Collecting outputs](#collecting-outputs)
- [Stopping a run: signals](#stopping-a-run-signals)
- [Exit codes](#exit-codes)
- [Inspecting workflows by hand](#inspecting-workflows-by-hand)
- [Python API](#python-api)
- [Troubleshooting](#troubleshooting)

## Before you start

**Install.** In this repo it is a uv workspace member, so `uv sync` at the repo root installs it and
the `cpg-cromwell` command. It depends on the core `cpg-utils` install only; Hail is not needed.

**Credentials.** It uses the ambient Google credentials of wherever it runs (the VM's attached
service account, or your `gcloud auth application-default` login). Those credentials need to:

- mint an ID token for the Cromwell audience;
- read the Secret Manager secret `{dataset}-cromwell-{access_level}-key` in the dataset's GCP
  project. That key is the service account Cromwell runs the workflow as;
- read the bundle, and read and write the re-attach and output locations in GCS.

**Config.** Set `CPG_CONFIG_PATH` to a cpg-utils config that has:

```toml
[workflow]
dataset_gcp_project = "my-project"   # where the secret lives; default google_project
access_level = "standard"            # used by `bundle` when --access-level is not given

[storage.my-dataset]
default  = "gs://cpg-my-dataset-main"
analysis = "gs://cpg-my-dataset-main-analysis"
tmp      = "gs://cpg-my-dataset-main-tmp"
```

**Cromwell server.** The URL and audience default to the CPG server. Override them with the
`CROMWELL_URL` and `CROMWELL_AUDIENCE` environment variables.

## The workflow in three steps

```bash
# 1. once per commit: put the WDL somewhere Cromwell submissions can read it from
cpg-cromwell bundle --repo gatk-sv --commit <40-char sha> --cwd wdl \
    --dest gs://cpg-my-dataset-main-tmp/gatk_sv_nf/bundles

# 2. describe one workflow in a JSON spec, then check what would be sent
cpg-cromwell render --spec spec.json

# 3. submit, watch, collect
cpg-cromwell run --spec spec.json \
    --reattach-root gs://cpg-my-dataset-main-tmp/gatk_sv_nf/cromwell_ids \
    --outputs-json outputs.json
```

## Step 1: bundle the WDL

A **bundle** is a snapshot of a repo's WDL at one commit, stored in GCS. You make it once per
commit and every workflow run from that commit reuses it.

```bash
cpg-cromwell bundle \
    --repo gatk-sv \
    --commit 0123456789abcdef0123456789abcdef01234567 \
    --cwd wdl \
    --dest gs://cpg-my-dataset-main-tmp/gatk_sv_nf/bundles \
    --access-level standard
```

It prints the bundle URI, e.g. `gs://.../bundles/gatk-sv/<sha>/`. Put that in your spec.

| Option | Meaning |
|---|---|
| `--repo` | Repository name under `--org` (default `populationgenomics`) |
| `--commit` | **Full 40-character SHA.** Branch names and short SHAs are rejected, because the bundle path and re-attach key need an identity that can't change |
| `--cwd` | Directory inside the repo that holds the WDL. Every `.wdl` under it is uploaded, keeping its relative path |
| `--lib` | Path relative to `--cwd` to include in `deps.zip` (the imports Cromwell resolves). Repeatable. Default `.` (everything under `--cwd`) |
| `--dest` | Where bundles live. Use a bucket only your access level can write to, such as the dataset's tmp bucket |
| `--access-level` | Defaults to `workflow.access_level` from the config. If neither is set, the strict rule applies |

**The main-branch rule.** For every access level except `test`, the commit must already be merged
into `main`. `bundle` checks this and records the result in the bundle's `bundle.json`. `run`
checks the record again before submitting. With `--access-level test` you can bundle any commit,
but the bundle is marked as not verified and can't be submitted at other access levels.

**It is safe to run again.** If a complete bundle already exists at that path, `bundle` returns
straight away. If a bundle was made at `test` level and the commit has since been merged, running
`bundle` again at a non-test level verifies it and rebuilds it. Asking for the same commit with a different `--cwd`, `--lib`
or `--org` is an error, so a bundle can't change underneath you.

Private repositories are cloned with the git credentials secret named by
`[infrastructure].git_credentials_secret_name` and `git_credentials_secret_project` in the config,
the same source the old `prepare_git_job` used.

## Step 2: write a spec

A **spec** is a JSON file describing one workflow run.

```json
{
  "name": "FormatVcfForGatk/my-dataset",
  "source": {
    "bundle": "gs://cpg-my-dataset-main-tmp/gatk_sv_nf/bundles/gatk-sv/<sha>/",
    "workflow": "FormatVcfForGatk.wdl"
  },
  "inputs": {"vcf": "gs://cpg-my-dataset-main/in.vcf.gz"},
  "input_prefix": "FormatVcfForGatk",
  "dataset": "my-dataset",
  "access_level": "standard",
  "output_prefix": "gatk_sv/output/FormatVcfForGatk/my-dataset",
  "ar_guid": "0f8e1c2d-...",
  "labels": {"stage": "formatvcfforgatk"},
  "collect": {"gatk_formatted_vcf": "gs://cpg-my-dataset-main/gatk_sv/out.vcf.gz"},
  "poll": {"min_seconds": 30, "max_seconds": 140, "jitter": true, "time_limit_seconds": 86400}
}
```

### Fields

| Field | Required | Meaning |
|---|---|---|
| `name` | yes | Your label for this run, used in logs and result files. Must be unique within a `run-many` file |
| `source.bundle` | yes | The URI printed by `cpg-cromwell bundle` |
| `source.workflow` | yes | Path of the top-level WDL inside the bundle |
| `inputs` | yes | Workflow inputs, **without** the workflow-name prefix |
| `input_prefix` | yes (may be `null`) | Prepended to every input key, e.g. `"vcf"` becomes `"FormatVcfForGatk.vcf"`. Also used to find outputs |
| `dataset` | yes | Chooses the service-account secret and the default log and tmp buckets |
| `access_level` | yes | `test`, `standard`, `full`, ... Chooses the secret and whether the main-branch rule applies |
| `output_prefix` | yes | Logs go to `<analysis bucket>/cromwell_logs/<output_prefix>` |
| `google_project` | no | Defaults to `workflow.dataset_gcp_project` from the config |
| `ar_guid` | no | Added to the workflow's GCP labels as `ar-guid`. It is **only** taken from here, never from the config or environment |
| `labels` | no | Extra GCP labels. Must not contain `ar-guid` or `compute-category`, which are always set for you |
| `copy_outputs` | no | `true` asks Cromwell to copy outputs to `<default bucket>/<output_prefix>` itself (`final_workflow_outputs_dir`). Default `false` |
| `collect` | no | Output key → destination(s). See [Collecting outputs](#collecting-outputs) |
| `poll` | no | How to watch the workflow. See below |

Unknown fields are rejected, so a typo in a field name fails straight away instead of being
silently ignored.

**Labels** must follow the GCP rules: keys start with a lowercase letter, and keys and values use
only lowercase letters, digits, `_` and `-`, with at most 63 characters. Values are **not**
lowercased or truncated for you; an invalid value is an error. There can be at most 64 labels,
counting the two that are added automatically.

### Polling

| `poll` field | Default | Meaning |
|---|---|---|
| `min_seconds` | 5 | Interval right after submission |
| `max_seconds` | 60 | Interval once the ramp is over |
| `ramp_seconds` | 1200 | The interval rises from min to max along a cosine curve over this long (the same curve as `watch_workflow`) |
| `jitter` | `false` | Draws min from `[min, 2·min]` and max from `[max, 2·max]` once per workflow, to spread load in large fan-outs |
| `max_consecutive_errors` | 25 | Failed status checks in a row before giving up with exit 75. A successful check resets the count |
| `time_limit_seconds` | none | After this long, abort the workflow and exit 124 |

### Check before you submit

```bash
cpg-cromwell render --spec spec.json            # reads the secret to fill in the service account
cpg-cromwell render --spec spec.json --offline  # doesn't touch Secret Manager
```

This prints the exact inputs and workflow options that `run` would send, with the secret redacted.
Nothing is submitted. The spec can be a local path, a `gs://` URI, or `-` for stdin.

## Step 3: run it

```bash
cpg-cromwell run --spec spec.json \
    --reattach-root gs://.../cromwell_ids \
    --outputs-json outputs.json \
    --result-json result.json
```

`run` will:

1. load the bundle and check the main-branch rule;
2. read the service-account secret and build the workflow options;
3. if `--reattach-root` is given, look for an earlier submission of the same workflow and attach to
   it instead of submitting again (see [re-attach](#relaunching-safely-re-attach));
4. otherwise submit, then poll until the workflow finishes;
5. print a summary of the workflow's calls when it ends;
6. on success, copy the `collect` outputs;
7. exit with a code that says what happened (see [Exit codes](#exit-codes)).

| Option | Meaning |
|---|---|
| `--spec` | Spec file: local, `gs://`, or `-` for stdin |
| `--reattach-root` | Where re-attach records are kept. Strongly recommended under any orchestrator. Without it, every launch submits a new workflow |
| `--outputs-json` | Written on success only, and **last**. If this file exists, collection finished |
| `--result-json` | The full result, written whether the run succeeded or not |
| `--no-abort-on-exit` | On SIGTERM/SIGINT, leave the workflow running instead of aborting it |
| `-v` / `--verbose` | Debug logging, including every poll |

Logs go to stderr. `result.json` looks like this:

```json
{
  "name": "FormatVcfForGatk/my-dataset",
  "workflow_id": "a1b2c3d4-...",
  "status": "Succeeded",
  "reattached": false,
  "submitted_ar_guid": "0f8e1c2d-...",
  "outputs": {"gatk_formatted_vcf": "gs://cpg-my-dataset-main/gatk_sv/out.vcf.gz"},
  "failures": [],
  "exit_code": 0,
  "error": null
}
```

`status` is one of `Succeeded`, `Failed`, `Aborted`, `TimedOut`, `Unreachable` (gave up after too
many errors in a row) or `NotSubmitted`. A run that detached on a signal can also report Cromwell's
live statuses: `Submitted`, `Running`, `On Hold` or `Aborting` (see
[signals](#stopping-a-run-signals)). Note that `Succeeded` with a non-zero `exit_code` means
the workflow succeeded but collecting its outputs failed. `failures` holds up to ten short messages
taken from Cromwell's failure metadata.

## Running many workflows

For fan-outs, such as one workflow per sample chunk, put one spec per line in a JSONL file and use
`run-many`. One process watches all of them, which keeps the load on Cromwell low.

```bash
cpg-cromwell run-many --specs specs.jsonl \
    --results-dir gs://.../results \
    --reattach-root gs://.../cromwell_ids \
    --submit-interval 2 \
    --max-in-flight 50
```

| Option | Meaning |
|---|---|
| `--specs` | JSONL file, one spec per line. Names must be unique |
| `--results-dir` | Required. One `<name>-<hash>.json` result file per workflow, written as each one finishes |
| `--submit-interval` | Seconds between submissions (default 1) |
| `--max-in-flight` | At most this many workflows running at once. New submissions wait for a slot |
| `--reattach-root`, `--no-abort-on-exit` | As for `run` |

Each workflow has its own polling curve, error budget and time limit. One workflow failing never
stops the others. At the end a summary table is logged, and the exit code is chosen from all the
results (see [Exit codes](#exit-codes)).

## Relaunching safely: re-attach

Orchestrators retry and resume. Without care that means submitting the same expensive workflow
twice. Pass `--reattach-root` and `cpg-cromwell` records each submission under a key computed from:

- the repo and commit;
- the workflow path;
- the prefixed inputs;
- the workflow options, **except** the secret and the labels. Labels are left out because each
  launch has a new `ar-guid`.

When the same workflow is launched again, the earlier one's status decides what happens:

| Earlier workflow is... | What this launch does |
|---|---|
| not found, `Failed` or `Aborted` | Submits a new one (call caching makes this cheap) |
| `Submitted`, `Running` or `On Hold` | Attaches and watches it |
| `Aborting` | Waits for the abort to finish (up to 10 minutes), then submits |
| `Succeeded` | Goes straight to collecting outputs |

The result shows `"reattached": true` and the `submitted_ar_guid` of the launch that originally
submitted it.

The records are written with GCS generation preconditions. If two processes launch the same
workflow at once, one wins; the other aborts its duplicate and attaches to the winner's.

Changing any input, the commit or an option gives a new key, so the next launch submits fresh.

## Collecting outputs

`collect` maps output keys (without the prefix) to destinations:

```json
"collect": {
  "merged_vcf": "gs://bucket/out/merged.vcf.gz",
  "per_sample_counts": ["gs://bucket/out/s1.counts", "gs://bucket/out/s2.counts"]
}
```

For each key, `cpg-cromwell` looks up `<input_prefix>.<key>` in the workflow's outputs.

Collection is **strict**. These are all errors (exit 1):

- the key is missing from the outputs, or its value is `null`;
- a list destination is given but the output isn't an array, or the array's length differs;
- a single destination is given but the output is an array or object.

If a workflow output is optional, leave it out of `collect`.

`gs://` values are copied server-side, so multi-GB files don't pass through the machine running
`cpg-cromwell`. A copy is skipped if the destination already holds an identical object, so collecting
again after a relaunch is cheap. Non-path values (numbers, strings, booleans) are passed through to
`outputs.json` as they are.

If you leave `collect` out, nothing is copied and `outputs.json` contains Cromwell's raw outputs.

## Stopping a run: signals

On **SIGTERM** or **SIGINT**, `cpg-cromwell` stops polling and then, by default, **aborts every
workflow it submitted or attached to** that hasn't finished. It writes the results and exits 143
(SIGTERM) or 130 (SIGINT). This is what you want under Nextflow: cancelling the pipeline cancels
the Cromwell work too.

The re-attach record is left in place. On the next launch the workflow reads as `Aborted`, so it is
resubmitted, and call caching makes the resubmission cheap.

With `--no-abort-on-exit` the process still stops cleanly on the signal (same exit codes, results
still written), but the workflows are **left running**. Before exiting it checks each workflow's
status with Cromwell once and records it in the result:

| Cromwell says | Result `status` | Result `error` |
|---|---|---|
| still going | `Submitted`, `Running`, `On Hold` or `Aborting` | `detached on signal (workflow left running)` |
| already finished | `Succeeded`, `Failed` or `Aborted` | `detached on signal; the workflow had already finished (...)` |
| no answer | `Unreachable` | `detached on signal; could not check the workflow status: ...` |

Outputs are never collected after a signal, even if the workflow had succeeded. The result keeps
the workflow ID, so you can follow it with `cpg-cromwell status`, or relaunch with the same
`--reattach-root` to pick it up again (a succeeded workflow then goes straight to collection).

If a signal arrives while outputs are being copied, the copy is cancelled and the run exits with
the signal's code. Relaunching re-attaches to the succeeded workflow and finishes the copy.

Signals are only handled when `cpg-cromwell` runs on the main thread, which is always true for the
CLI.

## Exit codes

| Code | Meaning | Retry? |
|---|---|---|
| 0 | Succeeded and outputs collected | – |
| 1 | Workflow `Failed`, or was `Aborted` by someone else; or output collection failed | no |
| 2 | Bad spec or usage, invalid labels, bundle not verified on main, secret or config missing, Cromwell rejected the submission | no: fix the input |
| 75 | Transient: Cromwell unreachable, couldn't mint a token, GCS server error | **yes**: with re-attach, retrying is cheap |
| 124 | `time_limit_seconds` exceeded; the workflow was aborted | no |
| 143 / 130 | SIGTERM / SIGINT | – |

`run-many` exits 0 only if every workflow succeeded. Otherwise it uses the first of these codes
that occurred, in this order: 143, 130, 75, 2, 1, 124. A transient error ranks above an ordinary
failure so the orchestrator retries the whole batch; re-attach means the workflows that already
finished aren't run again.

A Nextflow process can use this directly:

```groovy
errorStrategy { task.exitStatus == 75 ? 'retry' : 'terminate' }
maxRetries 3
```

## Inspecting workflows by hand

```bash
cpg-cromwell status <workflow-id>            # e.g. Running
cpg-cromwell status <workflow-id> --json
cpg-cromwell metadata <workflow-id> --display              # readable summary of the calls
cpg-cromwell metadata <workflow-id> --display --expand-completed
cpg-cromwell metadata <workflow-id> --expand-subworkflows  # raw JSON
cpg-cromwell outputs <workflow-id>
cpg-cromwell abort <workflow-id> [<workflow-id> ...]
```

## Python API

The CLI is a thin layer over these functions, and they behave the same way.

```python
from cpg_cromwell import PollPolicy, WorkflowSource, WorkflowSpec, run, run_many

spec = WorkflowSpec(
    name='FormatVcfForGatk/my-dataset',
    source=WorkflowSource(bundle='gs://.../bundles/gatk-sv/<sha>/', workflow='FormatVcfForGatk.wdl'),
    inputs={'vcf': 'gs://.../in.vcf.gz'},
    input_prefix='FormatVcfForGatk',
    dataset='my-dataset',
    access_level='standard',
    output_prefix='gatk_sv/output/FormatVcfForGatk/my-dataset',
    ar_guid=ar_guid,
    collect={'gatk_formatted_vcf': 'gs://.../out.vcf.gz'},
    poll=PollPolicy(min_seconds=30, max_seconds=140, jitter=True),
)

result = run(spec, reattach_root='gs://.../cromwell_ids')
if not result.ok:
    raise SystemExit(result.exit_code)
```

- **Inputs** can hold `pathlib` and `cloudpathlib` paths, lists and sets; they are turned into
  strings the same way `add_gatk_sv_jobs` does. Sets are sorted so the re-attach key stays stable.
- **`render(spec)`** returns `(inputs, options)` with the secret redacted, without submitting.
- **`build_bundle(...)`** is the `bundle` command as a function.
- **`run_many(specs, ...)`** takes the same options as the CLI command, plus `on_result`:

```python
def register(result):  # e.g. record the outputs in metamist
    ...


results = run_many(specs, reattach_root=..., results_dir=..., on_result=register)
```

`on_result` is called once per workflow, on the scheduler's thread, as soon as its result is final
(after collection). If it raises, that workflow's result becomes a failure (exit 1). It is not
called for workflows stopped by a signal.

Errors in the spec raise `SpecError`. Problems while running don't raise; they are reported in the
returned `WorkflowResult`s. Both carry an `exit_code` from the table above.

## Troubleshooting

**`commit must be a full 40-character lowercase SHA`.** Resolve the branch first:
`git rev-parse origin/main`.

**`commit ... is not merged into the main branch`** (from `bundle`) or **`... is not verified on
main`** (from `run`). Merge first, or use `access_level: test` while developing. A bundle built at
`test` level is fine to reuse once the commit is merged: run `bundle` again at the real access level.

**`no bundle.json at ...`.** The bundle doesn't exist, or its upload didn't finish. Run `bundle`
again.

**`config: ... (set CPG_CONFIG_PATH ...)`.** The config is missing `workflow.dataset_gcp_project`
or `storage.<dataset>`.

**`couldn't read secret {dataset}-cromwell-{access_level}-key`.** The credentials you are running
with can't read that secret in the dataset's project.

**`invalid value for label ...`.** Label values aren't normalised for you. Lowercase them and keep
them to 63 characters before putting them in the spec.

**Exit 75 over and over.** Cromwell or GCS is unreachable from where you're running. Check
`CROMWELL_URL`, network access, and that your credentials can mint an ID token for
`CROMWELL_AUDIENCE`. Run with `-v` to see each attempt.

**`Succeeded` but exit 1 with `output collection failed`.** The workflow worked, but an output was
missing, `null`, or the wrong shape for its `collect` entry. Compare `cpg-cromwell outputs <id>`
with your spec. After fixing the spec, relaunch with the same `--reattach-root`: as long as the
inputs and options didn't change, it re-attaches to the succeeded workflow and only collects.
