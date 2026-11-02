"""
Workflow options with exact parity to `cpg_utils.cromwell.run_cromwell_workflow` (design §7.2).

This and the CLI's config fallback are the only places that read `cpg_utils.config`.
The AR GUID is never read from the config (§7.9).
"""

import json
import os
from typing import Any

from cpg_cromwell.errors import SpecError
from cpg_cromwell.spec import AR_GUID_LABEL, COMPUTE_CATEGORY_LABEL, WorkflowSpec

REDACTED = '<redacted>'
SECRET_OPTION = 'user_service_account_json'  # noqa: S105  (an option name, not a secret)
# left out of the re-attach key: the secret, and labels (they carry the per-launch ar-guid)
KEY_EXCLUDED_OPTIONS = (SECRET_OPTION, 'google_labels')


def cromwell_secret_name(dataset: str, access_level: str) -> str:
    return f'{dataset}-cromwell-{access_level}-key'


def _config_error(e: Exception) -> SpecError:
    return SpecError(f'config: {e} (set CPG_CONFIG_PATH to a config with [workflow] and [storage.<dataset>])')


def read_cromwell_secret(dataset: str, access_level: str) -> str:
    """Secret '{dataset}-cromwell-{access_level}-key' in config workflow.dataset_gcp_project."""
    from cpg_utils.cloud import read_secret  # noqa: PLC0415
    from cpg_utils.config import ConfigError, get_gcp_project  # noqa: PLC0415

    name = cromwell_secret_name(dataset, access_level)
    try:
        project = get_gcp_project()
    except (ConfigError, KeyError) as e:
        raise _config_error(e) from e
    value = read_secret(project, name)
    if not value:
        raise SpecError(f"couldn't read secret {name} in project {project}")
    return value


def build_workflow_options(spec: WorkflowSpec, secret_json: str) -> dict[str, Any]:
    from cpg_utils.config import ConfigError, dataset_path, get_gcp_project  # noqa: PLC0415

    try:
        email = json.loads(secret_json).get('client_email')
    except (ValueError, AttributeError) as e:
        raise SpecError('the Cromwell service-account secret is not a JSON key') from e
    if not email:
        raise SpecError("the service_account didn't contain an entry for client_email")

    google_labels = dict(spec.labels)
    google_labels[COMPUTE_CATEGORY_LABEL] = 'cromwell'
    if spec.ar_guid:
        google_labels[AR_GUID_LABEL] = spec.ar_guid

    try:
        project = spec.google_project or get_gcp_project()
        logs_dir = dataset_path(f'cromwell_logs/{spec.output_prefix}', dataset=spec.dataset, category='analysis')
        gcs_root = os.path.join(dataset_path('', dataset=spec.dataset, category='tmp'), 'cromwell')
        outputs_dir = dataset_path(spec.output_prefix, dataset=spec.dataset) if spec.copy_outputs else None
    except (ConfigError, KeyError) as e:
        raise _config_error(e) from e

    # key order matches run_cromwell_workflow, so json.dumps() gives the same bytes
    options: dict[str, Any] = {
        'backend': 'batch',
        SECRET_OPTION: secret_json,
        'google_compute_service_account': email,
        'google_project': project,
        'google_labels': google_labels,
        'final_call_logs_dir': logs_dir,
        'final_workflow_log_dir': logs_dir,
        'gcp_batch_gcs_root': gcs_root,
    }
    if outputs_dir is not None:
        options['final_workflow_outputs_dir'] = outputs_dir
    return options


def redact_options(options: dict[str, Any]) -> dict[str, Any]:
    return {k: (REDACTED if k == SECRET_OPTION else v) for k, v in options.items()}


def options_for_key(options: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in options.items() if k not in KEY_EXCLUDED_OPTIONS}
