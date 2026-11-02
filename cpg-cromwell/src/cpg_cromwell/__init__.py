"""
cpg-cromwell: a Hail-free client and CLI for the CPG Cromwell server.

Never imports cpg_utils.cromwell (it pulls in hailtop). See CROMWELL_CLI_DESIGN.md.
"""

from cpg_cromwell.api import CromwellApi
from cpg_cromwell.bundle import build_bundle
from cpg_cromwell.errors import (
    EXIT_FAILED,
    EXIT_OK,
    EXIT_SIGINT,
    EXIT_SIGTERM,
    EXIT_TIMEOUT,
    EXIT_TRANSIENT,
    EXIT_USAGE,
    BundleError,
    CollectError,
    CromwellCliError,
    SpecError,
    TransientError,
    combine_exit_codes,
)
from cpg_cromwell.options import build_workflow_options
from cpg_cromwell.run import RunContext, render, result_filename, run, run_many
from cpg_cromwell.spec import PollPolicy, WorkflowResult, WorkflowSource, WorkflowSpec

__all__ = [
    'EXIT_FAILED',
    'EXIT_OK',
    'EXIT_SIGINT',
    'EXIT_SIGTERM',
    'EXIT_TIMEOUT',
    'EXIT_TRANSIENT',
    'EXIT_USAGE',
    'BundleError',
    'CollectError',
    'CromwellApi',
    'CromwellCliError',
    'PollPolicy',
    'RunContext',
    'SpecError',
    'TransientError',
    'WorkflowResult',
    'WorkflowSource',
    'WorkflowSpec',
    'build_bundle',
    'build_workflow_options',
    'combine_exit_codes',
    'render',
    'result_filename',
    'run',
    'run_many',
]
