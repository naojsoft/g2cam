#
# conftest.py -- guard against testing the wrong copy
#
"""These tests compare the legacy ``remoteObjects`` XML-RPC stack against the
tinyrpc-based one, so they are only meaningful when run against the working
trees of both.  Both packages can also be present in site-packages as ordinary
non-editable installs, in which case an unqualified import would quietly pick
up whatever was last installed and the harness would report on code nobody is
editing.

Both are expected to be installed editable (``pip install -e``).  This just
checks that they still are, and says so plainly if they are not.
"""

import pathlib

import pytest

# .../g2cam-pure/g2base/remoteObjects/tests/conftest.py
REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]


def _imported_from(module):
    return pathlib.Path(module.__file__).resolve()


def pytest_configure(config):
    import g2base

    g2base_path = _imported_from(g2base)
    if REPO_ROOT not in g2base_path.parents:
        raise pytest.UsageError(
            "g2base was imported from\n    %s\nbut these tests must run "
            "against the working tree at\n    %s\nInstall it editable: "
            "pip install -e %s" % (g2base_path, REPO_ROOT, REPO_ROOT))

    import tinyrpc
    tinyrpc_path = _imported_from(tinyrpc)
    if 'site-packages' in tinyrpc_path.parts:
        raise pytest.UsageError(
            "tinyrpc was imported from a non-editable install at\n    %s\n"
            "These tests exercise in-progress changes to tinyrpc, so it must "
            "be installed editable: pip install -e <tinyrpc checkout>"
            % (tinyrpc_path,))

    config.stash_tinyrpc_path = tinyrpc_path


def pytest_report_header(config):
    import g2base
    import tinyrpc
    return ["g2base:  %s" % _imported_from(g2base).parent,
            "tinyrpc: %s" % _imported_from(tinyrpc).parent]
