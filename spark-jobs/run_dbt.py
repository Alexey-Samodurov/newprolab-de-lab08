from __future__ import annotations

import os
import subprocess
import sys

INIT_SCRIPT = "/tmp/cm/dbt-init.sh"
PROJECT_DIR = "/tmp/dbt-project"


def main() -> int:
    """Run the dbt CLI inside the SparkApplication driver pod.

    Lays out the flat dbt ConfigMap into a proper project directory via
    ``dbt-init.sh``, then delegates to ``dbtRunner`` with the arguments
    passed on the command line (defaults to ``dbt debug`` when none given).

    Returns:
        0 on success, 1 on dbt failure, 2 if dbt raised an exception.
    """
    subprocess.run(["sh", INIT_SCRIPT], check=True)

    os.environ.setdefault("DBT_PROFILES_DIR", PROJECT_DIR)
    os.chdir(PROJECT_DIR)

    from dbt.cli.main import dbtRunner

    from log_utils import get_logger
    log = get_logger(__name__)

    args = sys.argv[1:] or ["debug"]
    log.info("invoking dbt with args: %s", args)

    res = dbtRunner().invoke(args)
    if res.exception is not None:
        log.error("dbt raised exception: %s", res.exception)
        return 2
    return 0 if res.success else 1


if __name__ == "__main__":
    sys.exit(main())
