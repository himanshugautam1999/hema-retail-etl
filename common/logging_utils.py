import logging
import sys


def get_logger(name: str, run_context: dict | None = None) -> logging.Logger:
    """
    Return a configured logger.

    Parameters
    ----------
    name:
        Logical name of the component, e.g. "bronze_job" or "trigger_lambda".
    run_context:
        Optional dict of key/value pairs (run id, source key, layer) that gets
        prefixed onto every message so log lines are self-describing in
        CloudWatch without needing to correlate across lines.
    """
    logger = logging.getLogger(name)

    # Guard against duplicate handlers when a job re-imports the module.
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)

    context_str = ""
    if run_context:
        context_str = " ".join(f"{k}={v}" for k, v in run_context.items())
        context_str = f"[{context_str}] "

    fmt = f"%(asctime)s %(levelname)s {name} {context_str}%(message)s"
    handler.setFormatter(logging.Formatter(fmt, datefmt="%Y-%m-%dT%H:%M:%S"))
    logger.addHandler(handler)

    # Don't propagate to the root logger; avoids double-printing in Glue.
    logger.propagate = False
    return logger
