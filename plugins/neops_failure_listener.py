"""Airflow listener plugin: sends every failed task (any DAG) to the Neops bridge (Azure Function).

Install: copy to $AIRFLOW_HOME/plugins/ on the scheduler and on every worker, set the env vars
below, then restart those components.

This runs INSIDE Airflow processes, so it is deliberately tiny: it never raises, it has a bounded
timeout, and it does no parsing. All parsing happens in the Azure Function.

Timeout trade-off: the Function may fetch the task log from the Airflow REST API before it answers
(up to LOG_ENRICH_BUDGET_SECONDS, default 25 s), so the default wait is 45 s. This only delays the
failing task's process; successful tasks are unaffected. Lower NEOPS_BRIDGE_TIMEOUT if you turn log
enrichment off.
"""
import json
import logging
import os
import time
import traceback
from datetime import datetime, timezone

import requests
from airflow.listeners import hookimpl
from airflow.plugins_manager import AirflowPlugin

log = logging.getLogger(__name__)

BRIDGE_URL = os.environ.get("NEOPS_BRIDGE_URL", "")        # full Function URL incl. ?code=...
BRIDGE_SECRET = os.environ.get("NEOPS_BRIDGE_SECRET", "")  # sent as X-Webhook-Secret
ENVIRONMENT = os.environ.get("NEOPS_BRIDGE_ENV", "unknown")  # e.g. dev / prod
UI_BASE_URL = os.environ.get("NEOPS_BRIDGE_UI_URL", "")     # optional Airflow UI base URL
TIMEOUT_SECONDS = float(os.environ.get("NEOPS_BRIDGE_TIMEOUT", "45"))
MAX_CHARS = 6000


def _iso(value):
    try:
        return value.isoformat() if value else None
    except Exception:
        return None


def _error_fields(error):
    """Turn the error argument (None, str or exception) into plain fields."""
    if error is None:
        return {"error_type": None, "error_message": None, "error_traceback": None}
    if isinstance(error, BaseException):
        tb = "".join(traceback.format_exception(type(error), error, error.__traceback__))
        return {
            "error_type": type(error).__name__,
            "error_message": str(error)[:MAX_CHARS],
            "error_traceback": tb[-MAX_CHARS:],  # keep the end: it holds the final exception
        }
    return {"error_type": None, "error_message": str(error)[:MAX_CHARS], "error_traceback": None}


def _operator_name(task_instance):
    name = getattr(task_instance, "operator", None)
    if name:
        return name
    return getattr(getattr(task_instance, "task", None), "task_type", None)


def _build_event(task_instance, error):
    get = lambda attr: getattr(task_instance, attr, None)  # noqa: E731
    event = {
        "source": "airflow",
        "event_type": "task_failed",
        "environment": ENVIRONMENT,
        "airflow_ui_url": UI_BASE_URL or None,
        "dag_id": get("dag_id"),
        "run_id": get("run_id"),
        "task_id": get("task_id"),
        "map_index": get("map_index"),
        "try_number": get("try_number"),
        "operator": _operator_name(task_instance),
        "hostname": get("hostname"),
        "state": str(get("state")) if get("state") is not None else "failed",
        "start_date": _iso(get("start_date")),
        "end_date": _iso(get("end_date")),
        "duration_seconds": get("duration"),
        "reported_at": datetime.now(timezone.utc).isoformat(),
    }
    event.update(_error_fields(error))
    return event


def _send(event):
    if not BRIDGE_URL:
        log.warning("NEOPS_BRIDGE_URL is not set; failure for %s.%s not sent", event.get("dag_id"), event.get("task_id"))
        return
    body = json.dumps(event, default=str)
    headers = {"Content-Type": "application/json", "X-Webhook-Secret": BRIDGE_SECRET}
    for attempt in (1, 2):
        try:
            resp = requests.post(BRIDGE_URL, data=body, headers=headers, timeout=TIMEOUT_SECONDS)
            if resp.status_code < 300:
                log.info("Neops bridge accepted failure of %s.%s", event.get("dag_id"), event.get("task_id"))
                return
            log.warning("Neops bridge returned %s (attempt %d)", resp.status_code, attempt)
            if resp.status_code < 500 and resp.status_code != 429:
                return  # client error: retrying will not help
        except requests.ReadTimeout:
            # The request reached the Function, which is still working. Resending would only hit the
            # dedupe check, so stop here (the Function keeps processing).
            log.warning("Neops bridge did not answer within %ss; not resending", TIMEOUT_SECONDS)
            return
        except Exception as exc:  # noqa: BLE001 - must never break Airflow
            log.warning("Neops bridge call failed (attempt %d): %s", attempt, exc)
        time.sleep(1)


def _handle(task_instance, error):
    try:
        _send(_build_event(task_instance, error))
    except Exception:  # noqa: BLE001
        log.exception("Neops failure listener crashed (ignored)")


# The `error` argument exists from Airflow 2.10. Registering a hook with an unknown argument
# would fail plugin validation on older versions, so pick the right signature.
try:
    from importlib.metadata import version
    from packaging.version import Version

    _HAS_ERROR_ARG = Version(version("apache-airflow")) >= Version("2.10.0")
except Exception:  # noqa: BLE001
    _HAS_ERROR_ARG = True

if _HAS_ERROR_ARG:

    class NeopsFailureListener:
        @hookimpl
        def on_task_instance_failed(self, previous_state, task_instance, error):
            _handle(task_instance, error)

else:

    class NeopsFailureListener:
        @hookimpl
        def on_task_instance_failed(self, previous_state, task_instance):
            _handle(task_instance, None)


class NeopsFailurePlugin(AirflowPlugin):
    name = "neops_failure_plugin"
    listeners = [NeopsFailureListener()]
