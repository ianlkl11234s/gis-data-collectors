"""utils.isolated_task 測試用的子行程 target（不連任何遠端）。"""
import os
import time
from datetime import date


def ok(day=None):
    return {"day": day, "when": date(2026, 11, 6), "pid": os.getpid()}


def boom():
    raise ValueError("bad thing postgresql://user:pw@host/db?password=hunter2")


def sleepy(seconds=30):
    time.sleep(seconds)
    return "late"


def hard_exit():
    os._exit(7)


def read_oom_adj():
    path = os.environ["ISOLATED_TASK_OOM_ADJ_PATH"]
    with open(path) as fh:
        return fh.read()
