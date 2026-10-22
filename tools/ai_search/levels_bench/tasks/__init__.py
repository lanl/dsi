from .calibration import CALIBRATION_TASKS
from .level1 import LEVEL1_TASKS
from .level2 import LEVEL2_TASKS
from .level3 import LEVEL3_TASKS

ALL_TASKS = list(LEVEL1_TASKS) + list(LEVEL2_TASKS) + list(LEVEL3_TASKS) + list(CALIBRATION_TASKS)


def get_task(task_id: str):
    return next(t for t in ALL_TASKS if t.task_id == task_id)
