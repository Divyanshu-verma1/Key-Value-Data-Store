# DAO - Data Access Object for tasks entities

from typing import List
from models import Task

tasks : List[Task] = []
next_id : int = 1

def get_next_id() -> int:
    global next_id
    current_id = next_id
    next_id += 1
    return current_id

def add_task(task: Task) -> None:
    tasks.append(task)

def get_all_tasks() -> List[Task]:
    return tasks