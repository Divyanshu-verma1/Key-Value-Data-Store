from typing import Dict, Any

class Task:
    def __init__(self, id: int, title: str, description: str, completed: bool = False):
        self.id = id
        self.title = title
        self.description = description
        self.completed = completed

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "completed": self.completed
        }