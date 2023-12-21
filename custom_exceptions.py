class TaskNotFound(Exception):
    def __init__(self, taskId, message = "Task not found") -> None:
        super().__init__(message)
        self.taskId = taskId

class InvalidArguments(Exception):
    def __init__(self, message = "Invalid arguments", *args) -> None:
        super().__init__(message)
        self.args = args

class InvalidWorker(Exception):
    def __init__(self, message = "Invalid worker type", *args) -> None:
        super().__init__(message)