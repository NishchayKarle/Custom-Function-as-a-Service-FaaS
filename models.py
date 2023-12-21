import uuid
from pydantic import BaseModel


class RegisterFn(BaseModel):
    name: str
    payload: str


class RegisterFnRep(BaseModel):
    function_id: uuid.UUID


class ExecuteFnReq(BaseModel):
    function_id: uuid.UUID
    payload: str


class ExecuteFnRep(BaseModel):
    task_id: uuid.UUID


class TaskStatusRep(BaseModel):
    task_id: uuid.UUID
    status: str


class TaskResultRep(BaseModel):
    task_id: uuid.UUID
    status: str
    result: str
