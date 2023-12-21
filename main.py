from fastapi import FastAPI, HTTPException
from models import *
import redis
import utils
import uuid
import constants

app = FastAPI()

redis_cl = redis.StrictRedis(
    host=constants.localhost, port=constants.rport, decode_responses=True
)


@app.post("/register_function", response_model=RegisterFnRep)
def register_fn(reg_fn: RegisterFn) -> RegisterFnRep:
    """Register serialized function with the service

    Args:
        reg_fn (RegisterFn): Object with the function to be registered

    Raises:
        HTTPException: Raise 500 Exception if function registration fails

    Returns:
        RegisterFnRep: Return object with the function id for the registered function
    """
    # generate UUID for the function
    fn_id = uuid.uuid4()
    fn_dict = {"name": reg_fn.name, "payload": reg_fn.payload}

    try:
        redis_cl.set(str(fn_id), utils.serialize(fn_dict))

        return RegisterFnRep(function_id=fn_id)

    except:
        raise HTTPException(status_code=500, detail="Function registration failed")


@app.post("/execute_function", response_model=ExecuteFnRep)
def execute_fn(exc_fn: ExecuteFnReq) -> ExecuteFnRep:
    """Execute function request with the function id ExecuteFnReq.function_id with payload ExecuteFnReq.payload

    Args:
        exc_fn (ExecuteFnReq): Object with the function id and serialized payload

    Raises:
        HTTPException: Raise 500 exception if executing the function fails
        HTTPException: Raise 404 if function with id is not found

    Returns:
        ExecuteFnRep:
    """
    # generate UUID for the task
    task_id = uuid.uuid4()

    # find func payload
    if redis_cl.exists(str(exc_fn.function_id)):
        try:
            fn_dict = utils.deserialize(redis_cl.get(str(exc_fn.function_id)))

            # create task
            task = {
                "fn_name": fn_dict["name"],
                "fn_payload": fn_dict["payload"],
                "param_payload": exc_fn.payload,
                "status": "QUEUED",
                "result": utils.serialize("NA"),
            }

            redis_cl.set(str(task_id), utils.serialize(task))
            redis_cl.rpush("tasks", str(task_id))

            return ExecuteFnRep(task_id=task_id)

        except:
            raise HTTPException(status_code=500, detail="Function execution failed")

    else:
        raise HTTPException(
            status_code=404, detail=f"Function with {exc_fn.function_id} not found"
        )


@app.get("/status/{task_id}", response_model=TaskStatusRep)
def get_task_status(task_id: uuid.UUID) -> TaskStatusRep:
    """Get request for the status of the task

    Args:
        task_id (uuid.UUID): ID of the task

    Raises:
        HTTPException: Raise 404 exception if task with id is not found

    Returns:
        TaskStatusRep: Object with task id and the current status of the task
    """
    if redis_cl.exists(str(task_id)):
        task = utils.deserialize(redis_cl.get(str(task_id)))
        status = task["status"]

        return TaskStatusRep(task_id=task_id, status=status)

    else:
        raise HTTPException(status_code=404, detail=f"Task with {task_id} not found")


@app.get("/result/{task_id}", response_model=TaskResultRep)
def get_task_results(task_id: uuid.UUID) -> TaskResultRep:
    """Get request for the result of the task

    Args:
        task_id (uuid.UUID): ID of the task

    Raises:
        HTTPException: Raise 404 exception if task with id is not found

    Returns:
        TaskResultRep: Object with task id, status of the task and the result of the execution
    """
    try:
        task = utils.deserialize(redis_cl.get(str(task_id)))
        status = str(task["status"])
        result = str(task["result"])

        return TaskResultRep(task_id=task_id, status=status, result=result)

    except:
        raise HTTPException(status_code=404, detail=f"Task with {task_id} not found")
