import requests
import random
from utils import serialize, deserialize
import sys
import time

base_url = "http://127.0.0.1:8000/"


def func_noop(x, y):
    """No op function that returns immediately"""
    return x + y


def func_sleep(x, y):
    """Function that takes some time to complete"""
    import time

    time.sleep(5)
    return x - y

def register_function(func):
    resp = requests.post(
        base_url + "register_function",
        json={"name": "test_func", "payload": serialize(func)},
    )

    resp = resp.json()
    function_id = resp["function_id"]

    return function_id


def test_performance(func, num_req: int):
    """Function to test latency, throughput and time taken for different configurations

    Args:
        func (function): func_noop or func_sleep
        num_req (int): number of requests
    """
    task_ids = []
    for _ in range(num_req):
        x, y = random.randint(-100, 100), random.randint(-100, 100)

        resp = requests.post(
            base_url + "execute_function",
            json={
                "function_id": register_function(func),
                "payload": serialize(((x, y), {})),
            },
        )

        resp = resp.json()
        task_id = resp["task_id"]

        task_ids.append(task_id)

    completed = set()
    while len(completed) != len(task_ids):
        for task_id in task_ids:
            if task_id in completed:
                continue

            resp = requests.get(f"{base_url}status/{task_id}").json()
            if resp["status"] == "COMPLETED":
                completed.add(task_id)

                if len(completed) != len(task_ids):
                    break


def show_parallel_execution(func, num_req):
    from datetime import datetime

    # Use sleep tasks
    task_ids = []

    for _ in range(num_req):
        x, y = random.randint(-100, 100), random.randint(-100, 100)

        resp = requests.post(
            base_url + "execute_function",
            json={
                "function_id": register_function(func),
                "payload": serialize(((x, y), {})),
            },
        )

        resp = resp.json()
        task_id = resp["task_id"]

        task_ids.append(task_id)

    for _ in range(num_req):
        resp = requests.get(f"{base_url}status/{task_id}").json()

        while resp["status"] != "RUNNING":
            resp = requests.get(f"{base_url}status/{task_id}").json()
            continue

        if resp["status"] == "RUNNING":
            print(
                f"Task: {task_id} started running at {datetime.utcnow().strftime('%H:%M:%S.%f')[:-3]}"
            )

def execute_func(func_id):
    resp = requests.post(base_url + "execute_function",
                         json={"function_id": func_id,
                               "payload": serialize(((2, 3), {}))})
    assert resp.status_code == 200
    return resp.json()["task_id"]

def time_api_registration(func, n):
    for _ in range(n):
        register_function(func)

def time_api_execution(func, n):
    func_id = register_function(func)
    for _ in range(n):
        execute_func(func_id)

def time_api_status(func, n):
    func_id = register_function(func)
    task_id = execute_func(func_id)

    for _ in range(n):
        resp = requests.get(f"{base_url}status/{task_id}")

def time_api_result(func, n):
    func_id = register_function(func)
    task_id = execute_func(func_id)

    for _ in range(n):
        resp = requests.get(f"{base_url}status/{task_id}")

if __name__ == "__main__":
    func_type = int(sys.argv[1])
    num_req = int(sys.argv[2])

    if func_type == 0:
        func = func_noop

    else:
        func = func_sleep

    s = time.perf_counter()

    # test_performance(func, num_req)
    # show_parallel_execution(func, num_req)
    time_api_result(func, num_req)

    e = time.perf_counter()
    print(
        f"Time taken to run {num_req} requets of type {'no op func' if func_type == 0 else 'sleep func'} : {e - s : .3f}s"
    )
