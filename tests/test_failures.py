import requests
import logging
import random
import dill
import codecs
import time


def serialize(obj) -> str:
    return codecs.encode(dill.dumps(obj), "base64").decode()


def deserialize(obj: str):
    return dill.loads(codecs.decode(obj.encode(), "base64"))


base_url = "http://127.0.0.1:8000/"


def func1(x, y):
    return x + y


def func2(a):
    1 / 0
    return a * 2


def func3():
    # no local import of libraries
    time.sleep(10)


def register_function(func):
    resp = requests.post(
        base_url + "register_function",
        json={"name": "FUNC", "payload": serialize(func)},
    )
    assert resp.status_code == 200
    resp = resp.json()
    return resp["function_id"]


def test_serilization_errors():
    function_id = register_function(func1)
    resp = requests.post(
        base_url + "execute_function",
        json={
            "function_id": function_id,
            "payload": serialize(((1, 2), {3: 4})),
        },
    )

    assert resp.status_code == 200
    resp = resp.json()
    assert "task_id" in resp
    task_id = resp["task_id"]

    for i in range(10):
        resp = requests.get(f"{base_url}result/{task_id}")
        resp = resp.json()

        if resp["status"] in ["QUEUED", "RUNNING"]:
            time.sleep(1)
            continue

        assert resp["status"] == "FAILED"
        break


def test_exception_inside_function():
    function_id = register_function(func2)
    resp = requests.post(
        base_url + "execute_function",
        json={
            "function_id": function_id,
            "payload": serialize(((1), {})),
        },
    )

    assert resp.status_code == 200
    resp = resp.json()
    assert "task_id" in resp
    task_id = resp["task_id"]

    for i in range(10):
        resp = requests.get(f"{base_url}result/{task_id}")
        resp = resp.json()

        if resp["status"] in ["QUEUED", "RUNNING"]:
            time.sleep(1)
            continue

        assert resp["status"] == "FAILED"
        break

    function_id = register_function(func3)
    resp = requests.post(
        base_url + "execute_function",
        json={
            "function_id": function_id,
            "payload": serialize(((), {})),
        },
    )

    assert resp.status_code == 200
    resp = resp.json()
    assert "task_id" in resp
    task_id = resp["task_id"]

    for i in range(10):
        resp = requests.get(f"{base_url}result/{task_id}")
        resp = resp.json()

        if resp["status"] in ["QUEUED", "RUNNING"]:
            time.sleep(1)
            continue

        assert resp["status"] == "FAILED"
        break
