import requests
import logging
import random
import dill
import codecs


def serialize(obj) -> str:
    return codecs.encode(dill.dumps(obj), "base64").decode()


def deserialize(obj: str):
    return dill.loads(codecs.decode(obj.encode(), "base64"))


base_url = "http://127.0.0.1:8000/"


def add(x, y):
    import time

    time.sleep(10)
    return x + y


def multiply(x, y):
    import time

    time.sleep(5)
    return x * y


def power(base, power):
    import time

    time.sleep(10)
    return base**power


def register_functions():
    function_ids = []
    resp = requests.post(
        base_url + "register_function", json={"name": "ADD", "payload": serialize(add)}
    )
    assert resp.status_code == 200
    resp = resp.json()
    assert "function_id" in resp
    function_ids.append(resp["function_id"])

    resp = requests.post(
        base_url + "register_function",
        json={"name": "MULTIPLY", "payload": serialize(multiply)},
    )
    assert resp.status_code == 200
    resp = resp.json()
    assert "function_id" in resp
    function_ids.append(resp["function_id"])

    resp = requests.post(
        base_url + "register_function",
        json={"name": "POWER", "payload": serialize(power)},
    )
    assert resp.status_code == 200
    resp = resp.json()
    assert "function_id" in resp
    function_ids.append(resp["function_id"])

    return function_ids


def execute_functions():
    functions = register_functions()
    task_ids_and_expected_results = []
    for _ in range(200):
        func = random.randint(0, 2)
        x, y = random.randint(1, 20), random.randint(-10, 10)

        if func == 0:
            actual = x + y
        elif func == 1:
            actual = x * y
        else:
            actual = x**y

        resp = requests.post(
            base_url + "execute_function",
            json={
                "function_id": functions[func],
                "payload": serialize(((x, y), {})),
            },
        )

        assert resp.status_code == 200
        resp = resp.json()
        assert "task_id" in resp
        task_ids_and_expected_results.append((resp["task_id"], actual))
        logging.warning(f"submitted task {resp['task_id']}")

    return task_ids_and_expected_results


def test_correctness():
    completed = set()

    tasks_and_results = set(execute_functions())

    while len(completed) != len(tasks_and_results):
        for task, res in tasks_and_results:
            if (task, res) in completed:
                continue

            resp = requests.get(f"{base_url}status/{task}")

            assert resp.status_code == 200
            resp = resp.json()
            assert resp["task_id"] == task
            if resp["status"] == "COMPLETED":
                completed.add((task, res))

            elif resp["status"] in {"RUNNING", "QUEUED"}:
                continue

            else:
                logging.warning(f"task {task} failed")
                assert True == False

    for task, res in completed:
        resp = requests.get(f"{base_url}result/{task}")

        assert resp.status_code == 200
        resp = resp.json()
        assert deserialize(resp["result"]) == res
