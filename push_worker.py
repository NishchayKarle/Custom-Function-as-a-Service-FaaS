import zmq
import utils
import sys
import concurrent.futures
import uuid
import constants


def execute_task(
    worker_id: uuid.UUID,
    task_id: uuid.UUID,
    ser_fn: str,
    ser_params: str,
):
    """Function to deserialize and run the function payload.

    Args:
        worker_id (uuid.UUID): ID of the worker
        task_id (uuid.UUID): ID of the task with the function payload
        ser_fn (str): Serialized function payload
        ser_params (str): Serialized function parameters
        num_free_processors (list): list with the number of free processors

    Returns:
        task_res (str): Serialized result of the task
    """
    # execute the function
    try:
        # deserilaize function and params
        fn = utils.deserialize(ser_fn)
        params = utils.deserialize(ser_params)

        args, kwargs = params
        result_payload = fn(*args, **kwargs)
        status = constants.COMPLETED

    except Exception as exp:
        result_payload = f"Exception occured while executing the function. ERROR: {exp}"
        status = constants.FAILED

    task_res = {
        "worker_id": worker_id,
        "task_id": task_id,
        "status": status,
        "result": utils.serialize(result_payload),
    }

    return utils.serialize(task_res)


def send_result(future: concurrent.futures.Future):
    """Callback function for push worker to send result back
        to the task dispatcher after executing a task

    Args:
        future (concurrent.futures.Future): future object with the result of the task
    """
    task_res = future.result()
    server_socket.send_string(f"{constants.RES}::{task_res}")


def start_worker(num_worker_processors: int, worker_id: str):
    """Function to start push worker. Connects to the task dispatcher and requests for work and starts working

    Args:
        num_worker_processors (int): Max number of worker processors
        dispatcher_url (str): URL for task dispatcher
        worker_id (int): ID of the worker
    """
    with concurrent.futures.ProcessPoolExecutor(
        max_workers=num_worker_processors
    ) as executor:
        while True:
            # send heartbeat
            server_socket.send_string(f"{constants.HBT}::{worker_id}")

            if server_socket.poll(1000):
                message = server_socket.recv_multipart()
                task = utils.deserialize(message[0].decode("utf-8"))
                task_id = task["task_id"]
                ser_payload = task["ser_fn"]
                ser_params = task["ser_params"]

                future = executor.submit(
                    execute_task,
                    worker_id,
                    task_id,
                    ser_payload,
                    ser_params,
                )

                future.add_done_callback(send_result)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 pull_worker.py <num_worker_processors> <dispatcher url>")

    else:
        num_worker_processors = int(sys.argv[1])
        dispatcher_url = sys.argv[2]

    # ID of the push woker
    worker_id = str(uuid.uuid4())

    with zmq.Context() as context:
        server_socket = context.socket(zmq.DEALER)
        server_socket.setsockopt(zmq.IDENTITY, bytes(worker_id, "utf-8"))
        server_socket.connect(dispatcher_url)
        server_socket.send_string(
            f"{constants.REG}::{worker_id}::{num_worker_processors}"
        )

        start_worker(
            num_worker_processors=num_worker_processors,
            worker_id=worker_id,
        )
