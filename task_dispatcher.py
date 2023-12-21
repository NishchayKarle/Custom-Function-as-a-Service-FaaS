import redis
import utils
import sys
import uuid
import zmq
import concurrent.futures
import time
import heapq
import constants
import custom_exceptions

r = redis.Redis(
    host=constants.localhost,
    port=constants.rport,
    decode_responses=True,
)


def execute_task(
    task_id: uuid.UUID, ser_fn: str, ser_params: str
) -> tuple[str, str, str]:
    """Function to deserialize and run the function payload.

    Args:
        task_id (uuid.UUID): ID of the task with the function payload
        ser_fn (str): Serialized function payload
        ser_params (str): Serialized function parameters

    Returns:
        tuple(task_id, status, result): (ID of the task executed,
                                         Status after the function payload run,
                                         Result of the function run)
    """
    # deserilaize function and params
    fn = utils.deserialize(ser_fn)
    params = utils.deserialize(ser_params)
    args, kwargs = params

    # execute the function
    try:
        result_payload = fn(*args, **kwargs)
        status = constants.COMPLETED

    except Exception as exp:
        result_payload = f"Exception occured while executing the function. ERROR: {exp}"
        status = constants.FAILED

    # serialize and return the  results
    return (task_id, status, utils.serialize(result_payload))


def LocalTaskDispatcher(num_worker_processors=8) -> None:
    """Task Dispatcher to manage Local Workers

    Args:
        num_worker_processors (int, optional): Number of local wokers/processors. Defaults to 8.
    """

    def update_redis_local_task_dispatcher(future: concurrent.futures.Future):
        """Function to update redis with result of the future object.

        Args:
            future (concurrent.futures.Future): future object
        """
        task_id, status, result_payload = future.result()

        task = utils.deserialize(r.get(task_id))
        task["status"] = status
        task["result"] = result_payload

        r.set(task_id, utils.serialize(task))

    with concurrent.futures.ProcessPoolExecutor(
        max_workers=num_worker_processors
    ) as executor:
        while True:
            # listen for task ids and assign the workload to a local worker
            if r.llen("tasks") > 0:
                task_id = r.lpop("tasks")

                if r.exists(task_id):
                    task = utils.deserialize(r.get(task_id))
                    ser_fn = task["fn_payload"]
                    ser_params = task["param_payload"]

                    future = executor.submit(execute_task, task_id, ser_fn, ser_params)

                    task["status"] = constants.RUNNING
                    r.set(task_id, utils.serialize(task))
                    future.add_done_callback(update_redis_local_task_dispatcher)

                else:
                    raise custom_exceptions.TaskNotFound(
                        f"TASK ID '{task_id}' NOT FOUND", task_id
                    )


def PullTaskDispatcher(port: int) -> None:
    """Task Dispatcher to manage Pull Workers"""

    def update_redis(task: str, worker_task_map: dict[str, set]) -> None:
        """Update redis cache with the result of the excuted task

        Args:
            task (str): serialized result from the task execution
            worker_task_map (dict): Dict of work load for each worker
        """
        task_update_msg = utils.deserialize(task)
        task_id = task_update_msg["task_id"]
        worker_id = task_update_msg["worker_id"]

        if r.exists(task_id):
            task = utils.deserialize(r.get(task_id))
            task["status"] = task_update_msg["status"]
            task["result"] = task_update_msg["result"]

            r.set(task_id, utils.serialize(task))

        if task_id in worker_task_map[worker_id]:
            worker_task_map[worker_id].remove(task_id)

    def reply_to_task_requests(
        task_id: str, worker_id: str, worker_task_map: dict[str, set]
    ) -> None:
        """Send task to worker with the request

        Args:
            task_id (str): ID of the task being sent
            worker_id (str): ID of the worker requesting the task
            worker_task_map (dict): Dict of work load for each worker
        """
        task = utils.deserialize(r.get(task_id))
        ser_fn, ser_params = task["fn_payload"], task["param_payload"]

        #  Send task to worker
        task_data = {
            "task_id": task_id,
            "ser_fn": ser_fn,
            "ser_params": ser_params,
        }

        worker_socket.send_string(utils.serialize(task_data))

        # update status of task in redis
        task["status"] = constants.RUNNING
        r.set(task_id, utils.serialize(task))

        # Add task to worker's work bucket
        work = worker_task_map.setdefault(worker_id, set())
        work.add(task_id)
        worker_task_map[worker_id] = work

    def check_heartbeat(
        max_heartbeats_missed: int,
        worker_last_heartbeat: dict[str, time.time],
        worker_task_map: dict[str, set],
    ) -> None:
        """Check for dead workers and re assign tasks to other workers.

        Args:
            max_heartbeats_missed (int): Number of heartbeats worker can miss without being considered dead
            worker_last_heartbeat (dict): Dict of workers most recent hearbeat
            worker_task_map (dict): Dict of work load for each worker
        """
        dead_workers = []
        for worker_id in worker_last_heartbeat:
            if time.time() - worker_last_heartbeat[worker_id] > max_heartbeats_missed:
                dead_workers.append(worker_id)

                if worker_id in worker_task_map:
                    for task_reassign in worker_task_map[worker_id]:
                        task = utils.deserialize(r.get(task_reassign))
                        task["status"] = constants.QUEUED
                        r.set(task_reassign, utils.serialize(task))

                        r.lpush("tasks", task_reassign)

        for dead in dead_workers:
            if dead in worker_task_map:
                del worker_task_map[dead]
            del worker_last_heartbeat[dead]

    # number of heartbeats (5 sec) after which a worker is considered dead
    max_heartbeats_missed = 5
    worker_last_heartbeat = dict()

    # maintain tasks each worker is assigned
    worker_task_map = dict()

    with zmq.Context() as context:
        worker_socket = context.socket(zmq.REP)
        worker_socket.bind(f"tcp://*:{port}")

        while True:
            time.sleep(0.01)

            if worker_socket.poll(1000):
                message = worker_socket.recv_string()

                info_list = message.split("::")
                msg_type, msg_value = info_list[:2]

                # handle task requests
                if msg_type == constants.REQ:
                    worker_id = msg_value

                    if r.llen("tasks") > 0:
                        task_id = r.lpop("tasks")

                        if r.exists(task_id):
                            reply_to_task_requests(task_id, worker_id, worker_task_map)

                        else:
                            raise custom_exceptions.TaskNotFound(
                                f"TASK ID '{task_id}' NOT FOUND", task_id
                            )

                    else:
                        worker_socket.send_string(constants.NOWORK)

                # handle task results when received
                elif msg_type == constants.RES:
                    worker_socket.send_string(constants.ACK)
                    update_redis(task=msg_value, worker_task_map=worker_task_map)

                # msg_type == hbt
                # update heartbeats
                else:
                    worker_id = msg_value
                    worker_socket.send_string("pong")
                    worker_last_heartbeat[worker_id] = time.time()

            # check for dead workers and reassign tasks
            check_heartbeat(
                max_heartbeats_missed, worker_last_heartbeat, worker_task_map
            )


def PushTaskDispatcher(port: int) -> None:
    """Task Dispatcher to manage Push Workers"""

    def push_task_to_worker(
        task_id: str,
        available_workers: list[(int, str)],
        worker_task_map: dict[str, set],
    ) -> None:
        """Send task to worker with the request

        Args:
            task_id (str): ID of the task being sent
            available_workers (list): List of number of processors and worker_id pairs(int, str)
            worker_task_map (dict): Dict of work load for each worker
        """
        task = utils.deserialize(r.get(task_id))
        ser_fn = task["fn_payload"]
        ser_params = task["param_payload"]

        #  Push task to worker
        task_data = {
            "task_id": task_id,
            "ser_fn": ser_fn,
            "ser_params": ser_params,
        }

        # update list of available processors with each worker
        curr_processes, worker_id = heapq.heappop(available_workers)
        heapq.heappush(available_workers, (curr_processes + 1, worker_id))

        # Add task to worker's work bucket
        work = worker_task_map.setdefault(worker_id, set())
        work.add(task_id)
        worker_task_map[worker_id] = work

        worker_id = bytes(worker_id, "utf-8")
        msg = bytes(utils.serialize(task_data), "utf-8")
        socket.send_multipart([worker_id, msg])

        # update status of task in redis
        task["status"] = constants.RUNNING
        r.set(task_id, utils.serialize(task))

    def update_redis(
        task: str,
        available_workers: list[(int, str)],
        worker_task_map: dict[str, set],
    ) -> None:
        """Update redis cache with the result of the excuted task

        Args:
            task (str): serialized result from the task execution
            available_workers (list): List of number of processors and worker_id pairs(int, str)
            worker_task_map (dict): Dict of work load for each worker
        """
        task_update_msg = utils.deserialize(task)
        task_id = task_update_msg["task_id"]
        worker_id = task_update_msg["worker_id"]

        if r.exists(task_id):
            task = utils.deserialize(r.get(task_id))
            task["status"] = task_update_msg["status"]
            task["result"] = task_update_msg["result"]

            r.set(task_id, utils.serialize(task))

        # update available processors with the worker
        for i, (curr_processes, curr_worker_id) in enumerate(available_workers):
            if curr_worker_id == worker_id:
                available_workers[i] = (curr_processes - 1, curr_worker_id)
        heapq.heapify(available_workers)

        if task_id in worker_task_map[worker_id]:
            worker_task_map[worker_id].remove(task_id)

    def check_heartbeat(
        max_heartbeats_missed: int,
        worker_last_heartbeat: dict[str, time.time],
        available_workers: list[(int, str)],
        worker_task_map: dict[str, set],
    ) -> None:
        """Check for dead workers and re assign tasks to other workers.

        Args:
            max_heartbeats_missed (int): Number of heartbeats worker can miss without being considered dead
            worker_last_heartbeat (dict): Dict of workers most recent hearbeat
            available_workers (list): List of number of processors and worker_id pairs(int, str)
            worker_task_map (dict): Dict of work load for each worker
        """
        dead_workers = set()
        for worker_id in worker_last_heartbeat:
            if time.time() - worker_last_heartbeat[worker_id] > max_heartbeats_missed:
                dead_workers.add(worker_id)

                if worker_id in worker_task_map:
                    for task_reassign in worker_task_map[worker_id]:
                        task = utils.deserialize(r.get(task_reassign))
                        task["status"] = constants.QUEUED
                        r.set(task_reassign, utils.serialize(task))

                        r.lpush("tasks", task_reassign)

        for dead in dead_workers:
            if dead in worker_task_map:
                del worker_task_map[dead]
            del worker_last_heartbeat[dead]

        for i, (_, worker_id) in enumerate(available_workers):
            if worker_id in dead_workers:
                available_workers.pop(i)

    # number of heartbeats (5 sec) after which a worker is considered dead
    max_heartbeats_missed = 5
    worker_last_heartbeat = dict()

    # maintain available number of processors with each worker
    available_workers = []

    # maintain tasks each worker is assigned
    worker_task_map = dict()
    with zmq.Context() as context:
        socket = context.socket(zmq.ROUTER)
        socket.bind(f"tcp://*:{port}")

        while True:
            if socket.poll(10):
                msg = socket.recv_string()

                info_list = msg.split("::")
                msg_type, *msg_values = info_list

                if msg_type == constants.REG:
                    worker_id, num_processors = msg_values
                    heapq.heappush(
                        available_workers, (-1 * int(num_processors), worker_id)
                    )

                # handle task results when received
                elif msg_type == constants.RES:
                    update_redis(msg_values[0], available_workers, worker_task_map)

                # update heartbeats
                elif msg_type == constants.HBT:
                    worker_id = msg_values[0]
                    worker_last_heartbeat[worker_id] = time.time()

            if (
                r.llen("tasks") > 0
                and available_workers
                and available_workers[0][0] != 0
            ):
                task_id = r.lpop("tasks")

                if r.exists(task_id):
                    push_task_to_worker(task_id, available_workers, worker_task_map)

                else:
                    raise custom_exceptions.TaskNotFound(
                        f"TASK ID '{task_id}' NOT FOUND", task_id
                    )

            # check for dead workers and reassign tasks
            check_heartbeat(
                max_heartbeats_missed,
                worker_last_heartbeat,
                available_workers,
                worker_task_map,
            )


if __name__ == "__main__":
    usage = "python3 task_dispatcher.py -m [local/pull/push] -p <port> -w <num_worker_processors>"

    for i in range(1, len(sys.argv), 2):
        argv = sys.argv[i]
        if argv == "-m":
            worker = sys.argv[i + 1]

        elif argv == "-p":
            port = int(sys.argv[i + 1])

        elif argv == "-w":
            num_worker_processors = int(sys.argv[i + 1])

        else:
            print(usage)
            raise custom_exceptions.InvalidArguments()

    if worker == constants.LOCAL:
        LocalTaskDispatcher(num_worker_processors=num_worker_processors)

    elif worker == constants.PULL:
        PullTaskDispatcher(port=port)

    elif worker == constants.PUSH:
        PushTaskDispatcher(port=port)

    else:
        print(usage)
        raise custom_exceptions.InvalidWorker()
