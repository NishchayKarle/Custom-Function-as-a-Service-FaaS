# Function as a Service (FaaS) platform

## Installation and Running:

  - All requirements are in `requirements.txt`. 

  - Install requirements using `pip install -r requirements.tx`

  - To run redis: `docker run -d -p 6379:6379 --name my-redis redis`

## Description of files:

  - `main.py`: Consists all the API endpoints and implementations.

  - `models.py`: Contains all the request and response models of API.

  - `utils.py`: Contains all common functions like `serialise` and `deserialise`.

  - `task_dispatcher.py`: Contains the implementation of task dispatcher.

  - `pull_worker.py`: Contains implementation of pull worker.

  - `push_worker.py`: Contains implementation of push worker.

  - `test_client.py`: Client used to do performance analysis.

  - `tests`: Folder that contains the pytests.

## Reports:
- [Technical report](./reports/technical_report.md)
- [Performance Evaluation](./reports/performance_report.md)
- [Testing report](./reports/testing_report.md)

---