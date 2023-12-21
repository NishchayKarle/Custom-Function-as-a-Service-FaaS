### Pytests:

1. **Test Correctness**
    - Register multiple functions.
    - Submit multiple randomly chosen tasks to push, pull and local workers with randomly generated args.
    - Calcuate expected results by running the functions within the test.
    - Wait for all the submitted tasks to complete.
    - Test final result of all tasks with expected result.
    - Test return codes, status during all parts of the cycle(Registration, Execution, Result retrival).

2. **Test Failures**
    - Test execution of functions with bad args, exceptions while running the functions

---

### Unit Tests:
* These are list of tests done manually and within pytests as part of bigger tests

1. **Function Registration:**
   - Test registering a function with valid input.
   - Test registering a function with invalid input.
   - Test registering multiple functions and check if they all have unique UUIDs.

2. **Task Execution:**
   - Test executing a task with valid input.
   - Test executing a task with an invalid function UUID.
   - Test executing a task with invalid parameters.
   - Test executing multiple tasks concurrently.

3. **Task Status:**
   - Test retrieving the status of a task that is in QUEUED state.
   - Test retrieving the status of a task that is in RUNNING state.
   - Test retrieving the status of a task that is in COMPLETE state.
   - Test retrieving the status of a task that is in FAILED state.

4. **Task Result:**
   - Test retrieving the result of a task that is in QUEUED state.
   - Test retrieving the result of a task that is in RUNNING state.
   - Test retrieving the result of a task that is in COMPLETE state.
   - Test retrieving the result of a task that is in FAILED state.

5. **Error Handling:**
   - Test handling errors during task execution.
   - Test handling during task status retrieval when there's an error during function execution.
   - Test handling during task result retrieval when there's an error during function execution.

---

### Integration Tests:

1. **End-to-End Workflow:**
   - Test the complete workflow from function registration to task execution to result retrieval.
   - Test if the task goes through all lifecycle states correctly.

2. **Concurrency and Performance:**
   - Test the system's ability to handle multiple concurrent requests.

---

### System Tests:

1. **Task Dispatcher:**
   - Test the local task dispatcher with a baseline implementation.
   - Test the pull task dispatcher with multiple workers.
   - Test the push task dispatcher with multiple workers.

2. **Worker Pool:**
   - Test the pull worker's ability to request tasks and execute them.
   - Test the push worker's ability to receive tasks and return results.

---

### Fault Tolerance Tests:

1. **Serialization Errors:**
   - Test the system's response to serialization errors during function registration and execution.

2. **Worker Failures:**
   - Simulate worker failures and ensure the system can detect and handle them appropriately.
   - Manually killed workers and tested that all the tasks with workers were re routed to other workers.

---

### Performance Evaluation:

   - Results are included in performance_report.md

1. **Push vs. Pull Model:**
   - Evaluate the performance of the push and pull models under different workloads.

2. **Scalability:**
   - Test how well the system scales with an increasing number of worker processes.

3. **Latency vs. Throughput:**
   - Measure the latency of task execution and the overall throughput of the system.

4. **Function Types:**
   - Test the system with different types of functions, "no-op" and "sleep" tasks.

---