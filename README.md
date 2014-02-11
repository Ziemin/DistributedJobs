DistributedJobs 
===============
It is a framework to run mpi based application.
To use it DistributedJobs header has to be included and libdj.a linked.

Framework enables user to specify graph of connected jobs to run.
Jobs are devided into types:
- input - responsible for providing input to the program
- task - fundametnal job, can be used to do batched and stream computations.
        In addition to that, task can be also treated as a mapper to implement map reduce algorithms.
        To implement task a relevant operator ()(const T& input, const string& parent) 
        has to be overloaded in implementation of our task.
- reducer - job to accumulate data from tasks and perform reduce operation at the and of reduction phase
          reducer can be both run on single and multiple machines
- coordinator - a job connected to some task able to broadcast results to all processes
- output - job whose duty is to print results

Entire flow of computation can be devided to phases:
- TASKS - in this phase input is dispatched to tasks, then they are triggered.
        At the and of this phase handle_finish is executed for every task
- REDUCTION - At the and of this handle_finish is executed for every reducer
            Then reducer can use pass_again method to create recursion. As a consequence
            new input may be provided to root tasks.
- PIPE_END - phase when all reducers are finished and no new input is created 
- WORK_END - program is finished, ready to close


Build
-----

Build requires cmake, boost 1.50, gcc 4.8 and openmpi 1.6.5

To build library with all examples and tests:
- mkdir build (in project's directory)
- cd build
- cmake ..
- make

Library is in generated lib folder and all binaries in bin

Examples
--------
All examples can be found in src/examples directory
