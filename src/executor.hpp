#ifndef EXECUTOR_HPP
#define EXECUTOR_HPP

namespace dj {
    namespace exec {

        /**
         * Context of te execution - contains information
         * about running tasks in pipeline, parallel tasks, coordinators and reducers
         */
        struct Context {

        };

        /**
         * Class responsible for executoion of pipelined tasks
         * and dispatching messages
         */
        class Executor {

            public:
                void start();

                void addTask();
                void addCoordinator();
                void addReducer();
        };

    }
}

#endif
