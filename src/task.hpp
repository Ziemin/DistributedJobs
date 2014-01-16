#ifndef TASK_HPP
#define TASK_HPP

#include <string>

namespace dj {

    /**
     * Base task class for pipelined tasks, reducers and coordinators
     */
    class BaseTask {

        public:
            std::string getName() const;

    };

    template <typename Input, typename Output>
        class Task : BaseTask {

        };

    template <typename Input, typename Output>
        class Coordinator : BaseTask {

        };

    template <typename Input, typename Output>
        class Reducer : BaseTask {

        };
}

#endif
