#ifndef EXECUTOR_HPP
#define EXECUTOR_HPP

#include <string>
#include <boost/mpi.hpp>
#include <thread>
#include "message.hpp"

namespace mpi = boost::mpi;

namespace dj {

    class execution_pipeline;

    namespace exec {

        /**
         * Class responsible for executoion of pipelined tasks
         * and dispatching messages
         */
        class executor {

            public:

                /**
                 * @param argv is in form - < hostname ... >
                 * @param pipeline this executor call pipeline's provide_executor with this
                 */
                executor(int argc, char* argv[], execution_pipeline& pipeline);

                void start();
                context_info context() const;

            private:
                void stop_threads();
                bool receive_data();

            private:
                mpi::environment env;
                mpi::communicator world;

                // input thread
                std::unique_ptr<std::thread> input_thread;
                // pipeline with all prepared jobs
                execution_pipeline& pipeline;
                // context of execution
                context_info _exec_context;;

        };
    }
}

#endif
