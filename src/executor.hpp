#ifndef EXECUTOR_HPP
#define EXECUTOR_HPP

#include <string>
#include <boost/mpi.hpp>
#include <boost/lockfree/queue.hpp>
#include <thread>
#include "message.hpp"

namespace mpi = boost::mpi;

namespace dj {

    class execution_pipeline;
    enum class enode_type;

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

                template<typename T>
                    void enqueue_input(const T& input) {
                        qd_work.push(new work_unit(
                                    work_unit::get_basic(input, work_unit::ework_type::INPUT_WORK)));
                    }

                void send(const work_unit& work, int to);
                void send(const message& mes, int to);

                // throws if not found
                std::pair<uint, uint> get_rank_and_index_for(enode_type from_n_type, int from_index, 
                        enode_type to_n_type, const std::string& dest) const;

                execution_pipeline& get_pipeline();

            private:
                void stop_threads();
                void request_data();
                void compute_work(work_unit& work);

            private:
                mpi::environment env;
                mpi::communicator world;

                // nonblocking queue with work to be processed
                boost::lockfree::queue<work_unit*> qd_work;
                // mpi request
                boost::mpi::request receive_request;
                boost::optional<mpi::status> req_status;
                // buffer
                std::string buffer; 

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
