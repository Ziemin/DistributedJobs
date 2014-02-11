#ifndef EXECUTOR_HPP
#define EXECUTOR_HPP

#include <string>
#include <boost/mpi.hpp>
#include <boost/lockfree/queue.hpp>
#include <thread>
#include <atomic>
#include <unordered_map>
#include <deque>
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
                                    work_unit::get_basic(input, work_unit::ework_type::INPUT_WORK, 0, 0)));
                    }

                void send(work_unit& work, int to);
                void async_send(const message& mes, int to);
                void send(const message& mes, int to);

                /**
                 * if target is empty and from_n_type is reducer, connected output_node's identity is returned
                 * if target is emtpy and there are more than one node possible to localize fo given node
                 * an arbitrary choice dependent on implementation is returned
                 * @return pair of (rank, index) of nodes
                 */
                std::pair<uint, uint> get_rank_and_index_for(enode_type from_n_type, int from_index, 
                        enode_type to_n_type, const std::string& dest) const;

                uint get_root_reducer_rank(uint reducer_index);

                execution_pipeline& get_pipeline();

            private:
                void set_reducers();
                void set_coordinators();
                void stop_threads();
                void request_data();
                void compute_work(work_unit& work);
                void eof_callback();
                void tell_about_the_end(// sounds so sad...
                        end_message::eend_message_type end_type, uint counter, uint from_rank, uint pass_number); 

                void process_end_message(end_message& mes, bool had_work);
                void process_task_end_message(end_message& mes, bool had_work);
                void process_reduction_end_message(end_message& mes, bool had_work);
                void process_work_end_message(end_message& mes, bool had_work);

                void finish_all_tasks();
                void finish_all_reducers();
                void reset_run();

            private:

                ecomputation_phase phase;
                mpi::environment env;
                mpi::communicator world;

                // nonblocking queue with work to be processed
                boost::lockfree::queue<work_unit*> qd_work;
                uint finished;
                uint current_pass;

                std::deque<end_message*> end_que;
                std::deque<end_message*> wait_end_que;
                // mpi request
                boost::mpi::request receive_request;
                // buffer
                std::string buffer; 

                // input thread
                std::unique_ptr<std::thread> input_thread;
                // pipeline with all prepared jobs
                execution_pipeline& pipeline;
                // context of execution
                context_info _exec_context;

                std::unordered_map<uint, std::vector<uint>> reducers_ranks;
                std::unordered_map<uint, uint> reducers_roots;
                std::unordered_map<uint, uint> coordinator_ranks;

                std::atomic_bool encountered_eof;

                bool is_finished;
                bool sent_task_end;
                bool sent_reduction_end;
                bool sent_work_end;
                bool going_again;
                int pass_number;
        };
    }
}

#endif
