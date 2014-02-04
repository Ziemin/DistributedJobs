#include "executor.hpp"
#include "message.hpp"
#include "pipeline.hpp"
#include "node.hpp"

namespace dj {

    namespace exec {

        inline bool is_work_tag(int tag) {
            return (tag >= 0 && tag <= static_cast<int>(work_unit::ework_type::COORDINATOR_COORDINATE));
        }

        executor::executor(int argc, char* argv[], execution_pipeline& pipeline)
            : pipeline(pipeline) 
        {
            _exec_context.rank = world.rank();
            _exec_context.size = world.size();
            if(argc >= 2) _exec_context.hostname = argv[1];

            locale_info::_context = &_exec_context;
            pipeline.provide_executor(this);
        }

        context_info executor::context() const {
            return _exec_context;
        }

        execution_pipeline& executor::get_pipeline() {
            return pipeline;
        }

        void executor::start() {

            if(!pipeline.get_node_graph().is_correct()) 
                throw std::runtime_error("given graph in pipeline is not correct");

            input_thread.reset(new std::thread([this]() { pipeline.get_input_provider()(); }));
            bool is_finished = false;
            bool pending_request = false;

            while(!is_finished) {

                if(!pending_request) {
                    request_data();
                    pending_request = true;
                }

                std::unique_ptr<work_unit> new_work;
                work_unit* work_ptr;
                while(qd_work.pop(work_ptr)) {

                    new_work.reset(work_ptr);
                    compute_work(*new_work);
                }

                if(req_status) {
                    pending_request = false;
                    if(is_work_tag(req_status->tag())) {
                        message mes = { req_status->tag(), buffer };
                        work_ptr = new work_unit();
                        *work_ptr << mes;
                        qd_work.push(work_ptr);
                    } else {
                        throw std::runtime_error("Unrecognized tag: " + std::to_string(req_status->tag()));
                    }
                } 
            }
        }

        void executor::compute_work(work_unit& work) {

            node_graph& graph = pipeline.get_node_graph();
            switch(work.work_type) {
                case work_unit::ework_type::INPUT_WORK:
                    {
                        work.work_type = work_unit::ework_type::TASK_WORK;
                        task_node* task_ptr = graph.task(work.index_to);
                        task_ptr->process_work(work, nullptr);
                    }
                    break;
                case work_unit::ework_type::TASK_WORK:
                    {
                        task_node* task_ptr = graph.task(work.index_to);
                        task_node* from = graph.task(work.index_from);
                        task_ptr->process_work(work, from);
                    }
                    break;
                case work_unit::ework_type::REDUCER_COLLECT:
                    {
                        reducer_node* reducer_ptr = graph.reducer(work.index_to);
                        reducer_ptr->process_work(work, nullptr);
                    }
                    break;
                case work_unit::ework_type::REDUCER_REDUCE:
                    {
                        reducer_node* reducer_ptr = graph.reducer(work.index_to);
                        task_node* from = graph.task(work.index_from);
                        reducer_ptr->process_work(work, from);
                    }
                    break;
                case work_unit::ework_type::COORDINATOR_COORDINATE:
                    {
                        coordinator_node* coordinator_ptr = graph.coordinator(work.index_to);
                        task_node* from = graph.task(work.index_from);
                        coordinator_ptr->process_work(work, from);
                    }
                    break;
                case work_unit::ework_type::COORDINATOR_OUTPUT:
                    {
                        task_node* task_ptr = graph.task(work.index_to);
                        coordinator_node* from = graph.coordinator(work.index_from);
                        task_ptr->process_work(work, from);
                    }
                    break;
            }
        }

        void executor::stop_threads() {
            if(input_thread) {
                input_thread->join();
                input_thread.reset();
            }
        }

        void executor::request_data() {
            receive_request = world.irecv(mpi::any_source, mpi::any_tag, buffer); 
        }

        void executor::send(const work_unit& work, int to) {

            if(to != (int) _exec_context.rank) {
                message mes; 
                mes << work;
                if(to == -1) // send to all other and process work myself
                    qd_work.push(new work_unit(work));

                send(mes, to);
            } else {
                qd_work.push(new work_unit(work));
            }
        }

        void executor::send(const message& mes, int to) {

            if(to == -1) { // send to all others
                for(uint i = 0; i < _exec_context.size; i++) {
                    if(i == _exec_context.rank) continue;
                    world.isend(i, mes.tag, mes.data);
                }
            } else if(to != (int)_exec_context.rank) {
                world.isend(to, mes.tag, mes.data);
            } else
                throw std::runtime_error("Cannot send message to myself");
        }

        std::pair<uint, uint> executor::get_rank_and_index_for(enode_type from_n_type, int from_index, 
                enode_type to_n_type, const std::string& dest) const 
        {

        }

    }
}
