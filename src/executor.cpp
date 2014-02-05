#include "executor.hpp"
#include "message.hpp"
#include "pipeline.hpp"
#include "node.hpp"

#include<functional>

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
            pipeline.get_input_provider().set_eof_callback(
                    std::bind(&executor::eof_callback, this));

            set_reducers();
            set_coordinators();
        }

        context_info executor::context() const {
            return _exec_context;
        }

        execution_pipeline& executor::get_pipeline() {
            return pipeline;
        }

        // TODO better eof handling depending on input types
        void executor::start() {

            if(!pipeline.get_node_graph().is_correct()) 
                throw std::runtime_error("given graph in pipeline is not correct");

            encountered_eof = false;

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

                if(req_status && !is_finished) {
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

            stop_threads();
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
                case work_unit::ework_type::REDUCER_END:
                    {
                        reducer_node* reducer_ptr = graph.reducer(work.index_to);
                        reducer_ptr->process_work(work, nullptr);
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
                case work_unit::ework_type::REDUCER_WORK_OUTPUT:
                    {
                        output_node* output_ptr = graph.output(work.index_to);
                        reducer_node* from = graph.reducer(work.index_from);
                        output_ptr->process_work(work, from);
                    }
                case work_unit::ework_type::TASK_WORK_OUTPUT:
                    {
                        output_node* output_ptr = graph.output(work.index_to);
                        task_node* from = graph.task(work.index_from);
                        output_ptr->process_work(work, from);
                    }
            }
        }

        void executor::stop_threads() {
            if(input_thread) {
                input_thread->join();
                input_thread.reset();
            }
        }

        void executor::set_reducers() {

            node_graph& nodes = pipeline.get_node_graph();
            auto& reducers = nodes.get_reducer_nodes();

            for(auto& rd_ptr: reducers) {
                if(_exec_context.rank == 0) // if zero then root - TODO smarter implementation
                    rd_ptr->set_as_root(true); 
                else 
                    rd_ptr->set_as_root(false);
                reducers_roots[rd_ptr->index()] = 0;

                switch(rd_ptr->reducer_type) {
                    case reducer_node::ereducer_type::SINGLE:
                        {
                            rd_ptr->set_reducers_count(1); 
                            std::vector<uint> ranks = { 0 }; // only zero is there
                            reducers_ranks[rd_ptr->index()] = std::move(ranks);
                        }
                        break;
                    case reducer_node::ereducer_type::MULTIPLE_DYNAMIC:
                    case reducer_node::ereducer_type::MULTIPLE_FIXED:
                        {
                            rd_ptr->set_reducers_count(_exec_context.size); // TODO smarter implementation
                            std::vector<uint> ranks; // only zero is there
                            for(uint i = 0; i < _exec_context.size; i++) ranks.emplace_back(i);
                            reducers_ranks[rd_ptr->index()] = std::move(ranks);
                        }
                        break;
                }
            }
        }

        void executor::set_coordinators() {

            node_graph& nodes = pipeline.get_node_graph();
            auto& coordinators = nodes.get_coordinator_nodes();

            uint rank = 1;
            for(auto& co_ptr: coordinators) { // spread them equally TODO maybe something smarter
                coordinator_ranks[co_ptr->index()] = rank;
                rank = (rank+1)%_exec_context.size;
            }
        }

        void executor::eof_callback() {
            encountered_eof = true;
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
            node_graph& graph = pipeline.get_node_graph();
            switch(from_n_type) {
                case enode_type::OUTPUT:
                    // nothing to do 
                    break;
                case enode_type::REDUCER:
                    switch(to_n_type) {
                        case enode_type::TASK:
                            return std::make_pair(_exec_context.rank, graph.root()->index());
                            break;
                        case enode_type::OUTPUT:
                            {
                                if(!dest.empty()) {
                                     return std::make_pair(_exec_context.rank, graph.output_index(dest));
                                } else if(!graph.get_output_nodes().empty()){
                                    // just return the first one TODO something smarter
                                    return std::make_pair(_exec_context.rank, graph.get_output_nodes()[0]->index());
                                }
                            }
                            break;
                        case enode_type::REDUCER:
                            {
                                uint rank = reducers_roots.at(from_index);
                                return std::make_pair(rank, from_index);
                            }
                        default:
                            break;
                    }
                    break;
                case enode_type::COORDINATOR:
                    if(to_n_type == enode_type::TASK) {
                        if(dest.empty() && graph.coordinator(from_index)->connected_tasks().size() == 1) {
                            return std::make_pair(
                                    _exec_context.rank, graph.coordinator(from_index)->connected_tasks()[0].first); 
                        } else if(!dest.empty()) {
                            // rank is this process - it doesn't matter since 
                            // coordinator sends output everywhere
                            return std::make_pair(_exec_context.rank, graph.task_index(dest)); 
                        }
                    }
                    break;
                case enode_type::TASK:
                    switch(to_n_type) {
                        case enode_type::TASK:
                            // just check if desired task exists, do not check if it is connected, TODO -check
                            return std::make_pair(_exec_context.rank, graph.task_index(dest));
                            break;
                        case enode_type::REDUCER:
                            if(!(dest.empty() && graph.get_reducer_nodes().empty())) {

                                uint ind;
                                if(dest.empty()) ind = graph.get_reducer_nodes()[0]->index();
                                else ind = graph.reducer_index(dest);

                                auto& reducers = reducers_ranks.at(ind);
                                if(reducers.size() == 1) 
                                    return std::make_pair(reducers[0], ind);
                                else {
                                    for(uint r: reducers) if(r == _exec_context.rank) 
                                        return std::make_pair(r, ind);
                                    // just return first element TODO sth smarter
                                    return std::make_pair(reducers[0], ind);
                                }
                            } 
                            break;
                        case enode_type::OUTPUT:
                            if(!(dest.empty() && graph.get_output_nodes().empty())) {

                                uint ind;
                                if(dest.empty()) ind = graph.get_output_nodes()[0]->index();
                                else ind = graph.output_index(dest);
                                return std::make_pair(_exec_context.rank, ind);
                            } 
                            break;
                        case enode_type::COORDINATOR:
                            // just check if desired coordinator exists, do not check if it is connected, TODO -check
                            {
                                uint ind = graph.coordinator_index(dest);
                                return std::make_pair(coordinator_ranks.at(ind), ind);
                            }
                            break;
                    }
                    break;
            }
            throw node_exception("Could not find node for to return rank");
        }

        uint executor::get_root_reducer_rank(uint reducer_index) {

            auto it = reducers_roots.find(reducer_index);
            if(it == end(reducers_roots)) 
                throw node_exception("No reducer for index: " + std::to_string(reducer_index));
            return it->second;
        }

    }
}
