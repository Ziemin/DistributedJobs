#include "executor.hpp"
#include "message.hpp"
#include "pipeline.hpp"
#include "node.hpp"

#include <cmath>
#include <functional>

namespace dj {

    namespace exec {

        inline bool is_work_tag(int tag) {
            return (tag >= 0 && tag <= static_cast<int>(work_unit::ework_type::TASK_WORK_OUTPUT));
        }

        inline bool is_end_tag(int tag) {
            return (tag >= static_cast<int>(end_message::eend_message_type::TASK_END) 
                    && tag <= static_cast<int>(end_message::eend_message_type::WORK_END));
        }

        executor::executor(int argc, char* argv[], execution_pipeline& pipeline)
            : qd_work(20), 
            pipeline(pipeline)
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
            bool had_work = false;

            input_thread.reset(new std::thread([this]() { pipeline.get_input_provider()(); }));
            is_finished = false;
            bool pending_request = false;
            sent_task_end = false;
            sent_reduction_end = false;
            sent_work_end = false;
            going_again = false;
            finished = 0;
            pass_number = 0;
            c_s = computation_stage::INPUT_READ;

            boost::optional<mpi::status> req_status;

            wait_end_que.clear();

            std::unique_ptr<work_unit> new_work;
            std::unique_ptr<end_message> end_mes;

            while(!is_finished) {

                // send request if it is not sent already
                if(!pending_request) {
                    request_data();
                    pending_request = true;
                }

                // process work in queue
                work_unit* work_ptr;
                had_work = false;
                while(qd_work.pop(work_ptr)) {
                    had_work = true;
                    new_work.reset(work_ptr);
                    compute_work(*new_work);
                }
                if(going_again) {
                    going_again = false; // we started recurrence
                    reset_run();
                }
                // process messages related only if had no work previously
                if(!had_work) {
                    while(!end_que.empty()) {
                        end_mes.reset(end_que.front());
                        end_que.pop_front();
                        process_end_message(*end_mes.get(), had_work);
                    }
                }

                // check if new messages appeard
                req_status = receive_request.test();
                if(req_status && !is_finished) {

                    pending_request = false;
                    // enqueue new work
                    if(is_work_tag(req_status->tag())) {
                        message mes = { req_status->tag(), buffer };
                        work_ptr = new work_unit();
                        *work_ptr << mes;
                        qd_work.push(work_ptr);
                    // enqueue end messages
                    } else if(is_end_tag(req_status->tag())) {
                        message mes = { req_status->tag(), buffer };
                        end_message* end_ptr = new end_message();
                        *end_ptr << mes;
                        end_que.push_back(end_ptr);
                    } else { // something is fucked up
                        throw std::runtime_error("Unrecognized tag: " + std::to_string(req_status->tag()));
                    }
                // check if we should start a "circle of death"
                // WARNING in current implementation only process with rank = 0 can start circle of death
                } else if(encountered_eof && !had_work && _exec_context.rank == 0) { 
                    if(!sent_task_end && c_s == computation_stage::INPUT_READ) {
                        tell_about_the_end(end_message::eend_message_type::TASK_END, 1, _exec_context.rank, 1);
                        sent_task_end = true;
                    } else if(!sent_reduction_end && c_s == computation_stage::TASKS_END) {
                        tell_about_the_end(end_message::eend_message_type::REDUCTION_END, 1, _exec_context.rank, 1);
                        sent_reduction_end = true;
                    } else if(!sent_work_end && c_s == computation_stage::REDUCTION_END) {
                        tell_about_the_end(end_message::eend_message_type::WORK_END, 1, _exec_context.rank, 1);
                        sent_work_end = true;
                    }
                }
            }

            world.barrier(); // wait for others to finish

            stop_threads();
        }

        void executor::compute_work(work_unit& work) {

            node_graph& graph = pipeline.get_node_graph();
            switch(work.work_type) {
                case work_unit::ework_type::INPUT_WORK:
                    {
                        reset_run();
                        // index to is not important in input work
                        work.work_type = work_unit::ework_type::TASK_WORK;
                        task_node* task_ptr = graph.task(pipeline.get_node_graph().root()->index());
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
                case work_unit::ework_type::REDUCER_WORK_OUTPUT:
                    {
                        output_node* output_ptr = graph.output(work.index_to);
                        reducer_node* from = graph.reducer(work.index_from);
                        output_ptr->process_work(work, from);
                    }
                    break;
                case work_unit::ework_type::TASK_WORK_OUTPUT:
                    {
                        output_node* output_ptr = graph.output(work.index_to);
                        task_node* from = graph.task(work.index_from);
                        output_ptr->process_work(work, from);
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
            buffer.clear();
            receive_request = world.irecv(mpi::any_source, mpi::any_tag, buffer); 
        }

        void executor::send(const work_unit& work, int to) {

            // going for recursion
            if(work.work_type == work_unit::ework_type::INPUT_WORK) {
                going_again = true;
            }

            if(to != (int) _exec_context.rank) {
                message mes; 
                mes << work;
                if(to == -1) // send to all other and process work myself
                    qd_work.push(new work_unit(work));

                async_send(mes, to);
            } else {
                qd_work.push(new work_unit(work));
            }
        }

        void executor::async_send(const message& mes, int to) {

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

        void executor::send(const message& mes, int to) {

            if(to == -1) { // send to all others
                for(uint i = 0; i < _exec_context.size; i++) {
                    if(i == _exec_context.rank) continue;
                    world.send(i, mes.tag, mes.data);
                }
            } else if(to != (int)_exec_context.rank) {
                world.send(to, mes.tag, mes.data);
            } else
                throw std::runtime_error("Cannot send message to myself");
        }

        void executor::tell_about_the_end(// sounds so sad...
                end_message::eend_message_type end_type, uint counter, uint from_rank, uint pass_number)
        {
            //if(context().rank == 1) {
                //std::cout << "telling about the end from: " << context().rank 
                    //<< "pass: " << pass_number << " counter: " <<  counter << " type: " << (int) end_type << std::endl;
                //std::cout << "current stage: " << (int) c_s << std::endl;
                //std::cout << "encountered eof: " << encountered_eof << std::endl;
            //}
            end_message end_mes { from_rank, pass_number, counter, end_type }; 
            message mes;
            mes << end_mes;

            if(_exec_context.size == 1) 
                end_que.push_back(new end_message(end_mes));
            else 
                send(mes, (_exec_context.rank+1)%_exec_context.size);
        }

        // TODO queue end_messages and change circle of death test
        void executor::process_end_message(end_message& mes, bool had_work) {

            //if(context().rank == 1) 
                //std::cout << "processing end : " << mes.from_rank
                    //<< "pass: " << mes.pass_number << " counter: " <<  mes.counter << " type: " << (int) mes.end_type << std::endl;
            switch(mes.end_type) {
                case end_message::eend_message_type::TASK_END:
                    process_task_end_message(mes, had_work);
                    break;
                case end_message::eend_message_type::REDUCTION_END:
                    process_reduction_end_message(mes, had_work);
                    break;
                case end_message::eend_message_type::WORK_END:
                    process_work_end_message(mes, had_work);
                    break;
            }
        }

        void executor::process_task_end_message(end_message& mes, bool had_work) {
            // already finished that stage of work
            if(c_s != computation_stage::INPUT_READ) { // we have alraedy left that state
                if(mes.from_rank == _exec_context.rank) return;
                tell_about_the_end(
                        end_message::eend_message_type::TASK_END, mes.counter+1, mes.from_rank, mes.pass_number);
                return;
            }
            if(mes.from_rank == _exec_context.rank) { // our message finished its circle
                // we had some work, probably propagated it, we should refrain from finishing
                if(had_work) {
                    sent_task_end = false;
                } else if(mes.counter == 2*_exec_context.size) {
                    // now we can finish all tasks - everyone is done with them
                    finish_all_tasks();
                    c_s = computation_stage::TASKS_END;
                } else {
                    if(mes.pass_number == 1 && mes.counter == _exec_context.size) 
                        tell_about_the_end(
                                end_message::eend_message_type::TASK_END, mes.counter+1, mes.from_rank, mes.pass_number+1);
                    else 
                        sent_task_end = false;
                }
            } else {
                bool tell_at_the_end = false;;
                uint counter;
                if(mes.pass_number == 1) {
                    if(!had_work && encountered_eof) counter = mes.counter+1;
                    else counter = mes.counter;
                    tell_at_the_end = true;
                } else {
                    if(!had_work && encountered_eof) {
                        counter = mes.counter+1;
                        tell_about_the_end(
                                end_message::eend_message_type::TASK_END, counter, mes.from_rank, mes.pass_number);
                        if(mes.counter == _exec_context.size + abs(_exec_context.rank-mes.from_rank)) {
                            finish_all_tasks();
                            c_s = computation_stage::TASKS_END;
                        }
                    } else {
                        counter = mes.counter;
                        tell_at_the_end = true;
                    }
                }
                if(tell_at_the_end)
                    tell_about_the_end(
                            end_message::eend_message_type::TASK_END, counter, mes.from_rank, mes.pass_number);
            }
        }

        void executor::process_reduction_end_message(end_message& mes, bool had_work) {
            if(c_s == computation_stage::REDUCTION_END || c_s == computation_stage::WORK_END) {
                if(mes.from_rank == _exec_context.rank) return;
                tell_about_the_end(
                        end_message::eend_message_type::REDUCTION_END, mes.counter+1, mes.from_rank, mes.pass_number);
            }
            else if(c_s == computation_stage::INPUT_READ) return;
            if(mes.from_rank == _exec_context.rank) { // our message finished its circle
                // we had some work, probably propagated it, we should refrain from finishing
                if(had_work) {
                    sent_reduction_end = false;
                } else if(mes.counter == 2*_exec_context.size) {
                    // now we can finish all tasks - everyone is done with them
                    finish_all_reducers();
                    c_s = computation_stage::REDUCTION_END;
                } else {
                    if(mes.pass_number == 1 && mes.counter == _exec_context.size) 
                        tell_about_the_end(
                                end_message::eend_message_type::REDUCTION_END, mes.counter+1, mes.from_rank, mes.pass_number+1);
                    else 
                        sent_task_end = false;
                }
            } else {
                bool tell_at_the_end = false;
                uint counter;
                if(mes.pass_number == 1) {
                    if(!had_work) counter = mes.counter+1;
                    else counter = mes.counter;
                    tell_at_the_end = true;
                } else {
                    if(!had_work) {
                        counter = mes.counter+1;
                        tell_about_the_end(
                                end_message::eend_message_type::REDUCTION_END, counter, mes.from_rank, mes.pass_number);
                        if(mes.counter == _exec_context.size + abs(_exec_context.rank-mes.from_rank)) {
                            finish_all_reducers();
                            c_s = computation_stage::REDUCTION_END;
                        }
                    } else {
                        tell_at_the_end = true;
                        counter = mes.counter;   
                    }
                }
                if(tell_at_the_end)
                    tell_about_the_end(
                            end_message::eend_message_type::REDUCTION_END, counter, mes.from_rank, mes.pass_number);
            }
        }

        void executor::process_work_end_message(end_message& mes, bool had_work) {
            if(c_s == computation_stage::WORK_END) {
                if(mes.from_rank == _exec_context.rank) return;
                tell_about_the_end(
                        end_message::eend_message_type::WORK_END, mes.counter+1, mes.from_rank, mes.pass_number);
                return;
            }
            else if(c_s != computation_stage::REDUCTION_END) return;
            if(mes.from_rank == _exec_context.rank) { // our message finished its circle
                // we had some work, probably propagated it, we should refrain from finishing
                if(had_work) {
                    sent_work_end = false;
                } else if(mes.counter == 2*_exec_context.size) {
                    is_finished = true;
                    c_s = computation_stage::WORK_END;
                } else {
                    if(mes.pass_number == 1 && mes.counter == _exec_context.size) 
                        tell_about_the_end(
                                end_message::eend_message_type::WORK_END, mes.counter+1, mes.from_rank, mes.pass_number+1);
                    else 
                        sent_work_end = false;
                }
            } else {
                uint counter;
                if(mes.pass_number == 1) {
                    if(!had_work) counter = mes.counter+1;
                    else counter = mes.counter;
                } else {
                    if(!had_work) {
                        if(mes.counter == _exec_context.size + abs(_exec_context.rank-mes.from_rank)) {
                            is_finished = true;
                            c_s = computation_stage::WORK_END;
                        }
                        counter = mes.counter+1;
                    } else counter = mes.counter;
                }
                tell_about_the_end(
                        end_message::eend_message_type::WORK_END, counter, mes.from_rank, mes.pass_number);
            }
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

        void executor::finish_all_tasks() {

            auto& tasks = pipeline.get_node_graph().get_task_nodes();
            for(auto& t: tasks) t->handle_finish();
        }

        void executor::finish_all_reducers() {
            // TODO only finishing root reducer now
            auto& reducers = pipeline.get_node_graph().get_reducer_nodes();
            for(auto& r: reducers) r->handle_finish();
        }

        void executor::reset_run() {
            c_s = computation_stage::INPUT_READ;
            sent_task_end = false;
            sent_reduction_end = false;
            sent_work_end = false;
        }

    }
}
