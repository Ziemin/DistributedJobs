#ifndef TASK_HPP
#define TASK_HPP

#include <string>
#include <type_traits>
#include <iostream>
#include "node.hpp"
#include "template_utils.hpp"
#include "executor.hpp"

namespace dj {


    class base_unit {

        public:
            virtual ~base_unit() = default;

            base_unit(std::string name);

            std::string name() const;
            void set_executor(exec::executor* processor);
            void set_node_index(int index);
            int index() const;

            int world_size() const;
            int rank() const;
            virtual void handle_finish() = 0; // no more data will be delivered in this pass

        private:
            std::string _name;
            int _index = -1;

        protected:
            exec::executor* processor = nullptr;
    };

    template <typename... OutputParameters>
        class base_task : public base_unit {

            public:
                base_task(std::string name) : base_unit(std::move(name)) { }

            protected:
                /**
                 * this emits a result of task directed to the target of name target
                 * since this is a task, even though TargetType can be OUTPUT it may be
                 * delivered to reducer if such is registered. The same for opposite site
                 */
                template <typename T, enode_type TargetType>
                    void emit(const T& value, const std::string& target="", int rk=-2) const {
                        using serialization::operator<<;

                        static_assert(is_any_same<T, OutputParameters...>{}, 
                                "Cannot emit value of undeclared output parameter");

                        work_unit result;
                        result.data << value;
                        result.type_name = typeid(T).name();
                        result.index_from = index();
                        result.locale = locale_info::get_basic();

                        std::pair<uint, uint> identity;

                        if(target.empty() && (TargetType == enode_type::REDUCER || TargetType == enode_type::OUTPUT)) {

                            try {
                                identity = processor->get_rank_and_index_for(
                                        enode_type::TASK, index(), TargetType, target);
                                if(TargetType == enode_type::REDUCER) 
                                    result.work_type = work_unit::ework_type::REDUCER_REDUCE;
                                else
                                    result.work_type = work_unit::ework_type::TASK_WORK_OUTPUT;
                            } catch(const node_exception& e) {
                                try {
                                    if(TargetType == enode_type::REDUCER) {
                                        identity = processor->get_rank_and_index_for(
                                                enode_type::TASK, index(), enode_type::OUTPUT, target);
                                        result.work_type = work_unit::ework_type::TASK_WORK_OUTPUT;
                                    } else {
                                        identity = processor->get_rank_and_index_for(
                                                enode_type::TASK, index(), enode_type::REDUCER, target);
                                        result.work_type = work_unit::ework_type::REDUCER_REDUCE;
                                    }
                                } catch(const node_exception& e) {
                                    throw std::runtime_error("Could not dispatch result of task anywhere");
                                }
                            }
                        } else {
                            try {
                                identity = processor->get_rank_and_index_for(
                                        enode_type::TASK, index(), TargetType, target);
                            } catch(const node_exception& e) {
                                throw std::runtime_error("Could not dispatch result of task anywhere");
                            }
                            switch(TargetType) {
                                case enode_type::TASK:
                                    result.work_type = work_unit::ework_type::TASK_WORK;
                                    break;
                                case enode_type::REDUCER:
                                    result.work_type = work_unit::ework_type::REDUCER_REDUCE;
                                    break;
                                case enode_type::COORDINATOR:
                                    result.work_type = work_unit::ework_type::COORDINATOR_COORDINATE;
                                    break;
                                case enode_type::OUTPUT:
                                    result.work_type = work_unit::ework_type::TASK_WORK_OUTPUT;
                                    break;
                            }
                        }
                        result.index_to = identity.second;

                        if(rk == -2) processor->send(result, identity.first);
                        else processor->send(result, rk);
                    }
        };


    class task_node;

    template <typename PipeInputType, typename InputType, typename OutputType>
        class base_reducer : public base_unit {

            public:
                base_reducer(std::string name) : base_unit(std::move(name)) { }

                virtual void reduce(const InputType& input, const std::string& parent) = 0;
                virtual void collect(const OutputType& data_to_collect) = 0;

                bool is_root_reducer() const {
                    return is_root;
                }

                void set_as_root(bool value) {
                    is_root = value;
                }

            protected:
                /**
                 * Sends output back to the first task in pipeline
                 */
                void pass_again(const PipeInputType& pipe_input, int rn=-2) {
                    using serialization::operator<<;

                    work_unit work;
                    work.work_type = work_unit::ework_type::INPUT_WORK;
                    work.data << pipe_input;
                    work.type_name = typeid(PipeInputType).name();
                    work.index_from = index();
                    work.locale = locale_info::get_basic();

                    std::pair<uint, uint> identity = processor->get_rank_and_index_for(
                            enode_type::REDUCER, index(), enode_type::TASK, ""); // it defaults to root node

                    work.index_to = identity.second;

                    if(rn == -2) processor->send(work, identity.first);
                    else processor->send(work, rn);
                }

                /**
                 * Returns reduced output
                 */
                void return_output(const OutputType& output) {
                    using serialization::operator<<;
                    work_unit work;
                    work.work_type = work_unit::ework_type::REDUCER_WORK_OUTPUT;
                    work.data << output;
                    work.type_name = typeid(OutputType).name();
                    work.index_from = index();

                    std::pair<uint, uint> identity = processor->get_rank_and_index_for(
                            enode_type::REDUCER, index(), enode_type::OUTPUT, ""); // it defaults to root node

                    work.index_to = identity.second;

                    processor->send(work, identity.first);
                }

                /**
                 * Sends reduced value to the root reducer of reducers group
                 */
                void send_to_root(const OutputType& output) {
                    if(is_root_reducer()) return; // we are root, should not duplicate our reduced data

                    work_unit work;
                    work.work_type = work_unit::ework_type::REDUCER_COLLECT;
                    work.data << output;
                    work.type_name = typeid(OutputType).name();
                    work.index_from = index();
                    work.index_to = index();
                    work.locale = locale_info::get_basic();
                    uint to = processor->get_root_reducer_rank(index());

                    processor->send(work, to);
                }

            private:
                bool is_root = false;
        };

    template <typename InputType, typename OutputType>
        class base_coordinator : public base_unit {

            public:
                base_coordinator(std::string name) : base_unit(std::move(name)) { }

                virtual void coordinate(const InputType& input, const std::string& parent) = 0;

            protected:
                /**
                 * Broadcasts coordinator output to all connected tasks
                 */
                void broadcast(const OutputType& coordinator_output, const std::string& target="") {

                    using serialization::operator<<;
                    work_unit work;
                    work.work_type = work_unit::ework_type::COORDINATOR_OUTPUT;
                    work.type_name = typeid(OutputType).name();
                    work.data << coordinator_output;
                    work.index_from = index();
                    work.locale = locale_info::get_basic();

                    std::pair<uint, uint> identity = processor->get_rank_and_index_for(
                            enode_type::COORDINATOR, index(), enode_type::TASK, target); // it defaults to root node
                    work.index_to = identity.second;

                    processor->send(work, -1); // to all
                }
        };

    template <typename InputType>
        class base_outputer : public base_unit {

            public:
                base_outputer(std::string name) : base_unit(std::move(name)) { }

                virtual void operator()(const InputType& input, const std::string& parent) = 0;
        };
}

#endif
