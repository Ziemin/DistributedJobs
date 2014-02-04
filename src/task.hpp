#ifndef TASK_HPP
#define TASK_HPP

#include <string>
#include <type_traits>
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
                 */
                template <typename T, enode_type TargetType>
                    void emit(const T& value, const std::string& target="") const {

                        static_assert(is_any_same<T, OutputParameters...>{}, 
                                "Cannot emit value of undeclared output parameter");

                        std::pair<uint, uint> identity = processor->get_rank_and_index_for(
                                enode_type::TASK, index(), TargetType, target);
                        work_unit result;
                        result.data << value;
                        result.index_from = index();
                        result.index_to = identity.second;
                        result.type_name = typeid(T).name();
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
                                result.work_type = work_unit::ework_type::WORK_OUTPUT;
                                break;
                        }

                        processor->send(result, identity.first);
                    }
        };


    class task_node;

    template <typename PipeInputType, typename InputType, typename OutputType>
        class base_reducer : public base_unit {

            public:
                base_reducer(std::string name) : base_unit(std::move(name)) { }

                virtual void reduce(const InputType& input, const task_node* parent) = 0;
                virtual void collect(const OutputType& data_to_collect) = 0;

            protected:
                /**
                 * Sends output back to the first task in pipeline
                 */
                void pass_again(const PipeInputType& pipe_input) {

                }

                /**
                 * Returns reduced output
                 */
                void return_output(const OutputType& output) {

                }

                /**
                 * Sends reduced value to the root reducer of reducers group
                 */
                void send_to_root(const OutputType& output) {

                }
        };

    template <typename InputType, typename OutputType>
        class base_coordinator : public base_unit {

            public:
                base_coordinator(std::string name) : base_unit(std::move(name)) { }

                virtual void coordinate(const InputType& input) = 0;

            protected:
                /**
                 * Broadcasts coordinator output to all connected tasks
                 */
                void broadcast(const OutputType& coordinator_output) {

                }

            protected:
        };
}

#endif
