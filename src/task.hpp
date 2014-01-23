#ifndef TASK_HPP
#define TASK_HPP

#include <string>
#include <type_traits>
#include "template_utils.hpp"

namespace dj {


    class base_unit {

        public:
            virtual ~base_unit() = default;

            base_unit(std::string name) : _name(std::move(name)) { }

            std::string name() const {
                return _name;
            }

        private:
            std::string _name;
    };

    template <typename... OutputParameters>
        class base_task : base_unit {

            public:
                base_task(std::string name) : base_unit(std::move(name)) { }

            protected:
                /**
                 * this emits a result of task directed to the target of name target
                 */
                template <typename T>
                    void emit(const T& value, const std::string& target="") const {

                        static_assert(is_any_same<T, OutputParameters...>{}, 
                                "Cannot emit value of undeclared output parameter");

                    }

        };


    class task_node;

    template <typename PipeInputType, typename InputType, typename OutputType>
        class base_reducer : base_unit {

            public:
                base_reducer(std::string name) : base_unit(std::move(name)) { }

                virtual void reduce(const InputType& input, const task_node* parent) = 0;
                virtual void collect(const OutputType& data_to_collect) = 0;

            protected:
                void pass_again(const PipeInputType& pipe_input) {

                }

                void return_output(const OutputType& output) {

                }
        };

}

#endif
