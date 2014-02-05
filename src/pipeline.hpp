#ifndef PIPELINE_HPP
#define PIPELINE_HPP

#include <iostream>
#include "node.hpp"
#include "executor.hpp"

namespace dj {

    namespace exec { class executor; }

    class node_graph;

    class input_provider {

        public:
            /**
             * Function running to provide input for tasks
             */
            virtual void operator()() = 0;

            void set_executor(exec::executor* processor);
            void set_eof_callback(std::function<void()> eof_callback);

        protected:
            exec::executor* processor = nullptr;

            template <typename InputType>
                void add_input(const InputType& input) {
                    processor->enqueue_input(input);
                }

            std::function<void()> eof_callback;
    };

    namespace input {
        /**
         * Every process reads his own input
         */
        template <typename T> 
            class multi_stdin_input : public input_provider {

                public:

                    virtual void operator()() {

                        T in_val;
                        while(std::cin.eof()) {
                            std::cin >> in_val;
                            add_input(in_val);
                        }
                        eof_callback();
                    }
            };
        
    }

    class execution_pipeline {

        public:
            execution_pipeline(); // sets default input_provider
            execution_pipeline(std::unique_ptr<input_provider> inputer);

            void provide_executor(exec::executor* exec);

            input_provider& get_input_provider();
            node_graph& get_node_graph();

        private:
            node_graph nodes;
            std::unique_ptr<input_provider> _inputer;

    };

}

#endif
