#include "pipeline.hpp"

namespace dj {

    void input_provider::set_executor(exec::executor* processor) {
        this->processor = processor;
    }

    void input_provider::set_eof_callback(std::function<void()> eof_callback) {
        this->eof_callback = eof_callback;
    }

    execution_pipeline::execution_pipeline() 
    : _inputer(new input::multi_stdin_input<int>()) 
    { }

    execution_pipeline::execution_pipeline(std::unique_ptr<input_provider> inputer) 
    : _inputer(std::move(inputer)) 
    { }

    void execution_pipeline::provide_executor(exec::executor* exec) {
        nodes.provide_executor(exec);
        _inputer->set_executor(exec);
    }

    input_provider& execution_pipeline::get_input_provider() {
        return *_inputer;
    }

    node_graph& execution_pipeline::get_node_graph() {
        return nodes;
    }

}

