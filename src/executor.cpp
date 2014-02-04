#include "executor.hpp"
#include "message.hpp"
#include "pipeline.hpp"

namespace dj {

    namespace exec {

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

        void executor::start() {

            input_thread.reset(new std::thread([this]() { pipeline.get_input_provider()(); }));
        }

        void executor::stop_threads() {
            if(input_thread) {
                input_thread->join();
                input_thread.reset();
            }
        }

        bool executor::receive_data() {
            return true;
        }
    }
}
