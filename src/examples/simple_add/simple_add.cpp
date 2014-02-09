#include "../../DistributedJobs"
#include <memory>

template <typename... OutputParameters>
    class add_task : public dj::base_task<OutputParameters...> { };

template <>
    class add_task<int> : public dj::base_task<int> 
{

    public:
        add_task() : base_task<int>("add_task") { }

        void operator()(int input, const std::string& /* from */) {
            counter += input;
        }

        virtual void handle_finish() override {
            emit<int, dj::enode_type::REDUCER>(counter);
        }

    private:
        int counter = 0;

};

template <typename PipeInputType, typename InputType, typename OutputType>
    class add_reducer : public dj::base_reducer<PipeInputType, InputType, OutputType> { };

template <>
    class add_reducer<int, int, int> : public dj::base_reducer<int, int, int> {

        public:
            add_reducer() : dj::base_reducer<int,int,int>("add_reducer") { }

            virtual void reduce(const int& input, const std::string& parent) override {
                accumulator += input;
            }

            virtual void collect(const int& data_to_collect) override {
                accumulator += data_to_collect;
            }

            virtual void handle_finish() override {
                return_output(accumulator);
            }

        private:
            int accumulator = 0;

    };

template <typename OutputerInput>
    class add_outputer;

template <>
    class add_outputer<int> : public dj::base_outputer<int> 
    {

        public:
            add_outputer() : dj::base_outputer<int>("add_outputer") { }

            virtual void operator()(const int& input, const std::string& /* parent */) override {
                std::cout << name() << " output: " << input << std::endl;
            }

            virtual void handle_finish() override { }

    };

int main(int argc, char* argv[]) {

    dj::execution_pipeline exec_pipe;
    dj::node_graph& graph = exec_pipe.get_node_graph();

    typedef dj::task<add_task<int>, int> add_job;
    typedef dj::reducer<add_reducer, int, int, int> add_sink;
    typedef dj::outputer<add_outputer, int> add_out;

    std::unique_ptr<dj::task_node> add_job_ptr(new add_job("add_task_node"));
    std::unique_ptr<dj::reducer_node> add_sink_ptr(new add_sink("add_sink_node", dj::reducer_node::ereducer_type::SINGLE));
    std::unique_ptr<dj::output_node> add_out_ptr(new add_out("add_out_node"));

    uint root_index = graph.add(std::move(add_job_ptr));
    uint reducer_index = graph.add(std::move(add_sink_ptr));
    uint output_index = graph.add(std::move(add_out_ptr));

    graph.set_root(root_index);
    graph.add_output_to_reducer(output_index, reducer_index);
    graph.add_reducer_to_task(reducer_index, root_index);

    dj::exec::executor processor(argc, argv, exec_pipe);
    processor.start();

    return 0;
}
