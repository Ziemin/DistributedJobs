#include "../../DistributedJobs"
#include <iostream>


template <typename... OutputParameters>
    class mapper : public dj::base_task<OutputParameters...> { };

// this job only emits its input to tripler
template <>
    class mapper<int> : public dj::base_task<int> 
{

    public:
        mapper() : base_task<int>("mapper") { }

        void operator()(int input, const std::string& /* from */) {
            emit<int, dj::enode_type::TASK>(input, "doubler", counter%world_size());
            counter++;
        }

        virtual void handle_finish() override {

        }
    private:
        int counter = 0;
};


template <typename... OutputParameters>
    class doubler : public dj::base_task<OutputParameters...> { };

// this job only emits its input to tripler
template <>
    class doubler<int> : public dj::base_task<int> 
{

    public:
        doubler() : base_task<int>("doubler") { }

        void operator()(int input, const std::string& /* from */) {
            emit<int, dj::enode_type::REDUCER>(2*input);
            std::cout << "emitting from: " << rank() << std::endl;
        }

        virtual void handle_finish() override {

        }
    private:
        int counter = 0;
};

template <typename OutputerInput>
    class add_outputer;

template <>
    class add_outputer<int> : public dj::base_outputer<int> 
    {

        public:
            add_outputer() : dj::base_outputer<int>("add_outputer") { }

            virtual void operator()(const int& input, const std::string& /* parent */) override {
                std::cout << " output: " << input << std::endl;
            }

            virtual void handle_finish() override { }

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
                if(is_root_reducer()) {
                    if(accumulator < max_val) pass_again(accumulator);
                    else return_output(accumulator);
                }
            }

        private:
            int accumulator = 0;
            int max_val = 50;

    };

typedef dj::task<mapper<int>,int> mp_n;
typedef dj::task<doubler<int>,int> doubler_n;
typedef dj::reducer<add_reducer, int, int, int> add_sink;
typedef dj::outputer<add_outputer, int> add_out;

int main(int argc, char* argv[]) {

    std::unique_ptr<dj::task_node> mapper_ptr(new mp_n("mapper"));
    std::unique_ptr<dj::task_node> doubler_ptr(new doubler_n("doubler"));
    std::unique_ptr<dj::reducer_node> add_sink_ptr(new add_sink("add_sink_node", dj::reducer_node::ereducer_type::SINGLE));
    std::unique_ptr<dj::output_node> add_out_ptr(new add_out("add_out_node"));

    dj::execution_pipeline exec_pipe;
    dj::node_graph& graph = exec_pipe.get_node_graph();

    uint root_index = graph.add(std::move(mapper_ptr));
    uint doubler_index = graph.add(std::move(doubler_ptr));
    uint reducer_index = graph.add(std::move(add_sink_ptr));
    uint output_index = graph.add(std::move(add_out_ptr));
    graph.set_root(root_index);
    graph.add_output_to_reducer(output_index, reducer_index);
    graph.add_directed(root_index, doubler_index);
    graph.add_reducer_to_task(reducer_index, doubler_index);

    dj::exec::executor processor(argc, argv, exec_pipe);
    processor.start();

    return 0;
}
