#include "../../DistributedJobs"
#include <memory>
#include <vector>
#include <iostream>

template <typename... OutputParameters>
    class add_task : public dj::base_task<OutputParameters...> { };

// this job only emits its input to tripler
template <>
    class add_task<int> : public dj::base_task<int> 
{

    public:
        add_task() : base_task<int>("add_task") { }

        void operator()(int input, const std::string& /* from */) {
            emit<int, dj::enode_type::TASK>(input, "tripler");
        }

        virtual void handle_finish() override {
        }
};

template <typename... OutputParameters>
    class triple_task : public dj::base_task<OutputParameters...> { };

// tripler triples input and when its being terminated sends it right to reducer
// it also adds to counter results from coordinator 
template <>
    class triple_task<int> : public dj::base_task<int> 
{

    public:
        triple_task() : base_task<int>("triple_task") { }

        void operator()(int input, const std::string& from) {
            if(from == "coordinator") {
                counter += input;
            } else {
                counter += 3*input;
                emit<int, dj::enode_type::COORDINATOR>(input, "coordinator");
            }
        }

        virtual void handle_finish() override {
            emit<int, dj::enode_type::REDUCER>(counter);
        }

    private:
        int counter = 0;

};

template <typename CoordinatorInput, typename CoordinatorOutput>
    class dumb_coordinator;

template <>
    class dumb_coordinator<int, int> : public dj::base_coordinator<int, int> 
    {

        public:

            dumb_coordinator() : dj::base_coordinator<int, int>("dumb_coordinator") { }

            virtual void coordinate(const int& input, const std::string& /* parent */) override {
                // I am so dumb, I only return 1 when being poked and only if input is even
                if(!(input&1)) broadcast(1, "tripler");
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
                if(is_root_reducer()) return_output(accumulator);
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
                std::cout << " output: " << input << std::endl;
            }

            virtual void handle_finish() override { }

    };


int main(int argc, char* argv[]) {

    std::vector<std::string> filenames;
    for(int i = 1; i < argc; i++) filenames.emplace_back(argv[i]);

    dj::execution_pipeline exec_pipe(std::unique_ptr<dj::input_provider>(
                new dj::input::multi_file_input_provider<int>(filenames)));
    dj::node_graph& graph = exec_pipe.get_node_graph();

    typedef dj::task<add_task<int>, int> add_job;
    typedef dj::task<triple_task<int>, int> triple_job;
    typedef dj::reducer<add_reducer, int, int, int> add_sink;
    typedef dj::outputer<add_outputer, int> add_out;
    typedef dj::coordinator<dumb_coordinator, int, int> dumb_coordinator;

    std::unique_ptr<dj::task_node> add_job_ptr(new add_job("add_task_node"));
    std::unique_ptr<dj::task_node> triple_job_ptr(new triple_job("tripler"));
    std::unique_ptr<dj::reducer_node> add_sink_ptr(new add_sink("add_sink_node", dj::reducer_node::ereducer_type::SINGLE));
    std::unique_ptr<dj::output_node> add_out_ptr(new add_out("add_out_node"));
    std::unique_ptr<dj::coordinator_node> dumb_coordinator_ptr(new dumb_coordinator("coordinator"));

    uint root_index = graph.add(std::move(add_job_ptr));
    uint triple_index = graph.add(std::move(triple_job_ptr));
    uint reducer_index = graph.add(std::move(add_sink_ptr));
    uint output_index = graph.add(std::move(add_out_ptr));
    uint coordinator_index = graph.add(std::move(dumb_coordinator_ptr));
    graph.add_coordinator(coordinator_index, triple_index);

    graph.set_root(root_index);
    graph.add_output_to_reducer(output_index, reducer_index);
    graph.add_directed(root_index, triple_index);
    graph.add_reducer_to_task(reducer_index, triple_index);

    dj::exec::executor processor(argc, argv, exec_pipe);
    processor.start();

    return 0;
}
