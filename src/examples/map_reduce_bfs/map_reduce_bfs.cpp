#include "../../DistributedJobs"

#include <boost/serialization/export.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/extended_type_info_no_rtti.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <iostream>
#include <vector>
#include <map>
#include <climits>
#include <istream>
#include <ostream>

using namespace std;

// Node structure
struct node {
    int id;
    vector<int> adj;
    int dist;
    enum ecolor { WHITE, GREY, BLACK } color;

    friend ostream& operator<<(ostream& os, const node& n) {
        os << "id: " << n.id << " dist: " << n.dist << " color: " << n.color <<
            " adj: ";
        for(int i: n.adj) os << i << " ";
        return os;
    }

    friend istream& operator>>(istream& is, node& n) {
        n.adj.clear();
        is >> n.id;
        int v, e;
        is >> v;
        for(int i = 0; i < v; i++) {
            is >> e;
            n.adj.push_back(e);
        }
        is >> n.dist;
        if(n.dist) n.dist = INT_MAX;
        else n.dist = 0;
        int c;
        is >> c;
        n.color = (ecolor)c;
        return is;
    }

    private:
        friend class boost::serialization::access;
        template<class Archive> void serialize(Archive& ar, const unsigned int /* version */) {
            ar & id;
            ar & adj;
            ar & dist;
            ar & color;
        }
};

template <typename... OutputParameters>
    class mapper : public dj::base_task<OutputParameters...> { };

// Maps incoming nodes to special tasks responsible for calculating new distance from 0 node
template <>
    class mapper<node> : public dj::base_task<node> 
{

    public:
        mapper() : dj::base_task<node>("mapper") { }

        void operator()(const node& input, const std::string& /* from */) {
            if(all_black) 
                emit<node, dj::enode_type::OUTPUT>(input);
            else {
                a++;
                switch(input.color) {
                    case node::BLACK:
                        b++;
                    case node::WHITE:
                        emit<node, dj::enode_type::TASK>(input, "site", input.id%world_size());
                        break;
                    case node::GREY:
                        {
                            node cp = input;
                            cp.color = node::BLACK;
                            emit<node, dj::enode_type::TASK>(cp, "site", cp.id%world_size());
                            for(auto i: input.adj) {
                                emit<node, dj::enode_type::TASK>(
                                        { i, vector<int>(), input.dist+1, node::GREY }, "site", i%world_size());
                            }
                        }
                        break;
                }
            }
        }

        virtual void handle_finish() override { 
            if(b == a) all_black = true;
            p++;
            b = a = 0;
        }

    private:
        int b = 0, a = 0, p = 0;
        bool all_black = false;
};

template <typename... OutputParameters>
    class site : public dj::base_task<OutputParameters...> { };

// this job is responsible for calculation of new distance
template <>
    class site<node> : public dj::base_task<node> 
{

    public:
        site() : base_task<node>("site") { }

        void operator()(const node& input, const std::string& /* from */) {
            auto it = nodes.find(input.id);
            if(it == end(nodes)) nodes[input.id] = vector<node>{input};
            else nodes[input.id].push_back(input);
        }

        virtual void handle_finish() override {
            for(auto& p: nodes) {
                node n;
                n.id = p.first;
                n.dist = INT_MAX;
                n.color = node::WHITE;
                for(node& np: p.second) {
                    if(!np.adj.empty()) n.adj = np.adj;
                    n.dist = min(n.dist, np.dist);
                    n.color = max(n.color, np.color);
                }
                emit<node, dj::enode_type::REDUCER>(n);
            }
            pass++;
            nodes.clear();
        }
    private:
        int pass = 0;
        map<int, vector<node>> nodes;
};

template <typename PipeInputType, typename InputType, typename OutputType>
    class node_reducer : public dj::base_reducer<PipeInputType, InputType, OutputType> { };

// This job upon completion sends new input to the mapper for the next pass
template <>
    class node_reducer<node, node, node> : public dj::base_reducer<node, node, node> {

        public:
            node_reducer() : dj::base_reducer<node,node,node>("reducer") { }

            virtual void reduce(const node& input, const std::string& /* parent */) override {
                nodes.push_back(input);
            }

            virtual void collect(const node& /*data_to_collect */) override {
            }

            virtual void handle_finish() override {
                for(node& n: nodes) pass_again(n, 0); // mapper is at 0
                nodes.clear();
                pass++;
            }

        private:
            int pass = 0;
            vector<node> nodes;

    };

template <typename OutputerInput>
    class outputer;

template <>
    class outputer<node> : public dj::base_outputer<node> 
    {

        public:
            outputer() : dj::base_outputer<node>("outputer") { }

            virtual void operator()(const node& input, const std::string& /* parent */) override {
                std::cout << "Node: " << input.id << " - " << input.dist << std::endl;
            }

            virtual void handle_finish() override { 
            }

    };

typedef dj::task<mapper<node>, node> mn;
typedef dj::task<site<node>, node> sn;
typedef dj::reducer<node_reducer, node, node, node> rn;
typedef dj::outputer<outputer, node> on;

int main(int argc, char* argv[]) {
    dj::execution_pipeline exec_pipe(std::unique_ptr<dj::input_provider>(
                new dj::input::single_stdin_input<node>()));
    dj::node_graph& graph = exec_pipe.get_node_graph();

    std::unique_ptr<dj::task_node> mapper_ptr(new mn("mapper"));
    std::unique_ptr<dj::task_node> site_ptr(new sn("site"));
    std::unique_ptr<dj::reducer_node> reducer_ptr(new rn("reducer", dj::reducer_node::ereducer_type::MULTIPLE_FIXED));
    std::unique_ptr<dj::output_node> out_ptr(new on("outputer"));

    uint root_index = graph.add(std::move(mapper_ptr));
    uint site_index = graph.add(std::move(site_ptr));
    uint reducer_index = graph.add(std::move(reducer_ptr));
    uint output_index = graph.add(std::move(out_ptr));

    graph.set_root(root_index);
    graph.add_directed(root_index, site_index);
    graph.add_output_to_task(output_index, root_index);
    graph.add_reducer_to_task(reducer_index, site_index);

    dj::exec::executor processor(argc, argv, exec_pipe);
    processor.start();

    return 0;
}
BOOST_SERIALIZATION_FACTORY_0(node)
BOOST_CLASS_EXPORT(node)
BOOST_SERIALIZATION_FACTORY_0(vector<int>)
BOOST_CLASS_EXPORT(vector<int>)
