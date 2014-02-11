#include "../../DistributedJobs"
#include <boost/serialization/export.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/extended_type_info_no_rtti.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <tuple>
#include <random>
#include <istream>
#include <ostream>

typedef std::mt19937 my_rng;

struct edge {

    int u, v;

    friend bool operator==(const edge& e1, const edge& e2) {
        return (e1.u == e2.u && e1.v == e2.v) || (e1.u == e2.v && e1.v == e2.u);
    }

    friend bool operator!=(const edge& e1, const edge& e2) {
        return !(e1 == e2);
    }

    friend  std::istream& operator>>(std::istream& is, edge& e) {
        is >> e.u;
        is >> e.v;
        return is;
    }

    friend  std::ostream& operator<<(std::ostream& os, const edge& e) {
        os << e.u << " " << e.v << std::endl;
        return os;
    }

    private:
        friend class boost::serialization::access;
        template<class Archive> void serialize(Archive& ar, const unsigned int /* version */) {
            ar & u;
            ar & v;
        }
};

inline bool is_adjacent(const edge& e1, const edge& e2) {
    if(e1.u == e2.u && e1 != e2) return true;
    if(e1.v == e2.u && e1 != e2) return true;
    if(e1.u == e2.v && e1 != e2) return true;
    if(e1.v == e2.v && e1 != e2) return true;
    return false;
}

inline bool is_star(const edge& e1, const edge& e2, const edge& e3) {
    return 
        (e1.u == e2.u && e1.u == e3.u) ||
        (e1.u == e2.u && e1.u == e3.v) ||
        (e1.u == e2.v && e1.u == e3.u) ||
        (e1.u == e2.v && e1.u == e3.v) ||
        (e1.v == e2.u && e1.v == e3.u) ||
        (e1.v == e2.u && e1.v == e3.v) ||
        (e1.v == e2.v && e1.v == e3.u) ||
        (e1.v == e2.v && e1.v == e3.v);
}

inline bool is_triangle(const edge& e1, const edge& e2, const edge& e3) {
    if(e1 == e2 || e1 == e3 || e2 == e3) return false;
    return is_adjacent(e1, e2) && is_adjacent(e1, e3) && is_adjacent(e2, e3) && !is_star(e1, e2, e3);
}

struct coordinator_message {

    int b;
    double w;
    edge e;

    private:
        friend class boost::serialization::access;
        template<class Archive> void serialize(Archive& ar, const unsigned int /* version */) {
            ar & b;
            ar & w;
            ar & e;
        }
};

struct site_result {

    int c;
    std::tuple<edge, edge, edge, bool> t;
    int m;

    private:
        friend class boost::serialization::access;
        template<class Archive> void serialize(Archive& ar, const unsigned int /* version */) {
            ar & c;
            ar & std::get<0>(t);
            ar & std::get<1>(t);
            ar & std::get<2>(t);
            ar & std::get<3>(t);
            ar & m;
        }
};

// site node
template <typename... OutputParameters>
    class site;

template <>
    class site<site_result, coordinator_message> : public dj::base_task<site_result, coordinator_message> 
{
    public:

        site() 
            : dj::base_task<site_result, coordinator_message>("site"), rng(std::random_device{}()), dist(0, 1.0) 
        { 
            std::get<3>(t) = false;
        }

        void operator()(const edge& e, const std::string& parent) {
            m++;
            double w = dist(rng);
            // level 1 sampling
            bool sampled1 = false;
            if(w < l1) {
                sampled1 = true;
                f1 = e;
                has1 = true;
                has2 = false;
                l1 = w;
                l2 = 1;
                c = 0;
                emit<coordinator_message, dj::enode_type::COORDINATOR>({1, w, e}, "coordinator");
            } 
            // level 2 sampling
            bool sampled2 = false;
            if(!sampled1 && has1 && is_adjacent(e, f1)) c++;
            w = dist(rng);
            if(w < l2) {
                sampled2 = true;
                f2 = e;
                has2 = true;
                l2 = w;
                emit<coordinator_message, dj::enode_type::COORDINATOR>({2, w, e}, "coordinator");
            }
            // triangle completion
            if(!sampled1 && !sampled2) {
                //std::cout << "edges:\n" << f1 << f2 << e;
                if(has1 && has2 && is_triangle(e, f1, f2)) {
                    std::cout << "triangle found" << std::endl;
                    t = std::make_tuple(e, f1, f2, true);
                }
            }
        }

        void operator()(const coordinator_message& cm, const std::string& parent) {
            std::get<3>(t) = false;
            if(cm.b == 1) {
                has1 = true;
                f1 = cm.e;
                l1 = cm.w;
                has2 = false;
                c = 0;
            } else {
                has2 = true;
                f2 = cm.e;
                l2 = cm.w;
            }
        }

        virtual void handle_finish() override {
            emit<site_result, dj::enode_type::REDUCER>({c, t, m});
        }

    private:
        my_rng rng;
        std::uniform_real_distribution<double> dist;
        bool has1 = false, has2 = false;
        edge f1, f2;
        int c = 0;
        int m = 0;
        double l1 = 1, l2 = 1;
        std::tuple<edge, edge, edge, bool> t;
};


// coordinator node
template <typename CoordinatorInput, typename CoordinatorOutput>
    class coordinator;

template <> 
    class coordinator<coordinator_message, coordinator_message> : public dj::base_coordinator<coordinator_message, coordinator_message> 
{

    public:
        coordinator() : dj::base_coordinator<coordinator_message, coordinator_message>("coordinator") { }

        virtual void coordinate(const coordinator_message& input, const std::string& /* parent */) override {
            if(input.b == 1 && g1 < input.w) {
                g1 = input.w;
                broadcast(input, "site");
            } else if(input.b == 2 && g2 < input.w) {
                g2 = input.w;
                broadcast(input, "site");
            }
            g1 = input.w;
            g2 = input.w;
        }

        virtual void handle_finish() override { }

    private:
        double g1 = 1;
        double g2 = 1;

};


// reducer ndoe
template <typename PipeInputType, typename InputType, typename OutputType>
    class reducer;

template <>
    class reducer<edge, site_result, int> : public dj::base_reducer<edge, site_result, int> 

{
    public:

        reducer() : dj::base_reducer<edge, site_result, int>("reducer") { }

        virtual void reduce(const site_result& input, const std::string& parent) override {
            m += input.m;
            c += input.c;
            std::cout << "got: " << input.m << " " << input.c << std::endl
                << std::get<0>(input.t) << " " << std::get<1>(input.t) << " " << std::get<2>(input.t) << std::endl;
            if(std::get<3>(input.t))
                found_triangle |= is_triangle(std::get<0>(input.t), std::get<1>(input.t), std::get<2>(input.t));
        }

        virtual void collect(const int& /* data_to_collect */) override {

        }

        virtual void handle_finish() override {
            return_output((found_triangle) ? c*m : 0);
        }

    private:
        int m = 0;
        int c = 0;
        bool found_triangle = false;
};


// output node
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
                new dj::input::multi_file_input_provider<edge>(filenames)));
    dj::node_graph& graph = exec_pipe.get_node_graph();

    typedef dj::task<site<site_result, coordinator_message>, edge, coordinator_message> site_task;
    typedef dj::reducer<reducer, edge, site_result, int> site_reducer;
    typedef dj::outputer<add_outputer, int> out;
    typedef dj::coordinator<coordinator, coordinator_message, coordinator_message> site_coordinator;

    std::unique_ptr<dj::task_node> site_ptr(new site_task("site"));
    std::unique_ptr<dj::reducer_node> reducer_ptr(new site_reducer("reducer", dj::reducer_node::ereducer_type::SINGLE));
    std::unique_ptr<dj::coordinator_node> coordinator_ptr(new site_coordinator("coordinator"));
    std::unique_ptr<dj::output_node> out_ptr(new out("out"));

    uint root_index = graph.add(std::move(site_ptr));
    uint c_index = graph.add(std::move(coordinator_ptr));
    uint r_index = graph.add(std::move(reducer_ptr));
    uint o_index = graph.add(std::move(out_ptr));

    graph.set_root(root_index);
    graph.add_coordinator(c_index, root_index);
    graph.add_reducer_to_task(r_index, root_index);
    graph.add_output_to_reducer(o_index, r_index);

    dj::exec::executor processor(argc, argv, exec_pipe);
    processor.start();

    return 0;
}

BOOST_SERIALIZATION_FACTORY_0(edge)
BOOST_CLASS_EXPORT(edge)
BOOST_SERIALIZATION_FACTORY_0(coordinator_message)
BOOST_CLASS_EXPORT(coordinator_message)
BOOST_SERIALIZATION_FACTORY_0(site_result)
BOOST_CLASS_EXPORT(site_result)
