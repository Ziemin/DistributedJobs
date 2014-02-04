#include "node.hpp"

namespace dj {

    // --- node_exception -----------------

    node_exception::node_exception(std::string message) : message(std::move(message)) { }

    const char* node_exception::what() const throw() {
        return message.c_str();
    }

    // --- base_node -----------------
    base_node::base_node(enode_type nt, std::string name) 
        : type(nt), _name(std::move(name))
    { }

    bool base_node::operator==(const base_node& other) const {
        return name() == other.name() && type == other.type;
    }

    bool base_node::operator!=(const base_node& other) const {
        return !(*this == other);
    }

    std::string base_node::name() const {
        return _name;
    }

    int base_node::index() const {
        return _index;
    }

    void base_node::set_index(int index) {
        _index = index;
    }


    // --- task_node -----------------
    task_node::task_node(std::string name)
        : base_node(enode_type::TASK, std::move(name))
    { }

    const std::vector<std::pair<uint, const task_node*>>& task_node::connected_tasks() const {
        return edge_tasks;
    }

    const std::vector<std::pair<uint, const coordinator_node*>>& task_node::connected_coorindators() const {
        return edge_coordinators;
    }

    bool task_node::add_task(uint task_num, const task_node* task) {
        auto new_task = std::make_pair(task_num, task);
        // check if such task is already added
        for(auto &t: edge_tasks) if(new_task == t) return false;

        edge_tasks.emplace_back(task_num, task);
        return true;
    }

    bool task_node::add_coordinator(uint coordinator_num, const coordinator_node* coordinator) {
        auto new_coordinator = std::make_pair(coordinator_num, coordinator);
        // check if such coordinator is already added
        for(auto &c: edge_coordinators) if(new_coordinator == c) return false;

        edge_coordinators.emplace_back(coordinator_num, coordinator);
        return true;
    }


    // --- reducer_node -----------------
    reducer_node::reducer_node(std::string name)
        : base_node(enode_type::REDUCER, std::move(name)) 
    { }

    // --- coorindator_node -----------------
    coordinator_node::coordinator_node(std::string name)
        : base_node(enode_type::COORDINATOR, std::move(name)) 
    { }

    // --- output_node -----------------
    output_node::output_node(std::string name)
        : base_node(enode_type::OUTPUT, std::move(name)) 
    { }


    // --- node_graph -----------------

    int node_graph::add(std::unique_ptr<task_node> task) {
        auto tp = make_pair(enode_type::TASK, task->name());
        if(name_to_index.find(tp) == end(name_to_index)) {

            int new_index = task_nodes.size();
            task->set_index(new_index);
            task_nodes.emplace_back(std::move(task));
            name_to_index[tp] = new_index;

            return new_index;
        } else
            throw node_exception("task with given name is already added");
    }

    int node_graph::add(std::unique_ptr<reducer_node> reducer) {
        auto tp = make_pair(enode_type::REDUCER, reducer->name());
        if(name_to_index.find(tp) == end(name_to_index)) {

            int new_index = reducer_nodes.size();
            reducer->set_index(new_index);
            reducer_nodes.emplace_back(std::move(reducer));
            name_to_index[tp] = new_index;

            return new_index;
        } else
            throw node_exception("reducer with given name is already added");
    }

    int node_graph::add(std::unique_ptr<coordinator_node> coordinator) {

        auto tp = make_pair(enode_type::COORDINATOR, coordinator->name());
        if(name_to_index.find(tp) == end(name_to_index)) {

            int new_index = coordinator_nodes.size();
            coordinator->set_index(new_index);
            coordinator_nodes.emplace_back(std::move(coordinator));
            name_to_index[tp] = new_index;

            return new_index;
        } else
            throw node_exception("coordinator with given name is already added");
    }

    int node_graph::add(std::unique_ptr<output_node> output) {

        auto tp = make_pair(enode_type::OUTPUT, output->name());
        if(name_to_index.find(tp) == end(name_to_index)) {

            int new_index = output_nodes.size();
            output->set_index(new_index);
            output_nodes.emplace_back(std::move(output));
            name_to_index[tp] = new_index;

            return new_index;
        } else
            throw node_exception("output with given name is already added");
    }


    int node_graph::task_index(const std::string& node_name) const {
        auto it = name_to_index.find(make_pair(enode_type::TASK, node_name));
        if(it == end(name_to_index)) return -1;
        else return it->second;
    }

    int node_graph::reducer_index(const std::string& node_name) const {
        auto it = name_to_index.find(make_pair(enode_type::REDUCER, node_name));
        if(it == end(name_to_index)) return -1;
        else return it->second;
    }

    int node_graph::output_index(const std::string& node_name) const {
        auto it = name_to_index.find(make_pair(enode_type::OUTPUT, node_name));
        if(it == end(name_to_index)) return -1;
        else return it->second;
    }

    int node_graph::coordinator_index(const std::string& node_name) const {
        auto it = name_to_index.find(make_pair(enode_type::COORDINATOR, node_name));
        if(it == end(name_to_index)) return -1;
        else return it->second;
    }


    void node_graph::add_output_to_task(uint output_index, uint task_index) {
        if(output_index > output_nodes.size()) 
            throw node_exception("Output with given index does not exist");
        if(task_index > task_nodes.size()) 
            throw node_exception("Task with given index does not exist");

        auto tp = std::make_pair(enode_type::TASK, task_index);
        if(sink_map.find(tp) == end(sink_map)) {
            sink_map[tp] = std::make_pair(enode_type::COORDINATOR, output_index);
        } else 
            throw node_exception("End of task is already connected");
    }

    void node_graph::add_reducer_to_task(uint reducer_index, uint task_index) {
        if(reducer_index > reducer_nodes.size()) 
            throw node_exception("Reducer with given index does not exist");
        if(task_index > task_nodes.size()) 
            throw node_exception("Task with given index does not exist");

        auto tp = std::make_pair(enode_type::TASK, task_index);
        if(sink_map.find(tp) == end(sink_map)) {
            sink_map[tp] = std::make_pair(enode_type::REDUCER, reducer_index);
        } else 
            throw node_exception("End of task is already connected");
    }

    void node_graph::add_output_to_reducer(uint output_index, uint reducer_index) {
        if(reducer_index > reducer_nodes.size()) 
            throw node_exception("Reducer with given index does not exist");
        if(output_index > output_nodes.size()) 
            throw node_exception("Output with given index does not exist");

        auto tp = std::make_pair(enode_type::REDUCER, reducer_index);
        if(sink_map.find(tp) == end(sink_map)) {
            sink_map[tp] = std::make_pair(enode_type::OUTPUT, output_index);
        } else 
            throw node_exception("End of reducer is already connected");
    }

    void node_graph::add_undirected(uint task_from, uint task_to) {
        add_directed(task_from, task_to);
        add_directed(task_to, task_from);
    }

    void node_graph::add_directed(uint task_from, uint task_to) {
        if(task_from > task_nodes.size() || task_to > task_nodes.size()) 
            throw node_exception("Task with given index does not exist");

        task_nodes[task_from]->add_task(task_to, task_nodes[task_to].get());
    }

    void node_graph::add_coordinator(uint coordinator_index, uint task_to) {
        if(task_to > task_nodes.size()) 
            throw node_exception("Task with given index does not exist");
        if(coordinator_index > coordinator_nodes.size()) 
            throw node_exception("Coordinator with given index does not exist");

        task_nodes[task_to]->add_coordinator(coordinator_index, coordinator_nodes[coordinator_index].get());
    }

    int node_graph::set_root(std::unique_ptr<task_node> node) {
        return root_index = add(std::move(node));
    }

    void node_graph::set_root(uint index) {
        if(index > task_nodes.size()) 
            throw node_exception("Task with given index does not exist");
        root_index = index;
    }

    bool node_graph::has_root() const {
        return root_index != -1;
    }

    const task_node* node_graph::root() const {
        return (root_index == -1) ? nullptr : task_nodes[root_index].get();
    }

    bool node_graph::is_correct() const {
        if(root_index == -1) return false;
        // TODO check if every not connected task has reducer or output
        // meanwhile only one reducer is available for entire graph
        return true;
    }

    void node_graph::provide_executor(exec::executor* processor) {
        for(auto& t: task_nodes) t->set_executor(processor);
        for(auto& t: reducer_nodes) t->set_executor(processor);
        for(auto& t: coordinator_nodes) t->set_executor(processor);
        for(auto& t: output_nodes) t->set_executor(processor);
    }
}

