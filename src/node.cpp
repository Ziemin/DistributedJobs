#include "node.hpp"

namespace dj {

    // --- base_node -----------------
    base_node::base_node(enode_type nt) 
        : type(nt) 
    { }

    bool base_node::operator==(const base_node& other) const {
        return name() == other.name();
    }

    bool base_node::operator!=(const base_node& other) const {
        return !(*this == other);
    }

    // --- task_node -----------------
    task_node::task_node()
        : base_node(enode_type::TASK), _reducer(nullptr), _coordinator(nullptr)
    { }

    bool task_node::add_parent(base_node* parent) {
        if(parent == nullptr) return false;
        // parents must not be multiplied
        if(parent->type != enode_type::TASK || parent->type != enode_type::INPUT)
            return false;
        for(auto p: _parents) if(*p == *parent) return false;
        _parents.push_back(parent);
        return true;
    }

    bool task_node::add_child(task_node* child) {
        if(child == nullptr) return false;
        // cannot add child while having reducer already
        if(has_reducer()) return false;
        // children must not be multiplied
        for(auto c: _children_tasks) if(*c == *child) return false;

        _children_tasks.push_back(child);
        return true;
    }

    bool task_node::add_reducer(reducer_node* reducer) {
        if(reducer == nullptr) return false;
        // cannot have reducer not being the last in the pipeline
        if(!_children_tasks.empty()) return false;

        _reducer = reducer;
        return true;
    }

    bool task_node::has_reducer() const {
        return _reducer == nullptr;
    }

    bool task_node::has_coordinator() const {
        return _coordinator == nullptr;
    }

    const std::vector<base_node*>& task_node::parents() const {
        return _parents;
    }

    const std::vector<task_node*>& task_node::children() const {
        return _children_tasks;
    }

    const reducer_node* task_node::reducer() const {
        return _reducer;
    }

    const coordinator_node* task_node::coordinator() const {
        return _coordinator;
    }

    // --- reducer_node -----------------
    reducer_node::reducer_node()
        : base_node(enode_type::REDUCER) 
    { }

    bool reducer_node::add_parent(task_node* parent) {
        if(parent == nullptr) return false;
        // parents must not be multiplied
        for(auto p: _parents) if(*p == *parent) return false;
        _parents.push_back(parent);
        return true;
    }

    const std::vector<task_node*>& reducer_node::parents() const {
        return _parents;
    }
}

