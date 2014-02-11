#include "task.hpp"

namespace dj {

    base_unit::base_unit(std::string name) : _name(std::move(name)) { }

    std::string base_unit::name() const {
        return _name;
    }

    void base_unit::set_executor(exec::executor* processor) {
        this->processor = processor;
    }

    void base_unit::set_node_index(int index) {
        _index = index;
    }

    int base_unit::index() const {
        return _index;
    }

    int base_unit::world_size() const {
        return (processor != nullptr) ? processor->context().size : 0;
    }

    int base_unit::rank() const {
        return (processor != nullptr) ? processor->context().rank : -1;
    }
}
