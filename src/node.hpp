#ifndef NODE_HPP
#define NODE_HPP

#include <functional>
#include <vector>
#include <memory>
#include <unordered_map>
#include <exception>

#include "template_utils.hpp"
#include "message.hpp"


namespace dj {

    template <typename T, typename B, typename Y> class base_reducer;
    template <typename T, typename B> class base_coordinator;
    template <typename T> class base_outputer;

    enum class enode_type {
        TASK, 
        REDUCER, 
        COORDINATOR,
        OUTPUT
    };

    class node_exception : std::exception {
        public:

            node_exception(std::string message);
            virtual ~node_exception() = default;;
            virtual const char* what() const throw();

        private:
            std::string message;
    };
}


namespace std {

    // hash implementations for unordered_map
    template <>
        struct hash<pair<dj::enode_type, string>> {

            typedef pair<dj::enode_type, string> argument_type;
            typedef size_t value_type;

            value_type operator()(const argument_type& argument) const {
                const value_type hash_1(hash<uint>()(static_cast<uint>(argument.first)) * 31);
                const value_type hash_2(hash<string>()(argument.second));
                return hash_2 ^ (hash_1 << 1);
            }
        };

    template <>
        struct hash<pair<dj::enode_type, uint>> {

            typedef pair<dj::enode_type, uint> argument_type;
            typedef size_t value_type;

            value_type operator()(const argument_type& argument) const {
                const value_type hash_1(hash<uint>()(static_cast<uint>(argument.first)) * 31);
                const value_type hash_2(hash<uint>()(argument.second));
                return hash_2 ^ ((hash_1+31) << 1);
            }
        };
}

namespace dj {

    class node_graph;

    class base_node {

        friend class node_graph;

        public:
            base_node(enode_type nt, std::string name);
            virtual ~base_node() = default;

            std::string name() const;
            bool operator==(const base_node& other) const;
            bool operator!=(const base_node& other) const;

            virtual void process_work(const work_unit& work, base_node* parent) = 0;
            virtual void set_executor(exec::executor* processor) = 0;
            virtual void set_index(int index) = 0;
            virtual int index() const = 0;

            const enode_type type;

        private:

            std::string _name;
    };

    class coordinator_node;

    class task_node : public base_node {

        friend class node_graph;

        public:
            task_node(std::string name);
            virtual ~task_node() = default;

            const std::vector<std::pair<uint, const task_node*>>& connected_tasks() const;
            const std::vector<std::pair<uint, const coordinator_node*>>& connected_coorindators() const;

        private:
            bool add_task(uint task_num, const task_node* task);
            bool add_coordinator(uint coordinator_num, const coordinator_node* coordinator);

            std::vector<std::pair<uint, const task_node*>> edge_tasks;
            std::vector<std::pair<uint, const coordinator_node*>> edge_coordinators;

    };

    class reducer_node : public base_node {

        friend class node_graph;

        public:
            
            enum class ereducer_type {
                SINGLE,             // only on physical reducer exists
                MULTIPLE_DYNAMIC,   // multiple number of physical reducers exists, it can be changed 
                MULTIPLE_FIXED      // fixed number of multiple reducers exists
            };
            const ereducer_type reducer_type;

            reducer_node(std::string name, ereducer_type reducer_type);

            //current implementation sets count to world.size()
            void set_reducers_count(int number);
            int reducers_count() const;
            virtual void set_as_root(bool value) = 0;

        private:

            int _reducers_count = 1;
    };

    class coordinator_node : public base_node {

        friend class node_graph;

        public:
            coordinator_node(std::string name);

            const std::vector<std::pair<uint, const task_node*>>& connected_tasks() const;

        private:
            bool add_task(uint task_num, const task_node* task);
            std::vector<std::pair<uint, const task_node*>> edge_tasks;

    };

    class output_node : public base_node {

        friend class node_graph;

        public:
            output_node(std::string name);
    };

    class node_graph {

        public:

            int add(std::unique_ptr<task_node> task);
            int add(std::unique_ptr<reducer_node> reducer);
            int add(std::unique_ptr<coordinator_node> coordinator);
            int add(std::unique_ptr<output_node> output);

            int task_index(const std::string& node_name) const;
            int reducer_index(const std::string& node_name) const;
            int output_index(const std::string& node_name) const;
            int coordinator_index(const std::string& node_name) const;

            void add_output_to_task(uint output_index, uint task_index);
            void add_reducer_to_task(uint reducer_index, uint task_index);

            void add_output_to_reducer(uint output_index, uint reducer_index);

            void add_undirected(uint task_from, uint task_to);
            void add_directed(uint task_from, uint task_to);
            void add_coordinator(uint coordinator_index, uint task_to);

            int set_root(std::unique_ptr<task_node> node);
            void set_root(uint index);
            bool has_root() const;
            const task_node* root() const;

            bool is_correct() const;

            void provide_executor(exec::executor* processor);

            /**
             * @throws runtime_error if no node for index exists
             */
            task_node* task(uint index);
            /**
             * @throws runtime_error if no node for index exists
             */
            reducer_node* reducer(uint index);
            /**
             * @throws runtime_error if no node for index exists
             */
            coordinator_node* coordinator(uint index);
            /**
             * @throws runtime_error if no node for index exists
             */
            output_node* output(uint index);

            std::vector<std::unique_ptr<task_node>>& get_task_nodes();
            std::vector<std::unique_ptr<reducer_node>>& get_reducer_nodes();
            std::vector<std::unique_ptr<coordinator_node>>& get_coordinator_nodes();
            std::vector<std::unique_ptr<output_node>>& get_output_nodes();

        private:

            std::vector<std::unique_ptr<task_node>> task_nodes;
            std::vector<std::unique_ptr<reducer_node>> reducer_nodes;
            std::vector<std::unique_ptr<coordinator_node>> coordinator_nodes;
            std::vector<std::unique_ptr<output_node>> output_nodes;

            int root_index = -1;

            std::unordered_map<std::pair<enode_type, std::string>, uint> name_to_index;
            std::unordered_map<std::pair<enode_type, uint>, std::pair<enode_type, uint>> sink_map;
    };

    template <typename, typename...>
        class task { };

    /**
     * node class is a task container responsible for 
     * prividing input to the task
     *
     *  An instance of a class:
     *  - operators () has to be defined to get input and process it
     *  - the same is for initialize method
     * 
     *  template<typename... OutputParameters>
     *  class Task : BaseTask<OutputParameters...> {
     *
     *      public:
     *          void operator()(const int& a, const base_node* parent) {
     *              BaseTask<OutputParameters...>::emit(a);
     *          }
     *
     *          void operator()(const float& f, const base_node* parent) {
     *              BaseTask<OutputParameters...>::emit(f);
     *          }
     *
     *          void operator()(const std::string& s, const base_node* parent) {
     *              BaseTask<OutputParameters...>::emit(s);
     *          }
     *
     *          void initialize(int a, std::string b, const base_node* parent) {
     *              ... do sth ...
     *          }
     *
     *      .
     *      .
     *      .
     *  };
     */
    template <template<typename...> class Task, typename... OutputParameters, typename... InputParameters>
        class task<Task<OutputParameters...>, InputParameters...> : public task_node {

            template <typename T>
                struct type_checker {

                        bool operator()(const work_unit& work, base_node* parent, Task<OutputParameters...>& task) const {
                            if(work.type_name != typeid(T).name()) 
                                return false;

                            T t;
                            work.data >> t;
                            if(parent != nullptr) task(t, parent->name());
                            else task(t, "");

                            return true;
                        }
                };

            public:

                task(std::string name) : task_node(std::move(name)) { }

                std::string task_name() const {
                    return _task.name();
                }

                virtual void set_index(int index) {
                    _task.set_node_index(index);
                }

                virtual int index() const {
                    return _task.index();
                }

                virtual void set_executor(exec::executor* processor) {
                    _task.set_executor(processor);
                }

                virtual void process_work(const work_unit& work, base_node* parent) {
                    if(!for_each_any<type_checker, InputParameters...>::run(work, parent, _task))
                            throw std::runtime_error("Input for task is not any of given types");
                }

                /**
                 * Initializes task
                 */
                template <typename... Args>
                    void initialize_task(Args&& ...args) {
                        _task.initialize(std::forward<Args>(args)...);
                    }

            private:
                /**
                 * Run task on provided argument.
                 * Argument has to be one of the types from InputParameters list.
                 */
                template <typename Arg>
                    void run(const Arg& arg, const base_node* parent) {
                        static_assert(is_any_same<Arg, InputParameters...>{}, "cannot provide this argument to task");
                        _task(arg, parent);
                    }

            private:
                // runnable task
                Task<OutputParameters...> _task;

        };


    template <template<typename...> class Coordinator, typename CoordinatorInput, typename CoordinatorOutput>
        class coordinator : public coordinator_node {

            static_assert(std::is_base_of<base_coordinator<CoordinatorInput, CoordinatorOutput>,
                    Coordinator<CoordinatorInput, CoordinatorOutput>>::value, 
                    "Coordinator is not derived class of base_coordinator");

            public:

                coordinator(std::string name) : coordinator_node(std::move(name)) { }

                std::string coordinator_name() const {
                    return _coordinator.name();
                }
                 
                virtual void set_index(int index) {
                    _coordinator.set_node_index(index);
                }

                virtual int index() const {
                    return _coordinator.index();
                }

                virtual void set_executor(exec::executor* processor) {
                    _coordinator.set_executor(processor);
                }

                template <typename... Args>
                    void initialize_coorindator(Args&& ...args) {
                        _coordinator.initialize(std::forward<Args>(args)...);
                    }

                virtual void process_work(const work_unit& work, base_node* parent) {

                    if(work.type_name != typeid(CoordinatorInput).name()) 
                        throw std::runtime_error(
                                std::string("Input for coordinator is not of type: ") + typeid(CoordinatorInput).name());

                    CoordinatorInput  work_data;
                    work.data >> work_data;

                    _coordinator.coordinate(work_data, parent->name());
                }

            private:
                Coordinator<CoordinatorInput, CoordinatorOutput> _coordinator;
        };


    template <template<typename...> class Reducer, typename PipeInput, typename ReducerInput, typename ReducerOutput>
        class reducer : public reducer_node {

            static_assert(std::is_base_of<base_reducer<PipeInput, ReducerInput, ReducerOutput>,
                    Reducer<PipeInput, ReducerInput, ReducerOutput>>::value, 
                    "Reducer is not derived class of base_reducer");

            public:

                reducer(std::string name, ereducer_type reducer_type) 
                    : reducer_node(std::move(name), reducer_type) 
                { }

                std::string reducer_name() const {
                    return _reducer.name();
                }

                virtual void set_index(int index) {
                    _reducer.set_node_index(index);
                }

                virtual int index() const {
                    return _reducer.index();
                }

                virtual void set_executor(exec::executor* processor) {
                    _reducer.set_executor(processor);
                }

                virtual void set_as_root(bool value) {
                    _reducer.set_as_root(value);
                }

                virtual void process_work(const work_unit& work, base_node* parent) {

                    if(work.type_name != typeid(ReducerInput).name() || work.type_name != typeid(ReducerOutput).name()) 
                        throw std::runtime_error(
                                std::string("Input for reducer is not of type: ") + typeid(ReducerInput).name());

                    switch(work.work_type) {

                        case work_unit::ework_type::REDUCER_REDUCE:
                            {
                                ReducerInput reduce_data;
                                work.data >> reduce_data;
                                _reducer.reduce(reduce_data, parent->name());
                            }
                            break;
                        case work_unit::ework_type::REDUCER_COLLECT:
                            {
                                ReducerOutput collect_data;
                                work.data >> collect_data;
                                _reducer.collect(collect_data);
                            }
                            break;
                        case work_unit::ework_type::REDUCER_END:
                            _reducer.handle_finish();
                            break;
                        default:
                            throw std::runtime_error("Wrong ework_type for reducer");
                    }
                }

                template <typename... Args>
                    void initialize_reducer(Args&& ...args) {
                        _reducer.initialize(std::forward<Args>(args)...);
                    }

            private:

                void run(const ReducerInput& input, const task_node* parent) {
                    _reducer.reduce(input, parent);
                }

                void run(const ReducerOutput& output_data) {
                    _reducer.collect(output_data);
                }

            private:
                Reducer<PipeInput, ReducerInput, ReducerOutput> _reducer;

        };

    template <template<typename...> class Outputer, typename OutputerInput>
        class outputer : public output_node {

            static_assert(std::is_base_of<base_outputer<OutputerInput>,
                    Outputer<OutputerInput>>::value, 
                    "Coordinator is not derived class of base_coordinator");

            public:

                outputer(std::string name) : output_node(std::move(name)) { }

                std::string outputer_name() const {
                    return _outputer.name();
                }
                 
                virtual void set_index(int index) {
                    _outputer.set_node_index(index);
                }

                virtual int index() const {
                    return _outputer.index();
                }

                virtual void set_executor(exec::executor* processor) {
                    _outputer.set_executor(processor);
                }

                template <typename... Args>
                    void initialize_outputer(Args&& ...args) {
                        _outputer.initialize(std::forward<Args>(args)...);
                    }

                virtual void process_work(const work_unit& work, base_node* parent) {

                    if(work.type_name != typeid(OutputerInput).name()) 
                        throw std::runtime_error(
                                std::string("Input for outputer is not of type: ") + typeid(OutputerInput).name());

                    OutputerInput  work_data;
                    work.data >> work_data;

                    _outputer()(work_data, parent->name());
                }

            private:
                Outputer<OutputerInput> _outputer;
        };
}

#endif
