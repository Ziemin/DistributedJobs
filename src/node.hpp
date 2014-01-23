#ifndef NODE_HPP
#define NODE_HPP

#include <functional>
#include <vector>

#include "template_utils.hpp"
#include "task.hpp"

namespace dj {

    enum class enode_type {
        TASK, 
        REDUCER, 
        INPUT, 
        OUTPUT,
        COORDINATOR
    };

    class base_node {
        public:
            base_node(enode_type nt);

            virtual std::string name() const = 0;
            virtual ~base_node() = default;
            bool operator==(const base_node& other) const;
            bool operator!=(const base_node& other) const;
            
            const enode_type type;
    };


    class reducer_node;
    class coordinator_node;
    class input_node;

    class task_node : public base_node {

        public:
            task_node();
            virtual ~task_node() = default;

            bool add_parent(base_node* parent);
            bool add_child(task_node* child);
            bool add_reducer(reducer_node* reducer);
            bool add_coordinator(coordinator_node* coordinator);

            bool has_reducer() const;
            bool has_coordinator() const;
            const std::vector<base_node*>& parents() const;
            const std::vector<task_node*>& children() const;
            const reducer_node* reducer() const;
            const coordinator_node* coordinator() const;

        private:
            std::vector<base_node*> _parents;
            std::vector<task_node*> _children_tasks;
            reducer_node* _reducer;
            coordinator_node* _coordinator;

    };

    class reducer_node : public base_node {

        public:
            reducer_node(output_node* out);
            virtual ~reducer_node() = default;

            bool add_parent(task_node* parent);
            const std::vector<task_node*>& parents() const;

        private:
            std::vector<task_node*> _parents;

    };


    class input_node : public base_node {

        public:
            input_node(task_node* first_task);
            virtual ~input_node() = default;

            const task_node* task() const;

        private:
            task_node* _task;
    };

    class output_node : public base_node {

        public:
            output_node(base_node* provider);
            virtual ~output_node() = default;

            const base_node* provider() const;

        private:
            base_node* _provider;
    };

    class pipeline : public task_node {

    };

    class coordinator_node : public pipeline {

        public:
            coordinator_node();
            virtual ~coordinator_node() = default;
    };

    template <typename, typename...>
        class node { };

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
        class node<Task<OutputParameters...>, InputParameters...> : public task_node {

            //static_assert(std::is_base_of<base_task<OutputParameters...>, Task<OutputParameters...>>);

            public:

                virtual std::string name() const {
                    return _task.name();
                }
                /**
                 * Run task on provided argument.
                 * Argument has to be one of the types from InputParameters list.
                 */
                template <typename Arg>
                    void run(const Arg& arg, const base_node* parent) {
                        static_assert(is_any_same<Arg, InputParameters...>{}, "cannot provide this argument to task");
                        _task(arg, parent);
                    }

                /**
                 * Initializes task
                 */
                template <typename... Args>
                    void initialize_task(Args&& ...args) {
                        _task.initialize(std::forward<Args>(args)...);
                    }

            private:
                // runnable task
                Task<OutputParameters...> _task;

        };


    template <typename Reducer, typename ReducerInput, typename PipeOutput>
        class node<Reducer, ReducerInput, PipeOutput> : public reducer_node {

            public:

                virtual std::string name() const {
                    return _reducer.name();
                }

                void run(const ReducerInput& input, const task_node* parent) {
                    _reducer.reduce(input, parent);
                }

                void run(const PipeOutput& output_data) {
                    _reducer.collect(output_data);
                }

                template <typename... Args>
                    void initialize_reducer(Args&& ...args) {
                        _reducer.initialize(std::forward<Args>(args)...);
                    }

            private:
                Reducer _reducer;

        };

}

#endif
