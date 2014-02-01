#define BOOST_TEST_MODULE task_test

#include <iostream>
#include <boost/test/unit_test.hpp>
#include "../task.hpp"
#include "../node.hpp"
#include "../message.hpp"

using namespace dj;

std::string last_string_input;
int last_int_input;

template <typename... Output>
    class simple_task : public base_task<Output...> {

        public:
            simple_task() : base_task<Output...>("simple_task") { }

            void operator()(const std::string& input) {
                last_string_input = input;
            }

            void operator()(int input) {
                last_int_input = input;
            }
    }; 

void operator>>(std::string input, std::string& output) {
    output = input;
}

void operator>>(std::string input, int& output) {
    output = std::stoi(input);
}


simple_task<std::string, int> task_instance;

BOOST_AUTO_TEST_SUITE(task_test)

    BOOST_AUTO_TEST_CASE(check_name) {

        BOOST_CHECK_EQUAL("simple_task", task_instance.name());
    }

    BOOST_AUTO_TEST_CASE(in_node_test) {

        task<simple_task<int, std::string>, std::string, int> t("simple_task_node");
        work_unit work;
        std::string string_input = "blabla";
        int int_input = 9;

        work.data = string_input;
        work.type_name = typeid(std::string).name();
        t.process_work(work);

        work.data = std::to_string(int_input);
        work.type_name = typeid(int).name();
        t.process_work(work);

        BOOST_CHECK_EQUAL(int_input, last_int_input);
        BOOST_CHECK_EQUAL(string_input, last_string_input);
    }
    
BOOST_AUTO_TEST_SUITE_END ( )

