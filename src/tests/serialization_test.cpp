#define BOOST_TEST_MODULE serialization_test

#include <iostream>
#include <tuple>
#include <boost/test/unit_test.hpp>
#include "../message.hpp"

using namespace dj;

struct tuple_wrapper {

    std::tuple<int, int, char> tp;

    private:
        friend class boost::serialization::access;
        template<class Archive> void serialize(Archive& ar, const unsigned int /* version */) {
            ar & std::get<0>(tp);
            ar & std::get<1>(tp);
            ar & std::get<2>(tp);
        }
};

BOOST_AUTO_TEST_SUITE(serialization_test)

    BOOST_AUTO_TEST_CASE(end_message_test) {

        uint from_rank = 1;
        uint pass_number = 2;
        uint counter = 3;
        end_message::eend_message_type end_type = end_message::eend_message_type::WORK_END;
        end_message end_mes { from_rank, pass_number, counter, end_type };

        message mes;
        mes << end_mes;
        BOOST_CHECK_EQUAL(mes.tag, static_cast<uint>(end_type));

        end_message end_mes_d;
        end_mes_d << mes;

        BOOST_CHECK_EQUAL(end_mes_d.from_rank, from_rank);
        BOOST_CHECK_EQUAL(end_mes_d.pass_number, pass_number);
        BOOST_CHECK_EQUAL(end_mes_d.counter, counter);
        BOOST_CHECK(end_mes_d.end_type ==  end_type);
    }

    BOOST_AUTO_TEST_CASE(work_unit_test) {

        uint index_to = 1;
        uint index_from = 2;
        uint rank = 4;
        uint64_t timestamp = 50005;
        std::string data("Ala ma kota");
        std::string hostname("host");
        work_unit::ework_type work_type = work_unit::ework_type::REDUCER_WORK_OUTPUT;
        locale_info locale(rank, hostname, timestamp);

        work_unit work;
        work.index_to = index_to;
        work.index_from = index_from;
        work.work_type = work_type;
        work.data = data;
        work.locale = locale; 

        message mes;
        mes << work;

        BOOST_CHECK_EQUAL(mes.tag, static_cast<uint>(work_type));

        work_unit work_d;
        work_d << mes;
        BOOST_CHECK_EQUAL(work_d.index_to, index_to);
        BOOST_CHECK_EQUAL(work_d.index_from, index_from);
        BOOST_CHECK_EQUAL(work_d.data, data);
        BOOST_CHECK(work_d.work_type ==  work_type);
        BOOST_CHECK_EQUAL(work_d.locale.rank, rank);
        BOOST_CHECK_EQUAL(work_d.locale.timestamp, timestamp);
        BOOST_CHECK_EQUAL(work_d.locale.hostname, hostname);
    }

    BOOST_AUTO_TEST_CASE(custom_type_serialization) {

        tuple_wrapper tw;
        auto t = std::make_tuple(1, 2, 'c');
        tw.tp = t;

        std::string serialized;
        using serialization::operator<<;
        using serialization::operator>>;
        serialized << tw;
        tuple_wrapper tw_d;
        serialized >> tw_d;
        BOOST_CHECK(std::get<0>(t) == std::get<0>(tw_d.tp));
        BOOST_CHECK(std::get<1>(t) == std::get<1>(tw_d.tp));
        BOOST_CHECK(std::get<2>(t) == std::get<2>(tw_d.tp));
    }

BOOST_AUTO_TEST_SUITE_END ( )

// this class has a default constructor
BOOST_SERIALIZATION_FACTORY_0(tuple_wrapper)
// specify the GUID for this class
BOOST_CLASS_EXPORT(tuple_wrapper)
