#ifndef MESSAGE_HPP 
#define MESSAGE_HPP 

#include <sstream>
#include <typeinfo>
#include <boost/serialization/export.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/extended_type_info_no_rtti.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>

namespace dj {

    struct work_unit;

    /**
     * Context of te execution - contains information
     * about running tasks in pipeline, parallel tasks, coordinators and reducers
     */
    struct context_info {
        uint rank;
        uint size;
        std::string hostname;

        static uint64_t get_current_timestamp();
    };

    struct message {
        message() = default;
        message(int tag, std::string data);
        message(message&& other);
        message& operator=(message&& other);

        message& operator<<(const work_unit& work); 

        int tag;
        std::string data;
    };

    namespace exec { class executor; }

    struct locale_info {
        friend class exec::executor;

        locale_info() = default;
        locale_info(uint rank, std::string hostname, uint64_t timestamp);
        locale_info(locale_info&& other);
        locale_info(const locale_info& other) = default;
        locale_info& operator=(locale_info&& other);

        static locale_info get_basic();

        uint rank;
        std::string hostname;
        uint64_t timestamp;

        private:
            static const context_info* _context;

            friend class boost::serialization::access;
            template<class Archive> void serialize(Archive& ar, const unsigned int /* version */) {
                ar & rank;
                ar & hostname;
                ar & timestamp;
            }
    };

    struct work_unit {

        enum class ework_type {
            INPUT_WORK,
            TASK_WORK,
            REDUCER_COLLECT,
            REDUCER_REDUCE,
            COORDINATOR_COORDINATE,
            COORDINATOR_OUTPUT,
            WORK_OUTPUT
        };

        work_unit(ework_type work_type, std::string data, std::string type_name, locale_info locale);
        work_unit() = default;
        work_unit(const work_unit& other) = default;
        work_unit(work_unit&& other);
        ~work_unit() = default;

        work_unit& operator<<(const message& mes);

        work_unit& operator=(work_unit&& other);
        work_unit& operator=(const work_unit& other) = default;

        ework_type work_type;
        std::string type_name;
        std::string data;
        uint index_to;
        uint index_from;
        locale_info locale;

        template <typename T>
            static work_unit get_basic(const T& t, work_unit::ework_type work_type) {

                std::ostringstream os;
                boost::archive::binary_oarchive archive(os, boost::archive::no_header);
                archive << t;
                return work_unit(work_type, os.str(), typeid(T).name(), locale_info::get_basic());
            }
    };


    namespace serialization {

        class serialization_exception : std::exception {

            public:
                serialization_exception(std::string type1, std::string type2) 
                    : mes("Types do not match: " + std::move(type1) + std::move(type2)) { }

                serialization_exception(std::string mes) 
                    : mes(std::move(mes)) { } 

                virtual const char* what() const throw() {
                    return mes.c_str();
                }

            private:
                std::string mes;

        };

        // serialization
        template <typename T>
            std::string& operator<<(std::string& data, const T& t) {

                try {
                    std::ostringstream os;
                    boost::archive::binary_oarchive archive(os, boost::archive::no_header);
                    archive << t;
                    data = os.str();

                } catch(boost::archive::archive_exception& ae) {
                    throw serialization_exception(ae.what());
                }

                return data;
            }

        // deserialization
        template <typename T>
            T& operator>>(const std::string& data, T& t) {

                try {
                    std::istringstream is(data);
                    boost::archive::binary_iarchive archive(is, boost::archive::no_header);
                    archive >> t;

                } catch(boost::archive::archive_exception& ae) {
                    throw serialization_exception(ae.what());
                }

                return t;
            }


    }
}

#endif
