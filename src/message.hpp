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


    struct message {

        std::string data;
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
            message& operator<<(message& mes, const T& t) {

                try {

                    std::ostringstream os;
                    boost::archive::binary_oarchive archive(os, boost::archive::no_header);
                    archive << typeinfo(t).name();
                    archive << t;
                    mes.data = os.str();

                } catch(boost::archive::archive_exception& ae) {
                    throw serialization_exception(ae.what());
                }

                return mes;
            }

        // deserialization
        template <typename T>
            T& operator>>(message& mes, const T& t) {

                try {
                    std::istringstream is(mes.data);
                    boost::archive::binary_iarchive archive(is, boost::archive::no_header);
                    std::string type_id;
                    archive >> type_id;
                    if(typeinfo(t).name() != type_id) 
                        throw serialization_exception("Expected: " + typeinfo(t).name(), "Got: " + type_id);

                } catch(boost::archive::archive_exception& ae) {
                    throw serialization_exception(ae.what());
                }

                return t;
            }

    }
}

#endif
