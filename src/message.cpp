#include "message.hpp"

namespace dj {

        message& operator<<(message& mes, work_unit& work) {

            std::ostringstream os;
            boost::archive::binary_oarchive archive(os, boost::archive::no_header);
            archive << work.work_type;
            archive << work.type_name;
            archive << work.data;

            mes.data = os.str();

            return mes;
        }

        message& operator>>(message& mes, work_unit& work) {

            std::istringstream is(mes.data);
            boost::archive::binary_iarchive archive(is, boost::archive::no_header);
            archive >> work.work_type;
            archive >> work.type_name;
            archive >> work.data;

            return mes;
        }

}
