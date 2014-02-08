#include <cassert>
#include <chrono>

#include "message.hpp"

namespace dj {

    uint64_t context_info::get_current_timestamp() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
    }

    message::message(int tag, std::string data) 
        : tag(tag), data(std::move(data)) 
    { }

    message::message(message&& other) : tag(other.tag), data(std::move(other.data)) { }

    message& message::operator=(message&& other) {
        tag = other.tag;
        data = std::move(other.data);
        return *this;
    }

    message& message::operator<<(const work_unit& work) {

        std::ostringstream os;
        ::boost::archive::binary_oarchive archive(os, ::boost::archive::no_header);
        tag = static_cast<int>(work.work_type);
        archive << work.type_name;
        archive << work.data;
        archive << work.locale;
        archive << work.index_to;
        archive << work.index_from;

        data = os.str();
        return *this;
    }

    message& message::operator<<(const end_message& mes) {
        std::ostringstream os;
        ::boost::archive::binary_oarchive archive(os, ::boost::archive::no_header);
        tag = static_cast<int>(mes.end_type);
        archive << mes.from_rank;
        archive << mes.pass_number;
        archive << mes.finished_count;

        data = os.str();
        return *this;
    }

    locale_info::locale_info(uint rank, std::string hostname, uint64_t timestamp) 
        : rank(rank), hostname(std::move(hostname)), timestamp(timestamp) 
    { }

    locale_info::locale_info(locale_info&& other) 
        : rank(other.rank), hostname(std::move(other.hostname)), timestamp(other.timestamp) 
    { }

    locale_info& locale_info::operator=(locale_info&& other) {
        rank = other.rank;
        hostname = std::move(other.hostname);
        timestamp = other.timestamp;
        return *this;
    }

    locale_info locale_info::get_basic() {
        assert(_context != nullptr);
        return { _context->rank, _context->hostname, context_info::get_current_timestamp() };
    }

    const context_info* locale_info::_context = nullptr;

    work_unit::work_unit(ework_type work_type, std::string data, std::string type_name, locale_info locale)
        : work_type(work_type),
        type_name(std::move(type_name)),
        data(std::move(data)),
        locale(std::move(locale)) 
    { }

    work_unit& work_unit::operator=(work_unit&& other) {
        work_type = other.work_type;
        type_name = std::move(other.type_name);
        data = std::move(other.data);
        locale = std::move(other.locale);

        return *this;
    }

    work_unit& work_unit::operator<<(const message& mes) {

        std::istringstream is(mes.data);
        ::boost::archive::binary_iarchive archive(is, ::boost::archive::no_header);
        work_type = static_cast<work_unit::ework_type>(mes.tag);
        archive >> type_name;
        archive >> data;
        archive >> locale;
        archive >> index_to;
        archive >> index_from;

        return *this;
    }

    end_message& end_message::operator<<(const message& mes) {

        std::istringstream is(mes.data);
        ::boost::archive::binary_iarchive archive(is, ::boost::archive::no_header);
        end_type = static_cast<end_message::eend_message_type>(mes.tag);
        archive >> from_rank;
        archive >> pass_number;
        archive >> finished_count;

        return *this;
    }
}

// this class has a default constructor
BOOST_SERIALIZATION_FACTORY_0(dj::locale_info)
// specify the GUID for this class
BOOST_CLASS_EXPORT(dj::locale_info)
