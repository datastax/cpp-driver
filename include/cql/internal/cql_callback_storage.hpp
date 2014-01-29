#ifndef CQL_CALLBACK_STORAGE_HPP_
#define CQL_CALLBACK_STORAGE_HPP_

#include <cstddef>
#include <stack>
#include <boost/thread/mutex.hpp>

#include "cql/cql.hpp"
#include "cql/cql_stream.hpp"

namespace cql {

// TType - type of array items,
//	must have default constructor.
template<typename TType>
class cql_callback_storage_t
{
public:

    cql_callback_storage_t(
        size_t size) :
        _contents(size),
        _is_used(size, false),
        _free_indexes()
	{
        boost::mutex::scoped_lock lock(_mutex);
        for (size_t index = 0; index < size; index++) {
            _free_indexes.push(index);
        }
	}

	inline size_t
	size() const
    {
		return _is_used.size();
	}

	inline cql_stream_t
	acquire_stream()
    {
        boost::mutex::scoped_lock lock(_mutex);
		if (_free_indexes.empty()) {
			return cql_stream_t::invalid_stream();
        }

        size_t index = _free_indexes.top();
        _free_indexes.pop();
		_is_used[index] = true;
		return cql_stream_t::from_stream_id(index);
	}

    // Releases given slot.
    // After call to this method slot will became invalid.
	void
	release_stream(
        cql_stream_t& stream)
    {
        boost::mutex::scoped_lock lock(_mutex);
		if (stream.is_invalid()) {
			return;
        }

		long index = stream.stream_id();
        check_boundaries(index);

		_is_used[index] = false;
        _free_indexes.push(index);
		stream = cql_stream_t::invalid_stream();
	}

	// Returns true when given slot is used.
	bool
	has_callbacks(
        const cql_stream_t& stream)
    {
        boost::mutex::scoped_lock lock(_mutex);
		if (stream.is_invalid()) {
			return false;
        }

        long index = stream.stream_id();
        check_boundaries(index);

		return _is_used[index];
	}

	TType
	get_callbacks(
        const cql_stream_t& stream)
    {
        boost::mutex::scoped_lock lock(_mutex);
		if (stream.is_invalid()) {
			throw std::invalid_argument("stream is invalid.");
        }

		long index = stream.stream_id();
        check_boundaries(index);


		return _contents[index];
	}
    
	void
	set_callbacks(
        const cql_stream_t& stream,
        const TType&        value)
    {
        boost::mutex::scoped_lock lock(_mutex);
		if (stream.is_invalid()) {
			throw std::invalid_argument("index is invalid.");
        }

		long index = stream.stream_id();
        check_boundaries(index);
		_contents[index] = value;
	}

private:
    void
    check_boundaries(
        const long index) const
    {
        if (index < 0 || (unsigned) index >= _is_used.size()) {
            throw std::out_of_range("stream id is out of range.");
        }
    }

    boost::mutex       _mutex;
    std::vector<TType> _contents;
    std::vector<bool>  _is_used;
    std::stack<long>   _free_indexes;
};

}

#endif // CQL_CALLBACK_STORAGE_HPP_
