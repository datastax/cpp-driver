#ifndef CQL_CALLBACK_STORAGE_HPP_
#define CQL_CALLBACK_STORAGE_HPP_

#include <cstddef>
#include <boost/atomic.hpp>
#include <boost/lockfree/stack.hpp>

#include "cql/cql.hpp"
#include "cql/cql_stream.hpp"

namespace cql {
    
// TType - type of array items, 
//	must have default constructor.
// Size - size of the array, should not exceed 4096
// 	for performance reasons.
template<typename TType, size_t Size>
class cql_callback_storage_t {
public:

	cql_callback_storage_t()
		: _free_indexes(Size) 
	{
		populate_free_indexes();
	}

	inline size_t 
	size() const {
		return Size;
	}

	inline cql_stream_t 
	acquire_stream() {
		size_t index;

		if(!_free_indexes.pop(index))
			return cql_stream_t::invalid_stream();

		_is_used[index] = true;
		return cql_stream_t::from_stream_id(index);
	} 

    // Releases given slot.
    // After call to this method slot will became invalid.
	void
	release_stream(cql_stream_t& stream) {
		if(stream.is_invalid())
			return;

		long index = stream.stream_id();
        check_boundaries(index);
        
		_is_used[index] = false;
		while(!_free_indexes.push(index))
			;

		stream = cql_stream_t::invalid_stream();
	}

	// Returns true when given slot is used.
	bool
	has_callbacks(const cql_stream_t& stream) const {
		if(stream.is_invalid())
			return false;

        long index = stream.stream_id();
        check_boundaries(index);
        
		return _is_used[index];
	}

	TType 
	get_callbacks(const cql_stream_t& stream) {
		if(stream.is_invalid())
			throw std::invalid_argument("stream is invalid.");
		
		long index = stream.stream_id();
        check_boundaries(index);
		
		boost::lock_guard<boost::detail::spinlock> lock(_locks[index]);
		return _contents[index];
	}

	void
	set_callbacks(
            const cql_stream_t& stream, 
            const TType&        value) 
    {
		if(stream.is_invalid())
			throw std::invalid_argument("index is invalid.");

		long index = stream.stream_id();
        check_boundaries(index);
        
		boost::lock_guard<boost::detail::spinlock> lock(_locks[index]);
		_contents[index] = value;
	}

private:
    void
    check_boundaries(const long index) const {
        if(index < 0 || (unsigned)index >= Size)
            throw std::out_of_range("stream id is out of range.");
    }
    
	void
	populate_free_indexes() {
		for(size_t index = 0; index < Size; index++)
			_free_indexes.unsynchronized_push(index);
	}

	TType							_contents[Size];
	boost::atomic_bool				_is_used[Size];
	boost::detail::spinlock 		_locks[Size];
	boost::lockfree::stack<long> 	_free_indexes;
};

}

#endif // CQL_CALLBACK_STORAGE_HPP_