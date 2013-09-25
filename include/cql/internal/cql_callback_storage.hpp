#ifndef CQL_CALLBACK_STORAGE_HPP_
#define CQL_CALLBACK_STORAGE_HPP_

#include <cstddef>
#include <boost/atomic.hpp>
#include <boost/lockfree/stack.hpp>

#include "cql/cql.hpp"

// std::lock_guard<boost::detail::spinlock>
namespace cql {

// TType - type of array items, 
//	must have default constructor.
// Size - size of the array, should not exceed 4096
// 	for performance reasons.
// 
// Currently array indexes maps 1:1 with cassandra
// stream IDs.
template<typename TType, size_t Size>
class cql_callback_storage_t {
public:
	
    // Represents array index
	class slot_t {
	public:
		// Returns true when given slot is invalid.
        // Only valid slots can be used with array operations
        // such as get().
		inline bool 
		is_invalid() const {
			return (_index == (size_t)(-1));
		}

		// Returns Cassandra stream ID associated with given slot.
		inline cql::cql_stream_id_t
		stream_id() const {
			return (cql::cql_stream_id_t)_index;
		}
        
        static inline slot_t
        slot_for(cql::cql_stream_id_t stream_id) {
            if ( (stream_id < 0) || stream_id >= (long)Size )
                return invalid_slot();
            
            return slot_t((size_t)stream_id);
        }
	private:
		inline
		slot_t(size_t index)
			: _index(index) { }

		// throw std::argument_exception when
		// this instance represent index greater than
		// or equal Size;
		inline void 
		check_index_boundaries() const {
			if(_index >= Size)
				throw std::out_of_range("index is greater than array size.");
		}

		static inline slot_t 
		invalid_slot() {
			return slot_t((size_t)(-1));
		}

		size_t _index;

		friend class cql_callback_storage_t<TType, Size>;
	};

	cql_callback_storage_t()
		: _free_indexes(Size) 
	{
		populate_free_indexes();
	}

	// Returns size of array.
	inline size_t 
	size() const {
		return Size;
	}

	// Attempts to allocate free array slot.
	// Returns (size_t)-1 on error, otherwise returns
	// index of newly allocated slot.
	inline slot_t 
	acquire_slot() {
		size_t index;

		if(!_free_indexes.pop(index))
			return slot_t::invalid_slot();

		_is_used[index] = true;
		return slot_t(index);
	} 

    // Releases given slot.
    // After call to this method slot will became invalid.
	void
	release_slot(slot_t& slot) {
		if(slot.is_invalid())
			return;

		slot.check_index_boundaries();

		size_t index = slot._index;
		_is_used[index] = false;

		while(!_free_indexes.push(index))
			;

		slot = slot_t::invalid_slot();
	}

	// Returns true when given slot is used.
	bool
	is_free_slot(const slot_t& slot) const {
		if(slot.is_invalid())
			return false;

		slot.check_index_boundaries();
		return !_is_used[slot._index];
	}

	TType 
	get_at(const slot_t& slot) {
		if(slot.is_invalid())
			throw std::invalid_argument("index is invalid.");
		
		slot.check_index_boundaries();
		size_t index = slot._index;
		
		boost::lock_guard<boost::detail::spinlock> lock(_locks[index]);
		return _contents[index];
	}

	void
	put_at(const slot_t& slot, const TType& value) {
		if(slot.is_invalid())
			throw std::invalid_argument("index is invalid.");

		slot.check_index_boundaries();
		size_t index = slot._index;

		boost::lock_guard<boost::detail::spinlock> lock(_locks[index]);
		_contents[index] = value;
	}

private:
	void
	populate_free_indexes() {
		for(size_t index = 0; index < Size; index++)
			_free_indexes.unsynchronized_push(index);
	}

	TType							_contents[Size];
	boost::atomic_bool				_is_used[Size];
	boost::detail::spinlock 		_locks[Size];
	boost::lockfree::stack<size_t> 	_free_indexes;
};

}

#endif // CQL_CALLBACK_STORAGE_HPP_