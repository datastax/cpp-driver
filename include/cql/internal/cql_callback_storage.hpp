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
// Size - size of array, should not exceed 4096
// 	for performance reasons.
template<typename TType, size_t Size>
class cql_thread_safe_array_t {
public:
	
	class slot_t {
	public:
		// Returns true when given slot is invalid.
		inline bool 
		is_invalid() const {
			return (_index == (size_t)(-1));
		}

		// Returns stream ID associated with given slot.
		inline cql::cql_stream_id_t
		stream_id() const {
			return (cql::cql_stream_id_t)_index;
		}
	private:
		inline
		index_t(size_t index)
			: _index(index) { }

		// throw std::argument_exception when
		// this instance represent index greater than
		// or equal Size;
		void 
		check_index() const {
			if(_index >= Size)
				throw std::out_of_range("index is greater than size()")
		}

		static inline index_t 
		invalid_index() {
			return index_t((size_t)(-1));
		}

		size_t _index;

		template<typename _TArg1, size_t _TArg2>
		friend class cql_thread_safe_array_t;
	};

	cql_thread_safe_array_t()
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
	inline index_t 
	allocate_slot() {
		size_t index;

		if(!_free_indexes.pop(&index))
			return index_t::invalid_index();

		_is_used[index] = true;
		return index_t(index);
	} 

	void
	release_slot(index_t& index) {
		if(index.is_invalid())
			return;

		index.check_index();

		size_t i = index._index;
		_is_used[i] = false;

		while(!_free_indexes.push(i))
			;

		index = index_t::invalid_index();
	}

	// Returns true when given slot is used.
	bool
	is_free_slot(const index_t& index) const {
		if(index.is_invalid())
			return false;

		index.check_index();

		return !_is_used[index._index];
	}

	TType 
	get(const index_t& slot) {
		if(slot.is_invalid())
			throw std::invalid_argument("index is invalid.");
		
		slot.check_index();
		size_t index = slot._index;
		// bla
		boost::lock_guard<boost::detail::spinlock> lock(_locks[index]);
		return _contents[index];
	}

	void
	put(const index_t& slot, const TType& value) {
		if(slot.is_invalid())
			throw std::invalid_argument("index is invalid.");

		slot.check_index();
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