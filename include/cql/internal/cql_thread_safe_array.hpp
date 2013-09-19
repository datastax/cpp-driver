#ifndef CQL_THREAD_SAFE_ARRAY_HPP_
#define CQL_THREAD_SAFE_ARRAY_HPP_

#include <cstddef>
#include <boost/atomic.hpp>
#include <boost/lockfree/stack.hpp>

// std::lock_guard<boost::detail::spinlock>
namespace cql {

// TType - type of array items, 
//	must have default constructor.
// Size - size of array, should not exceed 4096
// 	for performance reasons.
template<typename TType, size_t Size>
class cql_thread_safe_array_t
public:
	
	class index_t {
	public:
		// Returns true when given index is invalid.
		inline bool 
		is_invalid() const {
			return (_index == (size_t)(-1));
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
		initialize_allocated_markers();
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
	is_used(const index_t& index) const {
		if(index.is_invalid())
			return false;

		index.check_index();

		return _is_used[index._index];
	}

	TType 
	operator [](const index_t& index) {
		if(index.is_invalid())
			throw std::invalid_argument("index is invalid.");
		
		index.check_index();
		size_t i = index._index;

		boost::lock_guard<boost::detail::spinlock> lock(_locks[i]);
		return _contents[i];
	}

	void
	operator [](const index_t& index, const TType& value) {
		if(index.is_invalid())
			throw std::invalid_argument("index is invalid.");

		index.check_index();
		size_t i = index._index;

		boost::lock_guard<boost::detail::spinlock> lock(_locks[i]);
		_contents[i] = value;
	}

private:
	void
	populate_free_indexes() {
		for(size_t index = 0; index < Size; index++)
			_free_indexes.unsynchronized_push(index);
	}

	void
	initialize_allocated_markers() {
		memset(_allocated_marker, 0, sizeof(_allocated_marker));
	}

	TType							_contents[Size];
	boost::atomic_bool				_is_used[Size];
	boost::spinlock 				_locks[Size];
	boost::lockfree::stack<size_t> 	_free_indexes;
}

#endif // CQL_THREAD_SAFE_ARRAY_HPP_