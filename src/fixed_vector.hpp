#ifndef FIXED_ALLOCATOR_HPP
#define FIXED_ALLOCATOR_HPP

#include "macros.hpp"

#include "third_party/boost/boost/aligned_storage.hpp"

#include <memory>
#include <vector>

namespace cass {

// This is an allocator that starts using a fixed size buffer that only
// uses the heap when exceeded. The allocator can be
// copied so the vector itself needs to hold on to the fixed buffer.

template <class T, size_t N>
class FixedAllocator : public std::allocator<T> {
public:
  typedef typename std::allocator<T>::pointer pointer;
  typedef typename std::allocator<T>::const_pointer const_pointer;
  typedef typename std::allocator<T>::size_type size_type;

  struct Fixed {
    Fixed()
      : is_used(false) {}

    typedef boost::aligned_storage<N * sizeof(T),
                                   boost::alignment_of<T>::value> Storage;

    bool is_used;
    Storage data;
  };

  FixedAllocator()
    : fixed_(NULL) {}

  FixedAllocator(Fixed* buffer)
    : fixed_(buffer) {}

  FixedAllocator(const FixedAllocator<T, N>& allocator)
    : std::allocator<T>()
    , fixed_(allocator.fixed_) {}

  FixedAllocator(const std::allocator<T>& allocator)
    : std::allocator<T>(allocator)
    , fixed_(NULL) {}

  pointer allocate(size_type n, const_pointer hint = 0) {
    if(fixed_ != NULL && !fixed_->is_used && n <= N) {
      fixed_->is_used = true; // Don't reuse the buffer
      return reinterpret_cast<T*>(fixed_->data.address());
    } else {
      return std::allocator<T>::allocate(n, hint);
    }
  }

  void deallocate(pointer p, size_type n) {
    if(fixed_ != NULL && fixed_->data.address() == p) {
      fixed_->is_used = false; // It's safe to reuse the buffer
    } else {
      std::allocator<T>::deallocate(p, n);
    }
  }

private:
  Fixed* fixed_;
};

// This vector uses the fixed buffer as long as it doesn't exceed N items.
// This can help to avoid heap allocation in the common cases while also
// handling the cases that do exceed the fixed buffer.

template<class T, size_t N>
class FixedVector : public std::vector<T, FixedAllocator<T, N> > {
public:
  FixedVector()
    : std::vector<T, FixedAllocator<T, N> >(FixedAllocator<T, N>(&fixed_)) {
    this->reserve(N);
  }

  FixedVector(size_t inital_size)
    : std::vector<T, FixedAllocator<T, N> >(FixedAllocator<T, N>(&fixed_)) {
    this->resize(inital_size);
  }

private:
  typename FixedAllocator<T, N>::Fixed fixed_;

private:
  DISALLOW_COPY_AND_ASSIGN(FixedVector);
};

} // namespace cass

#endif // FIXED_ALLOCATOR_HPP
