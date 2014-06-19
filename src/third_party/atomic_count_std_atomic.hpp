#ifndef __CASS_THIRD_PARTY_ATOMIC_COUNT_STD_ATOMIC_HPP_INCLUDED__
#define __CASS_THIRD_PARTY_ATOMIC_COUNT_STD_ATOMIC_HPP_INCLUDED__

// This is from boost/shared_ptr/detail
//
//  Copyright 2013 Peter Dimov
//
//  Distributed under the Boost Software License, Version 1.0.
//  See accompanying file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt
//

#include <atomic>
#include <cstdint>

namespace cass {
namespace third_party {

class atomic_count
{
public:

    explicit atomic_count( long v ): value_( v )
    {
    }

    long operator++()
    {
        return value_.fetch_add( 1, std::memory_order_acq_rel ) + 1;
    }

    long operator--()
    {
        return value_.fetch_sub( 1, std::memory_order_acq_rel ) - 1;
    }

    operator long() const
    {
        return value_.load( std::memory_order_acquire );
    }

private:

    atomic_count(atomic_count const &);
    atomic_count & operator=(atomic_count const &);

    std::atomic_int_least32_t value_;
};

} } // namespace cass::third_party

#endif
