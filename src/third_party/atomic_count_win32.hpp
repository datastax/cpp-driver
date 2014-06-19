#ifndef __CASS_THIRD_PARTY_ATOMIC_COUNT_WIN32_HPP_INCLUDED__
#define __CASS_THIRD_PARTY_ATOMIC_COUNT_WIN32_HPP_INCLUDED__

// This is from boost/shared_ptr/detail
//
//  Copyright (c) 2001-2005 Peter Dimov
//
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
//

#include <windows.h>

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
        return InterlockedIncrement( &value_ );
    }

    long operator--()
    {
        return InterlockedDecrement( &value_ );
    }

    operator long() const
    {
        return static_cast<long const volatile &>( value_ );
    }

private:

    atomic_count( atomic_count const & );
    atomic_count & operator=( atomic_count const & );

    long value_;
};

} } // namespace cass::third_party

#endif
