#ifndef __CASS_THIRD_PARTY_ATOMIC_COUNT_GCC_HPP_INCLUDED__
#define __CASS_THIRD_PARTY_ATOMIC_COUNT_GCC_HPP_INCLUDED__

// This is from boost/shared_ptr/detail
//
//  Copyright (c) 2001, 2002 Peter Dimov and Multi Media Ltd.
//  Copyright (c) 2002 Lars Gullik Bjønnes <larsbj@lyx.org>
//  Copyright 2003-2005 Peter Dimov
//
//  Distributed under the Boost Software License, Version 1.0. (See
//  accompanying file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//

#if __GNUC__ * 100 + __GNUC_MINOR__ >= 402
# include <ext/atomicity.h> 
#else 
# include <bits/atomicity.h>
#endif

namespace cass {
namespace third_party {

class atomic_count
{
public:

    explicit atomic_count( long v ) : value_( v ) {}

    long operator++()
    {
        return __exchange_and_add( &value_, +1 ) + 1;
    }

    long operator--()
    {
        return __exchange_and_add( &value_, -1 ) - 1;
    }

    operator long() const
    {
        return __exchange_and_add( &value_, 0 );
    }

private:

    atomic_count(atomic_count const &);
    atomic_count & operator=(atomic_count const &);

    mutable _Atomic_word value_;
};

} } // namespace cass::third_party

#endif
