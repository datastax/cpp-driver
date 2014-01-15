#ifndef CQL_CCM_LOGGER_H_
#define CQL_CCM_LOGGER_H_

#ifdef CQL_NO_BOOST_LOG
    enum _cql_loglevel {
        info,
        warning,
        error
    };

    // ACHTUNG: this is obviously not thread safe.
    // Whenever possible, Boost.Log should be used.
    #define CQL_LOG(x) (x==info ? cout : cerr)
#else
    #include <boost/log/trivial.hpp>
    #define CQL_LOG(x) BOOST_LOG_TRIVIAL(x)
#endif

#endif // CQL_CCM_LOGGER_H_
