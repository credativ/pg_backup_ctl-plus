#ifndef __HAVE_POSTGRES_INCLUDES_
#define __HAVE_POSTGRES_INCLUDES_

extern "C" {
#include <postgres_fe.h>
#include "access/xlogdefs.h"
}

/**
 * likely() and unlikely() are already defined
 * in the PostgreSQL headers, colliding with the symbols
 * provides by boost/c++ 17. Undefine them explicitely, to
 * avoid compile problems.
 */

#ifdef likely
#undef likely
#endif

#ifdef unlikely
#undef unlikely
#endif

/*
 * PostgreSQL >= 12 comes with an overriden, own implementation
 * of strerror() and friends, which clashes in the definitions of
 * boost::interprocess::fill_system_message( int system_error, std::string &str)
 *
 * See /usr/include/boost/interprocess/errors.hpp
 * for details (path to errors.hpp may vary).
 *
 * Since boost does here all things on it's own (e.g. encapsulate Windows
 * error message behavior), we revoke all that definitions.
 */

#ifdef strerror
#undef strerror
#endif
#ifdef strerror_r
#undef strerror_r
#endif
#ifdef vsnprintf
#undef vsnprintf
#endif
#ifdef snprintf
#undef snprintf
#endif
#ifdef sprintf
#undef sprintf
#endif
#ifdef vfprintf
#undef vfprintf
#endif
#ifdef fprintf
#undef fprintf
#endif
#ifdef printf
#undef printf
#endif

#endif
