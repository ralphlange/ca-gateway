/*
 * Compatibility shim for vendored rsrv.h's `#include "dbCoreAPI.h"` (a quoted include, so
 * this file -- living alongside rsrv.h -- is found ahead of anything on the compiler's
 * include path). Base 7.0+ generates a real dbCoreAPI.h (defining DBCORE_API, used to mark
 * dbCore's exported symbols) as part of its modular per-library build; Base 3.15 predates that
 * convention entirely and has no such header, which otherwise fails the build with
 * "dbCoreAPI.h: No such file or directory" (rsrv.h isn't version-guarded here, since upstream
 * itself never needs to be -- every real Base checkout its version of rsrv.h came from always
 * has a matching dbCoreAPI.h alongside it).
 *
 * This mirrors Base 7.0's own generated dbCoreAPI.h content exactly (see
 * modules/libcom/src/O.Common/dbCoreAPI.h in a Base 7.0 checkout), so it's a no-op --
 * identical macro definitions -- when building against 7.0, and supplies the same macros
 * (functionally irrelevant here anyway, since everything is compiled directly into one
 * executable, never a separate dbCore shared library) when building against 3.15.
 */
#ifndef INC_dbCoreAPI_H
#define INC_dbCoreAPI_H

#if defined(_WIN32) || defined(__CYGWIN__)

#  if !defined(epicsStdCall)
#    define epicsStdCall __stdcall
#  endif

#  if defined(BUILDING_dbCore_API) && defined(EPICS_BUILD_DLL)
/* Building library as dll */
#    define DBCORE_API __declspec(dllexport)
#  elif !defined(BUILDING_dbCore_API) && defined(EPICS_CALL_DLL)
/* Calling library in dll form */
#    define DBCORE_API __declspec(dllimport)
#  endif

#elif __GNUC__ >= 4
#  define DBCORE_API __attribute__ ((visibility("default")))
#endif

#if !defined(DBCORE_API)
#  define DBCORE_API
#endif

#if !defined(epicsStdCall)
#  define epicsStdCall
#endif

#endif /* INC_dbCoreAPI_H */
