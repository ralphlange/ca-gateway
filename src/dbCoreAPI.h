#ifndef INC_dbCoreAPI_H
#define INC_dbCoreAPI_H

#ifndef DBCORE_API
#  if defined(_WIN32) || defined(__CYGWIN__)
#    if defined(BUILDING_dbCore_API) && defined(EPICS_BUILD_DLL)
#      define DBCORE_API __declspec(dllexport)
#    elif !defined(BUILDING_dbCore_API) && defined(EPICS_CALL_DLL)
#      define DBCORE_API __declspec(dllimport)
#    else
#      define DBCORE_API
#    endif
#  elif __GNUC__ >= 4
#    define DBCORE_API __attribute__ ((visibility("default")))
#  else
#    define DBCORE_API
#  endif
#endif

#ifndef epicsStdCall
#  define epicsStdCall
#endif

#endif /* INC_dbCoreAPI_H */
