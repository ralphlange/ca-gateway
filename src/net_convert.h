#ifndef INC_net_convert_H
#define INC_net_convert_H

#include "db_access.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifndef LIBCA_API
#  if defined(_WIN32) || defined(__CYGWIN__)
#    if defined(BUILDING_libca_API) && defined(EPICS_BUILD_DLL)
#      define LIBCA_API __declspec(dllexport)
#    else
#      define LIBCA_API __declspec(dllimport)
#    endif
#  else
#    define LIBCA_API
#  endif
#endif

typedef unsigned long arrayElementCount;

LIBCA_API int caNetConvert (
    unsigned type, const void *pSrc, void *pDest,
    int hton, arrayElementCount count );

#ifdef __cplusplus
}
#endif

#endif	/* ifndef INC_net_convert_H */
