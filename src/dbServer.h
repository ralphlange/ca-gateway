#ifndef INC_dbServer_H
#define INC_dbServer_H

#include <stddef.h>
#include "ellLib.h"
#include "dbCoreAPI.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct dbServer {
    ELLNODE node;
    const char *name;
    void (* report) (unsigned level);
    void (* stats) (unsigned *channels, unsigned *clients);
    int (* client) (char *pBuf, size_t bufSize);
    void (* init) (void);
    void (* run) (void);
    void (* pause) (void);
    void (* stop) (void);
} dbServer;

DBCORE_API int dbRegisterServer(dbServer *psrv);
DBCORE_API int dbUnregisterServer(dbServer *psrv);

#ifdef __cplusplus
}
#endif

#endif /* INC_dbServer_H */
