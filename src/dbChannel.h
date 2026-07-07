#ifndef INC_dbChannel_H
#define INC_dbChannel_H
#include "dbDefs.h"
#include "dbAddr.h"
#include "ellLib.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct dbChannel {
    const char *name;
    dbAddr addr;
    long  final_no_elements;
    short final_field_size;
    short final_type;
} dbChannel;
#define dbChannelName(pChan) ((pChan)->name)
#define dbChannelRecord(pChan) ((pChan)->addr.precord)
#define dbChannelElements(pChan) ((pChan)->addr.no_elements)
#define dbChannelFieldType(pChan) ((pChan)->addr.field_type)
#define dbChannelExportCAType(pChan) (dbDBRnewToDBRold[(pChan)->addr.dbr_field_type])
#define dbChannelFinalElements(pChan) ((pChan)->final_no_elements)
#define dbChannelFinalCAType(pChan) (dbDBRnewToDBRold[(pChan)->final_type])
#define dbChannelSpecial(pChan) ((pChan)->addr.special)
extern unsigned short dbDBRnewToDBRold[];
void dbChannelDelete(struct dbChannel *chan);
void dbChannelShow(struct dbChannel *chan, int level, int indent);
long dbChannelTest(const char *name);
#ifdef __cplusplus
}
#endif
#endif
