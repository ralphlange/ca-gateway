#ifndef INCdbAsLibh
#define INCdbAsLibh
#include "dbChannel.h"
#ifdef __cplusplus
extern "C" {
#endif
int asDbGetAsl(struct dbChannel *chan);
void * asDbGetMemberPvt(struct dbChannel *chan);
#ifdef __cplusplus
}
#endif
#endif
