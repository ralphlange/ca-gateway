#ifndef INC_net_convert_H
#define INC_net_convert_H
#include "gate_compat.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef unsigned long arrayElementCount;
int caNetConvert (unsigned type, const void *pSrc, void *pDest, int hton, arrayElementCount count );
#ifdef __cplusplus
}
#endif
#endif
