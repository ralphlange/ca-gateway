#ifndef INCdbNotifyh
#define INCdbNotifyh
#ifdef __cplusplus
extern "C" {
#endif
typedef enum { notifyOK, notifyError, notifyCanceled } notifyStatus;
typedef enum { putProcessRequest, putRequest } notifyRequestType;
typedef enum { pvPutNone, pvPutScalar, pvPutArray } notifyPutType;
typedef struct processNotify {
    void *usrPvt;
    struct dbChannel *chan;
    int (*putCallback)(struct processNotify *, notifyPutType);
    void (*doneCallback)(struct processNotify *);
    notifyRequestType requestType;
    notifyStatus status;
} processNotify;
void dbProcessNotify(processNotify *ppn);
void dbNotifyCancel(processNotify *ppn);
#ifdef __cplusplus
}
#endif
#endif
