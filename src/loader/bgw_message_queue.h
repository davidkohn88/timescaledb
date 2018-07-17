#ifndef BGW_MESSAGE_QUEUE_H
#define BGW_MESSAGE_QUEUE_H

#include <postgres.h>
#include <storage/shm_mq.h>


typedef enum tsbgwMessageType {
    STOP = 0,
    START,
    RESTART
} tsbgwMessageType;

typedef struct tsbgwMessage {
    tsbgwMessageType    message_type;
    
    pid_t               sender_pid;
    dsm_handle          ack_dsm_handle;       
    size_t              offset_to_shm_mq;

} tsbgwMessage;


extern tsbgwMessage* tsbgw_message_create(tsbgwMessageType message_type);
extern bool tsbgw_message_send_and_wait(tsbgwMessage *message);

/* called only by the launcher*/
extern tsbgwMessage* tsbgw_message_receive(void);
extern void tsbgw_message_send_ack(tsbgwMessage *message, bool success);
 
/*called at server startup*/
extern void tsbgw_message_queue_alloc(void);

/*called in every backend during shmem startup hook*/ 
extern void tsbgw_message_queue_shmem_startup(void);


#endif /*BGW_MESSAGE_QUEUE_H*/