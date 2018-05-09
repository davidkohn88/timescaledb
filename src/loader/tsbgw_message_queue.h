#ifndef BGW_MESSAGE_QUEUE_H
#define BGW_MESSAGE_QUEUE_H

#include <postgres.h>
#include <storage/dsm.h>


typedef enum TsbgwMessageType
{
	STOP = 0,
	START,
	RESTART
}			TsbgwMessageType;

typedef struct TsbgwMessage
{
	TsbgwMessageType message_type;

	pid_t		sender_pid;
	Oid			db_oid;
	dsm_handle	ack_dsm_handle;


}			TsbgwMessage;

extern bool tsbgw_message_send_and_wait(TsbgwMessageType message, Oid db_oid);

/* called only by the launcher*/
extern void tsbgw_message_queue_set_reader(void);
extern TsbgwMessage * tsbgw_message_receive(void);
extern void tsbgw_message_send_ack(TsbgwMessage * message, bool success);

/*called at server startup*/
extern void tsbgw_message_queue_alloc(void);

/*called in every backend during shmem startup hook*/
extern void tsbgw_message_queue_shmem_startup(void);
extern void tsbgw_message_queue_shmem_cleanup(void);


#endif							/* BGW_MESSAGE_QUEUE_H */
