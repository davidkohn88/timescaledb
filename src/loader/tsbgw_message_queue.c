#include <postgres.h>

#include <miscadmin.h>
#include <storage/lwlock.h>
#include <storage/shmem.h>
#include <storage/proc.h>
#include <storage/procarray.h>
#include <storage/shm_mq.h>

#include "tsbgw_message_queue.h"

#define TSBGW_MAX_MESSAGES 16
#define TSBGW_MESSAGE_QUEUE_NAME "timescale_bgw_message_queue"
#define TSBGW_MQ_TRANCHE_NAME "timescale_bgw_mq_tranche"

#define TSBGW_ACK_QUEUE_SIZE (MAXALIGN(shm_mq_minimum_size + sizeof(int)))
typedef struct TsbgwMessageQueue
{
	pid_t		reader_pid;		/* should only be set once at cluster launcher
								 * startup */
	LWLock	   *lock;			/* pointer to the lock to control
								 * adding/removing elements from queue */
	uint			read_upto;
	uint			num_elements;
	TsbgwMessage buffer[TSBGW_MAX_MESSAGES];
}			TsbgwMessageQueue;

typedef enum QueueResponseType
{
	MESSAGE_SENT = 0,
	QUEUE_FULL,
	READER_DETACHED
}			QueueResponseType;

static TsbgwMessageQueue * tsbgw_mq = NULL;

static void queue_init(bool reinit)
{
	bool		found;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	tsbgw_mq = ShmemInitStruct(TSBGW_MESSAGE_QUEUE_NAME, sizeof(TsbgwMessageQueue), &found);
	if (!found || reinit)
	{
		memset(tsbgw_mq, 0, sizeof(TsbgwMessageQueue));
		tsbgw_mq->reader_pid = InvalidPid;
		tsbgw_mq->lock = &(GetNamedLWLockTranche(TSBGW_MQ_TRANCHE_NAME))->lock;
	}
	LWLockRelease(AddinShmemInitLock);

}


/* this gets called when shared memory is initialized in a backend (shmem_startup_hook) */
extern void
tsbgw_message_queue_shmem_startup(void)
{
	queue_init(false);
}

/* this is called in the loader during server startup to allocate a shared memory segment*/
extern void
tsbgw_message_queue_alloc(void)
{
	RequestAddinShmemSpace(sizeof(TsbgwMessageQueue));
	RequestNamedLWLockTranche(TSBGW_MQ_TRANCHE_NAME, 1);
}


/* Notes on managing the queue/locking:
 * We decided that for this application, simplicity of locking scheme was more important than
 * being very good about concurrency as the frequency of these messages will be low and the
 * number of messages on this queue should be low, given that they mostly happen when we update
 * the extension. Therefore we decided to simply take an exclusive lock whenever we were modifying
 * anything in the shared memory segment to avoid collisions.
 */

/* Add a message to the queue - we can do this if the queue is not full */
static QueueResponseType queue_add(TsbgwMessageQueue * queue, TsbgwMessage * message)
{
	QueueResponseType		message_sent = QUEUE_FULL;
	pid_t		reader_pid = InvalidPid;

	LWLockAcquire(queue->lock, LW_EXCLUSIVE);
	if (queue->num_elements < TSBGW_MAX_MESSAGES)
	{
		memcpy(&queue->buffer[(queue->read_upto + queue->num_elements) % TSBGW_MAX_MESSAGES], message, sizeof(TsbgwMessage));
		queue->num_elements++;
		message_sent = MESSAGE_SENT;
		reader_pid = queue->reader_pid;
	}
	LWLockRelease(queue->lock);

	if (reader_pid != InvalidPid)
		SetLatch(&BackendPidGetProc(reader_pid)->procLatch);
	else
		message_sent = READER_DETACHED;
	return message_sent;
}

static void queue_set_reader(TsbgwMessageQueue * queue){

	LWLockAcquire(queue->lock, LW_EXCLUSIVE);
	if (queue->reader_pid == InvalidPid)
		queue->reader_pid = MyProcPid;
	else if (queue->reader_pid != MyProcPid)
		ereport(ERROR, (errmsg("only one reader for allowed for TimescaleBGW message queue")));
	LWLockRelease(queue->lock);

}


static TsbgwMessage * queue_remove(TsbgwMessageQueue * queue)
{
	TsbgwMessage *message = NULL;

	LWLockAcquire(queue->lock, LW_EXCLUSIVE);
	if (queue->reader_pid != MyProcPid)
		ereport(ERROR, (errmsg("cannot read if not reader for TimescaleBGW message queue")));

	if (queue->num_elements > 0)
	{
		message = palloc(sizeof(TsbgwMessage));
		memcpy(message, &queue->buffer[queue->read_upto], sizeof(TsbgwMessage));
		queue->read_upto = (queue->read_upto + 1) % TSBGW_MAX_MESSAGES;
		queue->num_elements--;
	}
	LWLockRelease(queue->lock);
	return message;
}

/*construct a message*/
static TsbgwMessage * tsbgw_message_create(TsbgwMessageType message_type, Oid db_oid)
{
	TsbgwMessage *message = palloc(sizeof(TsbgwMessage));
	dsm_segment *seg;

	seg = dsm_create(TSBGW_ACK_QUEUE_SIZE, 0);

	*message = (TsbgwMessage)
	{
		.message_type = message_type,
			.sender_pid = MyProcPid,
			.db_oid = db_oid,
			.ack_dsm_handle = dsm_segment_handle(seg)
	};

	return message;
}

/*
 * write element to queue, wait/error if queue is full
 * consumes message and deallocates
 */
extern bool
tsbgw_message_send_and_wait(TsbgwMessageType message_type, Oid db_oid)
{
	QueueResponseType		send_result;
	shm_mq	   *ack_queue;
	dsm_segment *seg;
	shm_mq_handle *ack_queue_handle;
	Size		bytes_received = 0;
	bool	   *data = NULL;
	TsbgwMessage *message;
	bool		message_sent=false;

	message = tsbgw_message_create(message_type, db_oid);

	seg = dsm_find_mapping(message->ack_dsm_handle);
	ack_queue = shm_mq_create(dsm_segment_address(seg), TSBGW_ACK_QUEUE_SIZE);
	shm_mq_set_receiver(ack_queue, MyProc);
	ack_queue_handle = shm_mq_attach(ack_queue, seg, NULL);


	send_result = queue_add(tsbgw_mq, message);
	if (send_result == MESSAGE_SENT)
	{
		shm_mq_wait_for_attach(ack_queue_handle);
		shm_mq_receive(ack_queue_handle, &bytes_received, (void **) &data, false);
		message_sent = (bytes_received != 0) && *data;
	}
	dsm_detach(seg);			/* queue detach happens in dsm detach callback */
	pfree(message);
	return message_sent;
}

/*
 * called only by the launcher
 */
extern TsbgwMessage * tsbgw_message_receive(void)
{
	return queue_remove(tsbgw_mq);
}

extern void
tsbgw_message_queue_set_reader(void)
{
	queue_set_reader(tsbgw_mq);
}
/*
 * called by launcher once it has taken action based on the contents of the message
 * consumes message and deallocates
 */
extern void
tsbgw_message_send_ack(TsbgwMessage * message, bool success)
{
	shm_mq	   *ack_queue;
	dsm_segment *seg;
	shm_mq_handle *ack_queue_handle;

	seg = dsm_attach(message->ack_dsm_handle);
	ack_queue = dsm_segment_address(seg);
	shm_mq_set_sender(ack_queue, MyProc);
	ack_queue_handle = shm_mq_attach(ack_queue, seg, NULL);
	shm_mq_send(ack_queue_handle, sizeof(bool), &success, false);
	dsm_detach(seg);
	pfree(message);
}
/* this gets called before shmem exit in the launcher (even if we're exiting in error, but not if we're exiting due to possible shmem corruption)*/
static void queue_shmem_cleanup(TsbgwMessageQueue *queue){
	/* if anyone's waiting on an ack, we need to send them an ack with false, we won't actually process the message
	 * action here, but we do need to tell them not to wait so they don't end up hanging after we quit*/
	LWLockAcquire(queue->lock, LW_EXCLUSIVE);
	if (queue->reader_pid != MyProcPid)
		ereport(ERROR, (errmsg("cannot read if not reader for TimescaleBGW message queue")));
	while (queue->num_elements > 0)
	{
		TsbgwMessage *message;
		message = palloc(sizeof(TsbgwMessage));
		memcpy(message, &queue->buffer[queue->read_upto], sizeof(TsbgwMessage));
		queue->read_upto = (queue->read_upto + 1) % TSBGW_MAX_MESSAGES;
		queue->num_elements--;
		tsbgw_message_send_ack(message, false);
	}
	LWLockRelease(queue->lock);
	/* now reinitialize the queue*/
	queue_init(true);
}

extern void tsbgw_message_queue_shmem_cleanup(void){
	queue_shmem_cleanup(tsbgw_mq);
}
