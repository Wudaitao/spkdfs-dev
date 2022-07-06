#include <dfs_dbg.h>
#include "dfs_types.h"
#include "dfs_memory.h"
#include "cfs.h"
#include "faio_manager.h"
#include "dn_conf.h"
#include "cfs_faio.h"

extern faio_manager_t *faio_mgr;

// setup cfs meta \sp \ faio
int cfs_setup(pool_t *pool, cfs_t *cfs, log_t *log)
{
	cfs->meta = (fs_meta_t *)pool_alloc(pool, sizeof(fs_meta_t));
    cfs->sp = (swap_opt_t *)pool_alloc(pool, sizeof(swap_opt_t));
	
    if (!cfs->meta || !cfs->sp) 
	{
        return NGX_ERROR;
    }

	cfs->cursize = nullptr;
    cfs->state = 0;
	// init parse func \ done func \ faio
    cfs_faio_setup(cfs->meta);

    cfs->meta->parsefunc(cfs->sp, cfs->meta);

    return NGX_OK;
}

int cfs_sendfile(cfs_t *cfs, int fd_out, int fd_in, 
	off_t* offset, size_t len, log_t *log)
{
    int rc = NGX_ERROR;
	
    if (!cfs || !offset) 
	{
        return NGX_ERROR;
    }

    rc = cfs->sp->io_opt.sendfile(fd_out, fd_in, offset, len, log);
    if (rc == NGX_ERROR)
	{
        return rc;
    } 
	else if (rc > 0) 
	{
        return rc;
    }
	
    return rc;
}

int cfs_sendfile_chain(cfs_t *cfs, file_io_t *fio, log_t *log)
{
    int rc = NGX_ERROR;
	
    if (!cfs || !fio) 
	{
        return NGX_ERROR;
    }

    rc = cfs->sp->io_opt.sendfilechain(fio, log); //cfs_faio_sendfile
    if (rc == NGX_ERROR)
	{
        return NGX_ERROR;
    } 
	else 
	{
        return NGX_OK;
    }
}

//
int cfs_write(cfs_t *cfs, file_io_t *fio, log_t *log)
{
    int rc = NGX_ERROR;
    if (!cfs || !fio) 
	{
        return NGX_ERROR;
    }

    // push fio data task to data queue
    rc = cfs->sp->io_opt.write(fio, log);//cfs_faio_write
    if (rc == NGX_ERROR)
	{
        return NGX_ERROR;
    } 
	else 
	{
        return NGX_OK;
    }
}

// 调用 cfs_faio_read
int cfs_read(cfs_t *cfs, file_io_t *fio, log_t *log)
{
    int rc = NGX_ERROR;
    if (!cfs || !fio) 
	{
        return NGX_ERROR;
    }

    rc = cfs->sp->io_opt.read(fio, log); // cfs_faio_read
    if (rc == NGX_ERROR)
	{
        return NGX_ERROR;
    } 
	else 
	{
        return NGX_OK;
    }
}

void cfs_close(cfs_t *cfs, int fd)
{
    cfs->sp->io_opt.close(fd); // close fd
}

// 调用  cfs_faio_open
int cfs_open(cfs_t *cfs, uchar_t *path, int flags, log_t *log)
{
    if (!cfs || !path) 
	{
        return NGX_ERROR;
    }
	
    return cfs->sp->io_opt.open(path, flags, log);
}

int cfs_size_add(volatile uint64_t *old_size, uint64_t size)
{
    uint64_t nsize = 0;

    do 
	{
        nsize = *old_size;
    } while (!CAS(old_size, nsize, *old_size + size));

    return NGX_OK;
}

int cfs_size_sub(volatile uint64_t *old_size, uint64_t size, log_t *log)
{
    uint64_t nsize = 0;
    int64_t  new_size = 0;
	
    if (!old_size) 
	{
        return NGX_ERROR;
    }
	
    new_size = *old_size - size;
    if (new_size < 0) 
	{
        return NGX_ERROR;
    }
	
    do 
	{
        nsize = *old_size;
    } while (!CAS(old_size, nsize, *old_size - size));

    return NGX_OK;
}

// cfs_faio_ioinit(int thread_num) in faio.c
int cfs_prepare_work(cycle_t *cycle)
{
	cfs_t *cfs = (cfs_t *)cycle->cfs;
	//  cfs_faio_ioinit
	return cfs->sp->io_opt.ioinit(/*sconf->dio_thread_num*/20);
}

//
int cfs_ioevent_init(io_event_t *io_event)
{
    if (!io_event) 
	{
        return NGX_ERROR;
    }

    queue_init((queue_t*)&io_event->posted_events); // thread->
    queue_init((queue_t*)&io_event->posted_bad_events);

    return NGX_OK;
}

// fio 回调
void ioevents_process_posted(volatile queue_t *posted,
    dfs_atomic_lock_t *lock, fio_manager_t *fio_manager)
{
    int               i = 0;
    queue_t          *eq = nullptr;
    file_io_t        *fio = nullptr;
    dfs_lock_errno_t  error;

    while (true)
	{
        dfs_atomic_lock_on(lock, &error);

        if (queue_empty(posted)) 
		{
            dfs_atomic_lock_off(lock, &error);
			
            break;
        }

        eq = queue_head(posted);

        queue_remove(eq);
        dfs_atomic_lock_off(lock, &error);

        fio = queue_data(eq, file_io_t, q);
        if (!fio) 
		{
            return;
        }

        // block_write_complete
//        dbg(fio);
//        dbg(fio->h);
//        dbg(fio->data);

        fio->h(fio->data, fio);
//         cfs_fio_manager_free(fio, fio_manager);
        i++;
    }
}

// fio 回调
void cfs_ioevents_process_posted(io_event_t *io_event,
    fio_manager_t *fio_manager)
{
    ioevents_process_posted(&io_event->posted_bad_events, 
		&io_event->bad_lock, fio_manager);
    ioevents_process_posted(&io_event->posted_events, 
		&io_event->lock, fio_manager);
}

// 初始化 notifier
int cfs_notifier_init(faio_notifier_manager_t *faio_notify)
{
    faio_errno_t error;
    memset(&error, 0x00, sizeof(faio_errno_t));

    if (faio_mgr) 
	{
        if (faio_notifier_init(faio_notify, faio_mgr, &error) != FAIO_OK) 
		{
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}

//
void cfs_recv_event(faio_notifier_manager_t *faio_notify)
{
    faio_errno_t error;
    faio_recv_notifier(faio_notify, &error);
}


