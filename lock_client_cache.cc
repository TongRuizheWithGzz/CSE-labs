// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <sys/time.h>
#include "tprintf.h"


int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst,
                                     class lock_release_user *_lu)
        : lock_client(xdst), lu(_lu) {
    srand(time(NULL) ^ last_port);
    rlock_port = ((rand() % 32000) | (0x1 << 10));
    const char *hname;
    // VERIFY(gethostname(hname, 100) == 0);
    hname = "127.0.0.1";
    std::ostringstream host;
    host << hname << ":" << rlock_port;
    id = host.str();
    last_port = rlock_port;
    rpcs *rlsrpc = new rpcs(rlock_port);
    rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
    rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);
    pthread_mutex_init(&lockManagerLock, NULL);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid) {

    pthread_mutex_lock(&lockManagerLock);
    if (lockManager.find(lid) == lockManager.end()) lockManager[lid] = new LockEntry();
    LockEntry *lockEntry = lockManager[lid];
    QueuingThread *thisThread = new QueuingThread();
    if (lockEntry->threads.empty()) {
        LockState s = lockEntry->state;
        lockEntry->threads.push_back(thisThread);
        if (s == FREE) {
            lockEntry->state = LOCKED;
            pthread_mutex_unlock(&lockManagerLock);
            return lock_protocol::OK;
        } else if (s == NONE) {
            return blockUntilGot(lockEntry, lid, thisThread);
        } else {
            pthread_cond_wait(&thisThread->cv, &lockManagerLock);
            return blockUntilGot(lockEntry, lid, thisThread);
        }
    } else {
        lockEntry->threads.push_back(thisThread);
        pthread_cond_wait(&thisThread->cv, &lockManagerLock);
        switch (lockEntry->state) {
            case FREE:
		lockEntry->state = LOCKED;
            case LOCKED:
                pthread_mutex_unlock(&lockManagerLock);
                return lock_protocol::OK;
            case NONE:
                return blockUntilGot(lockEntry, lid, thisThread);
            default:
                assert(0);
        }
    }
}


lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid) {

    pthread_mutex_lock(&lockManagerLock);
    int r;
    int ret = rlock_protocol::OK;
    LockEntry *lockEntry = lockManager[lid];
    bool fromRevoked = false;
    if (lockEntry->message == REVOKE) {
        fromRevoked = true;
        lockEntry->state = RELEASING;

        pthread_mutex_unlock(&lockManagerLock);
        ret = cl->call(lock_protocol::release, lid, id, r);
        pthread_mutex_lock(&lockManagerLock);
        lockEntry->message = EMPTY;
        lockEntry->state = NONE;
    } else lockEntry->state = FREE;

    delete lockEntry->threads.front();
    lockEntry->threads.pop_front();
    if (lockEntry->threads.size() >= 1) {
        if (!fromRevoked) lockEntry->state = LOCKED;
        pthread_cond_signal(&lockEntry->threads.front()->cv);
    }
    pthread_mutex_unlock(&lockManagerLock);
    return ret;

}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid,
                                  int &) {
    int r;
    int ret = rlock_protocol::OK;
    pthread_mutex_lock(&lockManagerLock);
    LockEntry *lockEntry = lockManager[lid];
    if (lockEntry->state == FREE) {
        lockEntry->state = RELEASING;
        pthread_mutex_unlock(&lockManagerLock);
        ret = cl->call(lock_protocol::release, lid, id, r);
        pthread_mutex_lock(&lockManagerLock);
        lockEntry->state = NONE;
        if (lockEntry->threads.size() >= 1) {
            pthread_cond_signal(&lockEntry->threads.front()->cv);
        }
    } else { lockEntry->message = REVOKE; }
    pthread_mutex_unlock(&lockManagerLock);
    return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid,
                                 int &) {
    int ret = rlock_protocol::OK;
    pthread_mutex_lock(&lockManagerLock);
    LockEntry *lockEntry = lockManager[lid];
    lockEntry->message = RETRY;
    pthread_cond_signal(&lockEntry->threads.front()->cv);
    pthread_mutex_unlock(&lockManagerLock);
    return ret;
}


lock_protocol::status lock_client_cache::
blockUntilGot(lock_client_cache::LockEntry *lockEntry,
              lock_protocol::lockid_t lid,
              QueuingThread *thisThread) {
    int r;
    lockEntry->state = ACQUIRING;
    while (lockEntry->state == ACQUIRING) {
        pthread_mutex_unlock(&lockManagerLock);
        int ret = cl->call(lock_protocol::acquire, lid, id, r);
        pthread_mutex_lock(&lockManagerLock);
        if (ret == lock_protocol::OK) {
            lockEntry->state = LOCKED;
            pthread_mutex_unlock(&lockManagerLock);
            return lock_protocol::OK;
        } else {
            if (lockEntry->message == EMPTY) {
                pthread_cond_wait(&thisThread->cv, &lockManagerLock);
                lockEntry->message = EMPTY;
            } else lockEntry->message = EMPTY;
        }
    }
    assert(0);
}

lock_client_cache::~lock_client_cache() {
    pthread_mutex_lock(&lockManagerLock);
    std::map<lock_protocol::lockid_t, LockEntry *>::iterator iter;
    for (iter = lockManager.begin(); iter != lockManager.end(); iter++)
        delete iter->second;
    pthread_mutex_unlock(&lockManagerLock);
}







