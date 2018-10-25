// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <sys/time.h>
#include "tprintf.h"

#define __DEBUG

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

    assert(pthread_mutex_init(&lockManagerLock, NULL) == 0);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid) {

    int ret = lock_protocol::OK, r;
    __lock(&lockManagerLock);
    if (lockManager.find(lid) == lockManager.end()) {
        LockEntry *newLockEntry = __initLockEntry();
        lockManager[lid] = newLockEntry;
    }

    LockEntry *lockEntry = lockManager[lid];

#ifdef __DEBUG
    std::string ms;
#endif

    while (true) {
        switch (lockEntry->clientState) {
            case NONE:
                lockEntry->clientState = ACQUIRING;
                __unlock(&lockManagerLock);
                ret = cl->call(lock_protocol::acquire, lid, id, r);
                // What can happen here?
                // 1.   Other threads call acquire(). They found that the state of lock
                //      is acquiring, so they now there is a thread acuiring the lock. Therefore, they just
                //      sleep on the conditional variable "canAcquire".
                // 2.   The server send the retry RPC before this acquire rpc returns RETRY,
                //      So we need to do extra checking below.
                // 3.   The server send the revoke RPC before this acquire rpc returns OK.
                //      As the same, do the check below.
                __lock(&lockManagerLock);

                assert(ret == lock_protocol::OK || ret == lock_protocol::RETRY);
                if (ret == lock_protocol::OK) {
                    // It doesn't matter whether a revoke RPC arrives at the client
                    // before acquire RPC returns OK. If revoker RPC arrives first,
                    // the thread will notice the fact in release() call.
                    // So, grant the lock!!
                    lockEntry->clientState = LOCKED;

                    Message m = lockEntry->message;
                    assert(m == REVOKE_RECEIVED || m == EMPTY);


                    m == REVOKE_RECEIVED ? ms = "REVOKE_RECEIVED" : ms = "EMPTY";
                    tprintf("ACQUIRE:Client[%d],state[NONE]~~~~reponse[OK],RACING message:%s,ACTION: return\n",
                            rlock_port, ms.c_str());


                    __unlock(&lockManagerLock);
                    return ret;

                } else if (ret == lock_protocol::RETRY) {
                    Message m = lockEntry->message;
                    assert(m == RETRY_RECEIVED || m == EMPTY);

                    m == RETRY_RECEIVED ? ms = "RETRY_RECEIVED" : ms = "EMPTY";
                    if (m == EMPTY) { // NO retry RPC arrives before acquire RPC
                        tprintf("ACQUIRE:Client[%d] state[NONE]~~~~reponse[RETRY] RACING message:%s,ACTION: SLEEP\n",
                                rlock_port, ms.c_str());
                        __wait(&lockEntry->shouldAcquireAgain, &lockManagerLock);
                    } else {
                        tprintf("ACQUIRE:Client[%d] state[NONE]~~~~reponse[RETRY] RACING message:%s,ACTION: reloop\n",
                                rlock_port, ms.c_str());
                        lockEntry->message = EMPTY;
                    }
                    // retry RPC arrives at the client before
                    // the corresponding acquire returns the RETRY failure code.
                    // Mark that we have notice the message(m = EMPTY),
                    // and try again through the while(true) loop.

                }
                break;

                // Another thread is holding the lock.
                // Try to be waken in release() call.
            case LOCKED:


                // Another thread is returning the lock to the server.
                // Try to be waken in release() call.
            case RELEASING:

                __wait(&lockEntry->shouldAcquireAgain, &lockManagerLock);
                break;

            case ACQUIRING:
                if (lockEntry->message == RETRY_RECEIVED) {
                    lockEntry->message = EMPTY;
                    __unlock(&lockManagerLock);
                    ret = cl->call(lock_protocol::acquire, lid, id, r);
                    __lock(&lockManagerLock);
                    assert(ret == lock_protocol::OK || ret == lock_protocol::RETRY);
                    if (ret == lock_protocol::OK) {
                        lockEntry->clientState = LOCKED;
                        Message m = lockEntry->message;
                        assert(m == REVOKE_RECEIVED || m == EMPTY);
                        m == REVOKE_RECEIVED ? ms = "REVOKE_RECEIVED" : ms = "EMPTY";
                        tprintf("ACQUIRE:Client[%d],state[ACQUIRING]~~~~reponse[OK],RACING message:%s,ACTION: return\n",
                                rlock_port, ms.c_str());

                        __unlock(&lockManagerLock);
                        return ret;
                    } else if (ret == lock_protocol::RETRY) {
                        Message m = lockEntry->message;
                        assert(m == RETRY_RECEIVED || m == EMPTY);
                        if (m == EMPTY) { // NO retry RPC arrives before acquire RPC
                            tprintf("ACQUIRE:Client[%d] state[ACQUIRING]~~~~reponse[RETRY] RACING message:%s,ACTION: SLEEP\n",
                                    rlock_port, ms.c_str());
                            __wait(&lockEntry->shouldAcquireAgain, &lockManagerLock);
                        } else {
                            tprintf("ACQUIRE:Client[%d] state[ACQUIRING]~~~~reponse[RETRY] RACING message:%s,ACTION: reloop\n",
                                    rlock_port, ms.c_str());
                            lockEntry->message = EMPTY;
                        }
                    }
                } else {
                    tprintf("ACQUIRE:Client[%d] state[ACQUIRING]~~~~No RETRYING message, others acquiring?ACTION: sleep\n",
                            rlock_port);
                    __wait(&lockEntry->shouldAcquireAgain, &lockManagerLock);
                }
                break;

                // The lock is available.
            case FREE:
                lockEntry->clientState = LOCKED;
                __unlock(&lockManagerLock);
                return lock_protocol::OK;
        }
    }

}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid) {

    int r;
    __lock(&lockManagerLock);
    LockEntry *lockEntry = __checkLockEntry(lid);
    assert(lockEntry->clientState == LOCKED);
    assert(lockEntry->message != RETRY_RECEIVED);
    if (lockEntry->message == REVOKE_RECEIVED) {
        lockEntry->clientState = RELEASING;
        __unlock(&lockManagerLock);
        cl->call(lock_protocol::release, lid, id, r);
        // What can happen here?
        // 1.   Other threads call acquire().
        //      They will find that the state of lock is RELEASING,
        //      then they sleep to be waken.
        __lock(&lockManagerLock);
        lockEntry->clientState = NONE;
        lockEntry->message = EMPTY;
    } else lockEntry->clientState = FREE;
    // I don't mind whether there are some threads waiting.
    // Just notify them.
    __signal(&lockEntry->shouldAcquireAgain);
    __unlock(&lockManagerLock);
    return lock_protocol::OK;

}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid,
                                  int &) {
    int ret = rlock_protocol::OK, r;
    __lock(&lockManagerLock);
    LockEntry *lockEntry = __checkLockEntry(lid);
    ClientState clientState = lockEntry->clientState;
    assert(clientState == LOCKED || clientState == FREE || clientState == ACQUIRING);
    if (clientState == FREE) {
        lockEntry->clientState = RELEASING;
        __unlock(&lockManagerLock);
        ret = cl->call(lock_protocol::release, lid, id, r);
        // What can happen here?
        // 1.   Other threads call acquire().
        //      They will find that the state of lock is RELEASING,
        //      then they sleep to be waken.
        __lock(&lockManagerLock);
        lockEntry->clientState = NONE;
        __signal(&lockEntry->shouldAcquireAgain);
    }
        // Otherwise,  a thread may hold the lock meaning the state is LOCKED,
        // or, and the tricky case, the state is ACQUIRING. That is, the acquire RPC call
        // from the client to server hasn't bee validated be the client.
    else lockEntry->putMessage(REVOKE_RECEIVED);
    __unlock(&lockManagerLock);
    return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid,
                                 int &) {
    int ret = rlock_protocol::OK;
    __lock(&lockManagerLock);
    LockEntry *lockEntry = __checkLockEntry(lid);
    assert(lockEntry->clientState == ACQUIRING);
    lockEntry->putMessage(RETRY_RECEIVED);
    pthread_cond_signal(&lockEntry->shouldAcquireAgain);
    __unlock(&lockManagerLock);
    return ret;
}

lock_client_cache::LockEntry *
lock_client_cache::__checkLockEntry(lock_protocol::lockid_t lid) {

    assert(lockManager.find(lid) != lockManager.end());
    LockEntry *lockEntry = lockManager[lid];
    assert(lockEntry);
    return lockEntry;

}

lock_client_cache::LockEntry *
lock_client_cache::__initLockEntry() {
    LockEntry *newLockEntry = new LockEntry;
    newLockEntry->clientState = NONE;
    newLockEntry->message = EMPTY;
    pthread_cond_init(&newLockEntry->shouldAcquireAgain, NULL);
    return newLockEntry;
}

void lock_client_cache::__lock(pthread_mutex_t *lock) {
    assert(pthread_mutex_lock(lock) == 0);
}

void lock_client_cache::__unlock(pthread_mutex_t *lock) {
    assert(pthread_mutex_unlock(lock) == 0);

}

void lock_client_cache::__wait(pthread_cond_t *cv, pthread_mutex_t *lock) {
    pthread_cond_wait(cv, lock);
}

void lock_client_cache::__signal(pthread_cond_t *cv) {
    pthread_cond_signal(cv);
}


void lock_client_cache::LockEntry::putMessage(lock_client_cache::Message m) {
    assert(this->message == EMPTY);
    this->message = m;

}
