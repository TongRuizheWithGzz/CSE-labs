// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


lock_server_cache::lock_server_cache() {
    nacquire = 0;
    pthread_mutex_init(&lockManagerLock, NULL);

}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id,
                               int &) {
    int r;
    lock_protocol::status ret = lock_protocol::OK;
    __lock(&lockManagerLock);
    if (lockManager.find(lid) == lockManager.end()) {
        LockEntry *newLockEntry = __initLockEntry();
        lockManager[lid] = newLockEntry;
    }

    LockEntry *lockEntry = lockManager[lid];
    State state = lockEntry->state;
    std::string owner;
    switch (state) {
        case RETRYING:
            // The client we would like to grant the lock is sending a retry RPC.
            // We should grant the lock to him
            if (lockEntry->retryingClient == id) {
                assert(!lockEntry->waitingClients.count(id));
                lockEntry->retryingClient.clear();
                lockEntry->state = LOCKED;
                lockEntry->holdingClient = id;
                __unlock(&lockManagerLock);
                return lock_protocol::OK;
            }
            // Or else, this must be a new client. Add him to the waitingSets.
        case REVOKING:
            //I have noticed your RPC call, please wait patiently.
            assert(!lockEntry->waitingClients.count(id));
            lockEntry->waitingClients.insert(id);
            __unlock(&lockManagerLock);
            return lock_protocol::RETRY;
        case LOCKED:
            // Because we use cache locks, the client held the lock should never
            // ask the lock again!!
            assert(!lockEntry->waitingClients.count(id));
            lockEntry->waitingClients.insert(id);
            owner = lockEntry->holdingClient;
            assert(owner.length() != 0);
            lockEntry->state = REVOKING;
            __unlock(&lockManagerLock);

            // What can happen here?
            // 1.   Other clients send acquire() RPC.
            //      Simply add them to the waiting set.
            handle(owner).safebind()->call(rlock_protocol::revoke, lid, r);
            // What can happen here?
            // 1.   When call() returns, since the lock is not being held,
            //      So the release RPC may have been called by the client.
            //      But it doesn't matter, because release() RPC will send retry RPC to client.
            return lock_protocol::RETRY;
        case NONE:
            // New lock entry.
            assert(lockEntry->waitingClients.size() == 0);
            assert(lockEntry->holdingClient.length() == 0);
            lockEntry->holdingClient = id;
            lockEntry->state = LOCKED;
            __unlock(&lockManagerLock);
            return lock_protocol::OK;
        default:
            assert(0);

    }
    return ret;
}

int
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id,
                           int &r) {

    lock_protocol::status ret = lock_protocol::OK;
    __lock(&lockManagerLock);
    LockEntry *lockEntry = lockManager[lid];
    assert(lockEntry);

    // Unless other client asking for the lock, no client needs call release().
    // Therefore, the waitingClients.size() > 0 here.
    assert(lockEntry->state == REVOKING);
    assert(lockEntry->waitingClients.size() != 0);
    assert(lockEntry->holdingClient == id);

    std::string nextWaitingClient = *(lockEntry->waitingClients.begin());
    lockEntry->waitingClients.erase(lockEntry->waitingClients.begin());
    lockEntry->retryingClient = nextWaitingClient;
    lockEntry->state = RETRYING;
    __unlock(&lockManagerLock);
    // What can happen here?
    // 1.   Other clients send acquire() RPC.
    //      Simply add them to the waiting set.

    handle(nextWaitingClient).safebind()->call(rlock_protocol::retry, lid, r);
    return ret;

}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r) {
    tprintf("stat request\n");
    r = nacquire;
    return lock_protocol::OK;
}

lock_server_cache::LockEntry *
lock_server_cache::__initLockEntry() {
    LockEntry *lockEntry = new LockEntry;
    lockEntry->state = NONE;
    return lockEntry;
}


void lock_server_cache::__lock(pthread_mutex_t *lock) {
    assert(pthread_mutex_lock(lock) == 0);

}

void lock_server_cache::__unlock(pthread_mutex_t *lock) {
    assert(pthread_mutex_unlock(lock) == 0);

}







