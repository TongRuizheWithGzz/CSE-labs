#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <set>
#include <arpa/inet.h>
#include <sys/time.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


lock_server_cache::lock_server_cache() {
    VERIFY(pthread_mutex_init(&lockManagerLock, NULL) == 0);
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string clientID,
                               int &) {
    int r;
    std::map<lock_protocol::lockid_t, LockEntry *>::iterator iter;
    pthread_mutex_lock(&lockManagerLock);


    iter = lockManager.find(lid);
    if (iter == lockManager.end()) {
        lockManager[lid] = new LockEntry;
        lockManager[lid]->state = NONE;
        assert(lockManager[lid]->owner.empty());
        assert(lockManager[lid]->retryingClient.empty());
        assert(lockManager[lid]->waitingClients.empty());
    }

    LockEntry *lockEntry = lockManager[lid];


    switch (lockEntry->state) {
        case NONE:
            assert(lockEntry->owner.empty());
            lockEntry->state = LOCKED;
            lockEntry->owner = clientID;
            pthread_mutex_unlock(&lockManagerLock);
            return lock_protocol::OK;

        case LOCKED:
            assert(!lockEntry->waitingClients.count(clientID));
            assert(!lockEntry->owner.empty());
            assert(lockEntry->owner != clientID);

            lockEntry->waitingClients.insert(clientID);
            lockEntry->state = REVOKING;

            pthread_mutex_unlock(&lockManagerLock);
            handle(lockEntry->owner).safebind()->call(rlock_protocol::revoke, lid, r);

            return lock_protocol::RETRY;

        case REVOKING:
            assert(!lockEntry->waitingClients.count(clientID));
            lockEntry->waitingClients.insert(clientID);
            pthread_mutex_unlock(&lockManagerLock);
            return lock_protocol::RETRY;
        case RETRYING:
            if (clientID == lockEntry->retryingClient) {
                assert(!lockEntry->waitingClients.count(clientID));
                lockEntry->retryingClient.clear();
                lockEntry->state = LOCKED;
                lockEntry->owner = clientID;
                if (!lockEntry->waitingClients.empty()) {
                    lockEntry->state = REVOKING;
                    pthread_mutex_unlock(&lockManagerLock);
                    handle(clientID).safebind()->call(rlock_protocol::revoke, lid, r);
                    return lock_protocol::OK;
                } else {
                    pthread_mutex_unlock(&lockManagerLock);
                    return lock_protocol::OK;
                }
            } else {
                assert(!lockEntry->waitingClients.count(clientID));
                lockEntry->waitingClients.insert(clientID);
                pthread_mutex_unlock(&lockManagerLock);
                return lock_protocol::RETRY;
            }
        default:
            assert(0);
    }
}

int
lock_server_cache::release(lock_protocol::lockid_t lid, std::string clientID,
                           int &r) {
    lock_protocol::status ret = lock_protocol::OK;
    pthread_mutex_lock(&lockManagerLock);
    LockEntry *lockEntry = lockManager[lid];
    assert(lockEntry);
    assert(lockEntry->state == REVOKING);
    assert(lockEntry->waitingClients.size() >= 0);
    assert(lockEntry->owner == clientID);
    std::string nextWaitingClient = *(lockEntry->waitingClients.begin());
    lockEntry->waitingClients.erase(lockEntry->waitingClients.begin());
    lockEntry->retryingClient = nextWaitingClient;
    lockEntry->owner.clear();
    lockEntry->state = RETRYING;
    pthread_mutex_unlock(&lockManagerLock);
    handle(nextWaitingClient).safebind()->call(rlock_protocol::retry, lid, r);
    return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r) {
    tprintf("stat request\n");
    r = nacquire;
    return lock_protocol::OK;
}

lock_server_cache::~lock_server_cache() {

    pthread_mutex_lock(&lockManagerLock);
    std::map<lock_protocol::lockid_t, LockEntry *>::iterator iter;
    for (iter = lockManager.begin(); iter != lockManager.end(); iter++)
        delete iter->second;

    pthread_mutex_unlock(&lockManagerLock);
}


