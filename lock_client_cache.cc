// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <sys/time.h>
#include "tprintf.h"

#define __myAssert() do { \
       Message _m = lockEntry->message; \
       LockState _s = lockEntry->state;\
       if (_m == RETRY) \
            assert(_s == ACQUIRING);\
       if (_m == REVOKE)\
             assert(_s == LOCKED || _s == ACQUIRING || _s == RELEASING);\
        } while (0);\


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
    pthread_mutex_init(&client_mutex, NULL);
    mask = 4095;
    shift = 12;
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid) {

    int ret = lock_protocol::OK;
    int r;
    std::string message;
    std::map<lock_protocol::lockid_t, lock_entry>::iterator iter;
    pthread_mutex_lock(&client_mutex);
    iter = lockmap.find(lid);
    if (iter == lockmap.end()) {
        iter = lockmap.insert(std::make_pair(lid, lock_entry())).first;
    }

    while (true) {
        switch (iter->second.state) {
            case NONE:
                iter->second.state = ACQUIRING;
                iter->second.retry = false;

                message = wrap(lid, id);
                pthread_mutex_unlock(&client_mutex);
                ret = cl->call(lock_protocol::acquire, lid, message, r);
                pthread_mutex_lock(&client_mutex);
                if (ret == lock_protocol::OK) {
                    iter->second.state = LOCKED;
                    pthread_mutex_unlock(&client_mutex);
                    return ret;
                } else if (ret == lock_protocol::RETRY) {
                    if (!iter->second.retry) {
                        pthread_cond_wait(&iter->second.retryqueue, &client_mutex);
                    }
                }
                break;
            case FREE:
                iter->second.state = LOCKED;
                pthread_mutex_unlock(&client_mutex);
                return lock_protocol::OK;
                break;
            case LOCKED:
                pthread_cond_wait(&iter->second.waitqueue, &client_mutex);
                break;
            case ACQUIRING:
                if (!iter->second.retry) {
                    pthread_cond_wait(&iter->second.waitqueue, &client_mutex);
                } else {
                    iter->second.retry = false;
                    message = wrap(lid, id);
                    pthread_mutex_unlock(&client_mutex);
                    ret = cl->call(lock_protocol::acquire, lid, message, r);
                    pthread_mutex_lock(&client_mutex);
                    if (ret == lock_protocol::OK) {
                        iter->second.state = LOCKED;
                        pthread_mutex_unlock(&client_mutex);
                        return ret;
                    } else if (ret == lock_protocol::RETRY) {
                        if (!iter->second.retry)
                            pthread_cond_wait(&iter->second.retryqueue, &client_mutex);
                    }
                }
                break;
            case RELEASING:
                pthread_cond_wait(&iter->second.releasequeue, &client_mutex);
                break;
        }
    }
    return lock_protocol::OK;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid) {
    int r;
    std::string message;
    lock_protocol::status ret = lock_protocol::OK;
    std::map<lock_protocol::lockid_t, lock_entry>::iterator iter;
    pthread_mutex_lock(&client_mutex);
    iter = lockmap.find(lid);
    if (iter == lockmap.end()) {
        printf("ERROR: can't find lock with lockid = %d\n", lid);
        return lock_protocol::NOENT;
    }
    if (iter->second.revoked) {
        iter->second.state = RELEASING;
        iter->second.revoked = false;
        message = wrap(lid, id);
        pthread_mutex_unlock(&client_mutex);
        ret = cl->call(lock_protocol::release, lid, message, r);
        pthread_mutex_lock(&client_mutex);
        iter->second.state = NONE;
        pthread_cond_broadcast(&iter->second.releasequeue);
        pthread_mutex_unlock(&client_mutex);
        return ret;
    } else {
        tprintf("Client[%5d]<==RELEASE(NoRevoke & No WaitingThread)==>,lockState:[%s], simplyReturns\n", rlock_port,
                __getState(iter->second.state).c_str());

        iter->second.state = FREE;
        pthread_cond_signal(&iter->second.waitqueue);
        pthread_mutex_unlock(&client_mutex);
        return lock_protocol::OK;
    }
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid,
                                  int &) {
    int r;
    int ret = rlock_protocol::OK;
    std::string message;
    std::map<lock_protocol::lockid_t, lock_entry>::iterator iter;
    pthread_mutex_lock(&client_mutex);

    uint32_t nOfComingRPC = (uint32_t) (lid & mask);
    lid = lid >> shift;


    iter = lockmap.find(lid);
    if (iter == lockmap.end()) {
        assert(0);
    }

    tprintf("Client[%5d]<==REVOKE==>, lockState:[%s],lid[%lu],nOfComingRPC[%u]\n", rlock_port,
            __getState(iter->second.state).c_str(), lid, nOfComingRPC);

    if ((unsigned) nOfComingRPC > iter->second.RCPRecord.size()) {
        assert(iter->second.RCPRecord.size() == (unsigned) (nOfComingRPC - 1));
    } else {
        pthread_mutex_unlock(&client_mutex);

        return iter->second.RCPRecord[nOfComingRPC - 1];
    }


    if (iter->second.state == FREE) {
        iter->second.state = RELEASING;
        message = wrap(lid, id);

        pthread_mutex_unlock(&client_mutex);

        ret = cl->call(lock_protocol::release, lid, message, r);
        pthread_mutex_lock(&client_mutex);

        iter->second.state = NONE;

        pthread_cond_broadcast(&iter->second.releasequeue);
        iter->second.RCPRecord.push_back(rlock_protocol::OK);
        pthread_mutex_unlock(&client_mutex);
    } else {
        iter->second.revoked = true;
        iter->second.RCPRecord.push_back(rlock_protocol::OK);
        pthread_mutex_unlock(&client_mutex);
    }

    return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid,
                                 int &) {
    int ret = rlock_protocol::OK;
    std::map<lock_protocol::lockid_t, lock_entry>::iterator iter;
    pthread_mutex_lock(&client_mutex);

    uint32_t nOfComingRPC = (uint32_t) (lid & mask);
    lid = lid >> shift;


    iter = lockmap.find(lid);
    if (iter == lockmap.end()) {
        assert(0);
    }

    tprintf("Client[%5d]<==retry==>, lockState:[%s],lid[%lu],nOfComingRPC[%u]\n", rlock_port,
            __getState(iter->second.state).c_str(), lid, nOfComingRPC);

    if ((unsigned) nOfComingRPC > iter->second.RCPRecord.size()) {
        assert(iter->second.RCPRecord.size() == (unsigned) (nOfComingRPC - 1));
    } else {
        pthread_mutex_unlock(&client_mutex);
        return iter->second.RCPRecord[nOfComingRPC - 1];
    }


    iter->second.retry = true;
    pthread_cond_signal(&iter->second.retryqueue);


    iter->second.RCPRecord.push_back(rlock_protocol::OK);

    pthread_mutex_unlock(&client_mutex);
    return ret;
}

std::string
lock_client_cache::wrap(lock_protocol::lockid_t lid, std::string s) {
    lockmap[lid].RPCCount++;
    std::ostringstream o;
    o << "-";
    o << lockmap[lid].RPCCount;
    return s + o.str();
}

std::string lock_client_cache::__getState(lock_client_cache::lock_state m) {
    switch (m) {
        case NONE:
            return "NONE";
        case ACQUIRING:
            return "ACQUIRING";
        case FREE:
            return "FREE";
        case LOCKED:
            return "LOCKED";
        case RELEASING:
            return "RELEASING";
        default :
            assert(0);
    }
}




