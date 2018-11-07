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
    shift = 12;
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string m,
                               int &) {
    int r;
    std::map<lock_protocol::lockid_t, LockEntry *>::iterator iter;
    RPCRecord::iterator iter2;
    pthread_mutex_lock(&lockManagerLock);
    std::string clientID = getClientID(m);
    int nOfComingRPC = getRPCCount(m);


    iter = lockManager.find(lid);
    if (iter == lockManager.end()) {
        lockManager[lid] = new LockEntry;
        lockManager[lid]->state = NONE;
        assert(lockManager[lid]->owner.empty());
        assert(lockManager[lid]->retryingClient.empty());
        assert(lockManager[lid]->waitingClients.empty());
    }

    LockEntry *lockEntry = lockManager[lid];
    iter2 = lockEntry->receivedRpcRecord.find(clientID);

    if (iter2 == lockEntry->receivedRpcRecord.end()) {
        lockEntry->receivedRpcRecord[clientID] = std::vector<lock_protocol::xxstatus>();
    }


    if ((unsigned) nOfComingRPC > lockEntry->receivedRpcRecord[clientID].size()) {
        assert(lockEntry->receivedRpcRecord[clientID].size() == (unsigned) (nOfComingRPC - 1));
    } else {
        pthread_mutex_unlock(&lockManagerLock);
        return lockEntry->receivedRpcRecord[clientID][nOfComingRPC - 1];
    }


    switch (lockEntry->state) {

        case NONE:
            assert(lockEntry->owner.empty());
            lockEntry->state = LOCKED;
            lockEntry->owner = clientID;
            lockEntry->receivedRpcRecord[clientID].push_back(lock_protocol::OK);

            pthread_mutex_unlock(&lockManagerLock);
            return lock_protocol::OK;

        case LOCKED:
            assert(!lockEntry->waitingClients.count(clientID));
            assert(!lockEntry->owner.empty());
            assert(lockEntry->owner != clientID);

            lockEntry->waitingClients.insert(clientID);
            lockEntry->state = REVOKING;
            lockEntry->receivedRpcRecord[clientID].push_back(lock_protocol::RETRY);


            lid = lid << shift;
            lockEntry->sendedRPCCount[lockEntry->owner]++;
            lid = lid | lockEntry->sendedRPCCount[lockEntry->owner];
            pthread_mutex_unlock(&lockManagerLock);
            handle(lockEntry->owner).safebind()->call(rlock_protocol::revoke, lid, r);

            return lock_protocol::RETRY;

        case REVOKING:

            assert(!lockEntry->waitingClients.count(clientID));
            lockEntry->waitingClients.insert(clientID);
            lockEntry->receivedRpcRecord[clientID].push_back(lock_protocol::RETRY);
            pthread_mutex_unlock(&lockManagerLock);

            return lock_protocol::RETRY;


        case RETRYING:
            if (clientID == lockEntry->retryingClient) {
                assert(!lockEntry->waitingClients.count(clientID));
                lockEntry->retryingClient.clear();
                lockEntry->state = LOCKED;
                lockEntry->owner = clientID;

                if (lockEntry->waitingClients.size() > 0) {
                    lockEntry->state = REVOKING;
                    lockEntry->receivedRpcRecord[clientID].push_back(lock_protocol::OK);


                    lid = lid << shift;
                    lockEntry->sendedRPCCount[clientID]++;
                    lid = lid | lockEntry->sendedRPCCount[clientID];

                    tprintf("Server[%d]<==acquire==>RETRYING client come, give him and revoke, nOfRPC[%d]\n",
                            __getPort(clientID), lockEntry->sendedRPCCount[clientID]);


                    pthread_mutex_unlock(&lockManagerLock);
                    handle(clientID).safebind()->call(rlock_protocol::revoke, lid, r);
                    return lock_protocol::OK;

                } else {
                    lockEntry->receivedRpcRecord[clientID].push_back(lock_protocol::OK);
                    pthread_mutex_unlock(&lockManagerLock);
                    return lock_protocol::OK;
                }


            } else {
                assert(!lockEntry->waitingClients.count(clientID));
                lockEntry->waitingClients.insert(clientID);
                lockEntry->receivedRpcRecord[clientID].push_back(lock_protocol::RETRY);

                pthread_mutex_unlock(&lockManagerLock);
                return lock_protocol::RETRY;
            }
        default:
            assert(0);
    }


}

int
lock_server_cache::release(lock_protocol::lockid_t lid, std::string m,
                           int &r) {
    lock_protocol::status ret = lock_protocol::OK;
    pthread_mutex_lock(&lockManagerLock);
    LockEntry *lockEntry = lockManager[lid];
    assert(lockEntry);
    std::string clientID = getClientID(m);
    int nOfComingRPC = getRPCCount(m);

    if ((unsigned) nOfComingRPC > lockEntry->receivedRpcRecord[clientID].size())
        assert(lockEntry->receivedRpcRecord[clientID].size() == (unsigned) (nOfComingRPC - 1));
    else {
        pthread_mutex_unlock(&lockManagerLock);
        return lockEntry->receivedRpcRecord[clientID][nOfComingRPC - 1];
    }


    assert(lockEntry->state == REVOKING);
    assert(lockEntry->waitingClients.size() >= 0);
    assert(lockEntry->owner == clientID);

    std::string nextWaitingClient = *(lockEntry->waitingClients.begin());
    lockEntry->waitingClients.erase(lockEntry->waitingClients.begin());
    lockEntry->retryingClient = nextWaitingClient;
    lockEntry->owner.clear();
    lockEntry->state = RETRYING;
    lockEntry->receivedRpcRecord[clientID].push_back(lock_protocol::OK);

    lid = lid << shift;
    lockEntry->sendedRPCCount[nextWaitingClient]++;
    lid = lid | lockEntry->sendedRPCCount[nextWaitingClient];

    tprintf("Server[%d]<==release==> toRetry[%d], nOfRPC[%d]\n", __getPort(clientID), __getPort(nextWaitingClient),
            lockEntry->sendedRPCCount[nextWaitingClient]);

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

int lock_server_cache::__getPort(std::string id) {
    std::istringstream client(id);
    int r;
    char separater;
    for (int i = 0; i < 4; i++) {
        client >> r;
        client >> separater;
    }
    client >> r;
    return r;
}

std::string lock_server_cache::__dumpClients(LockEntry &le) {

    std::string result;
    std::ostringstream message;
    std::string tmp;


    std::ostringstream port;
    if (le.owner != "") {
        port << __getPort(le.owner);
        tmp = port.str();
        port.clear();
    } else tmp = "NoOwner";
    message << "Owner[" << tmp << "], ";

    switch (le.state) {
        case LOCKED:
            tmp = "LOCKED";
            break;
        case REVOKING:
            tmp = "LOCKED_AND_WAIT";
            break;
        case NONE:
            tmp = "FREE";
            break;
        case RETRYING:
            tmp = "RETRYING";
            break;
    }
    message << "lockState[" << tmp << "], waitSet:[";
    std::set<std::string>::iterator iter = le.waitingClients.begin();
    while (iter != le.waitingClients.end()) {
        message << __getPort((*iter)) << ",";
        iter++;
    }

    message << "]" << "\n";

    return message.str();
}

std::string lock_server_cache::__randomNumberGenerator() {
    return std::string();
}


int lock_server_cache::getRPCCount(std::string id) {
    std::istringstream input(id);
    int r;
    char separater;
    for (int i = 0; i < 5; i++) {
        input >> r;
        input >> separater;
    }
    input >> r;
    return r;
}

std::string lock_server_cache::getClientID(std::string id) {
    std::string delimiter = "-";
    std::string r = id.substr(0, id.find(delimiter));
    return r;
}
