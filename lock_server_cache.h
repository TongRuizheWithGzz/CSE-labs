#ifndef lock_server_cache_h
#define lock_server_cache_h


#include <string>


#include <map>
#include <set>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"


class lock_server_cache {
private:
    int nacquire;

    enum State {
        NONE,   // The server knows nothing about the client.
        LOCKED, // The lock is being held by an client.
        REVOKING, // The server has sent a REVOKE RPC.
        RETRYING, // The lock is free. The server has dispatched a retry RPC.
    };

    struct LockEntry {
        State state;
        std::string holdingClient;
        std::string retryingClient;
        std::set<std::string> waitingClients;

    };

    std::map<lock_protocol::lockid_t, LockEntry *> lockManager;

    pthread_mutex_t lockManagerLock;

    LockEntry *__initLockEntry();

    void __lock(pthread_mutex_t *lock);

    void __unlock(pthread_mutex_t *lock);

    int __getPort(std::string id);

public:


    lock_server_cache();

    lock_protocol::status stat(lock_protocol::lockid_t, int &);


    int acquire(lock_protocol::lockid_t, std::string id, int &);

    int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
