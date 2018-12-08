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
        NONE, LOCKED, REVOKING, RETRYING
    };


    struct LockEntry {
        std::string owner;
        std::string retryingClient;
        std::set<std::string> waitingClients;
        State state;


    };

    std::map<lock_protocol::lockid_t, LockEntry *> lockManager;
    pthread_mutex_t lockManagerLock;


public:

    lock_server_cache();

    ~lock_server_cache();

    lock_protocol::status stat(lock_protocol::lockid_t, int &);

    int acquire(lock_protocol::lockid_t, std::string id, int &);

    int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
