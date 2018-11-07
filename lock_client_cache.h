// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include <pthread.h>
#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"


// Classes that inherit lock_release_user can override dorelease so that
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 5.
class lock_release_user {
public:
    virtual void dorelease(lock_protocol::lockid_t) = 0;

    virtual ~lock_release_user() {};
};

class lock_client_cache : public lock_client {
private:

    enum lock_state {
        NONE, FREE, LOCKED, ACQUIRING, RELEASING
    };

    uint32_t shift;
    uint32_t mask;

    struct lock_entry {
        bool revoked;
        bool retry;
        lock_state state;
        int RPCCount;
        std::vector<rlock_protocol::xxstatus> RCPRecord;
        pthread_cond_t waitqueue;
        pthread_cond_t releasequeue;
        pthread_cond_t retryqueue;

        lock_entry() : revoked(false), retry(false), state(NONE) {
            RPCCount = 0;

            VERIFY(pthread_cond_init(&waitqueue, NULL) == 0);
            VERIFY(pthread_cond_init(&releasequeue, NULL) == 0);
            VERIFY(pthread_cond_init(&retryqueue, NULL) == 0);
        }
    };


    class lock_release_user *lu;

    int rlock_port;

    std::string id;
    pthread_mutex_t client_mutex;
    std::map<lock_protocol::lockid_t, lock_entry> lockmap;

    std::string wrap(lock_protocol::lockid_t lid, std::string s);

    std::string __getState(lock_state m);

public:
    static int last_port;

    lock_client_cache(std::string xdst, class lock_release_user *l = 0);

    virtual ~lock_client_cache() {};

    lock_protocol::status acquire(lock_protocol::lockid_t);

    lock_protocol::status release(lock_protocol::lockid_t);

    rlock_protocol::status revoke_handler(lock_protocol::lockid_t,
                                          int &);

    rlock_protocol::status retry_handler(lock_protocol::lockid_t,
                                         int &);
};


#endif
