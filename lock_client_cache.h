// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"
#include <pthread.h>
#include <map>

// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 6.
class lock_release_user {
public:
    virtual void dorelease(lock_protocol::lockid_t) = 0;

    virtual ~lock_release_user() {};
};


class lock_client_cache : public lock_client {
private:
    class lock_release_user *lu;

    pthread_mutex_t lockManagerLock;
    int rlock_port;
    std::string hostname;
    std::string id;

    enum ClientState {
        NONE, FREE, LOCKED, ACQUIRING, RELEASING
    };

    enum Message {
        EMPTY, REVOKE_RECEIVED, RETRY_RECEIVED
    };


    struct LockEntry {
        ClientState clientState;
        Message message;
        pthread_cond_t shouldAcquireAgain;

        void putMessage(Message m);
    };

    std::map<lock_protocol::lockid_t, LockEntry *> lockManager;

    LockEntry *__checkLockEntry(lock_protocol::lockid_t);

    LockEntry *__initLockEntry();

    void __lock(pthread_mutex_t *lock);

    void __unlock(pthread_mutex_t *lock);

    void __wait(pthread_cond_t *, pthread_mutex_t *);

    void __signal(pthread_cond_t *);


public:
    static int last_port;


    lock_client_cache(std::string xdst, class lock_release_user *l = 0);

    virtual ~lock_client_cache() {};

    lock_protocol::status acquire(lock_protocol::lockid_t);

    lock_protocol::status release(lock_protocol::lockid_t);

    rlock_protocol::status revoke_handler(lock_protocol::lockid_t, int &);

    rlock_protocol::status retry_handler(lock_protocol::lockid_t,
                                         int &);
};


#endif
