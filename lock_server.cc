// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

void Pthread_mutex_lock(pthread_mutex_t *mutex);

void Pthread_mutex_unlock(pthread_mutex_t *mutex);

void Pthread_cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex);

void Pthread_cond_signal(pthread_cond_t *cond);

lock_server::lock_server() :
        nacquire(0) {
    lock = PTHREAD_MUTEX_INITIALIZER;


}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r) {
    lock_protocol::status ret = lock_protocol::OK;
    printf("stat request from clt %d\n", clt);
    r = nacquire;
    return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r) {
    lock_protocol::status ret = lock_protocol::OK;
    // Your lab2 part2 code goes here

    Pthread_mutex_lock(&lock);

    if (lock_state.find(lid) == lock_state.end()) {
        pthread_cond_t *init = new pthread_cond_t;
        *init = PTHREAD_COND_INITIALIZER;
        cond_vars[lid] = init;
        lock_state[lid] = 1;
        Pthread_mutex_unlock(&lock);
        r = ret;
        return ret;
    }

    if (lock_state[lid] == 0) {
        lock_state[lid] = 1;
        Pthread_mutex_unlock(&lock);
        r = ret;
        return ret;
    } else {
        while (lock_state[lid] == 1)
            Pthread_cond_wait(cond_vars[lid], &lock);
        lock_state[lid] = 1;
    }
    Pthread_mutex_unlock(&lock);
    return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r) {
    lock_protocol::status ret = lock_protocol::OK;
    // Your lab2 part2 code goes here
    Pthread_mutex_lock(&lock);

    lock_state[lid] = 0;
    Pthread_cond_signal(cond_vars[lid]);
    Pthread_mutex_unlock(&lock);
    r = ret;

    return ret;
}


void Pthread_mutex_lock(pthread_mutex_t *mutex) {
    int rc = pthread_mutex_lock(mutex);
    assert(rc == 0);
}

void Pthread_mutex_unlock(pthread_mutex_t *mutex) {
    int rc = pthread_mutex_unlock(mutex);
    assert(rc == 0);
}

void Pthread_cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex) {
    int rc = pthread_cond_wait(cond, mutex);
    assert(rc == 0);
}

void Pthread_cond_signal(pthread_cond_t *cond) {
    int rc = pthread_cond_signal(cond);
    assert(rc == 0);
}
