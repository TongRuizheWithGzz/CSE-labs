#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>

#include <map>
#include <set>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"

typedef std::map<std::string, std::vector<lock_protocol::xxstatus> > RPCRecord;
typedef std::map<std::string, int> cRPCRecord;


class lock_server_cache {


private:

    int nacquire;
    enum State {
        NONE, LOCKED, REVOKING, RETRYING
    };

    uint32_t shift;

    struct LockEntry {
        std::string owner;
        std::string retryingClient;
        std::set<std::string> waitingClients;
        RPCRecord receivedRpcRecord;
        cRPCRecord sendedRPCCount;
        State state;


    };


    std::map<lock_protocol::lockid_t, LockEntry *> lockManager;
    pthread_mutex_t lockManagerLock;


    int __getPort(std::string id);


    std::string __dumpClients(LockEntry &le);

    std::string __randomNumberGenerator();

    int getRPCCount(std::string);

    std::string getClientID(std::string id);

public:

    lock_server_cache();

    lock_protocol::status stat(lock_protocol::lockid_t, int &);

    int acquire(lock_protocol::lockid_t, std::string id, int &);

    int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
