#include "datanode.h"
#include <arpa/inet.h>
#include "extent_client.h"
#include <unistd.h>
#include <algorithm>
#include "threader.h"

#define _DEBUG 1
#define debug_log(...)                               \
    do                                               \
    {                                                \
        if (_DEBUG)                                  \
        {                                            \
              printf("Line %d ",__LINE__);           \
              printf(__VA_ARGS__);                   \
        }                                            \
        fflush(stdout);                              \
    } while (0)

using namespace std;

int DataNode::init(const string &extent_dst, const string &namenode, const struct sockaddr_in *bindaddr) {
    ec = new extent_client(extent_dst);

    // Generate ID based on listen address
    id.set_ipaddr(inet_ntoa(bindaddr->sin_addr));
    id.set_hostname(GetHostname());
    id.set_datanodeuuid(GenerateUUID());
    id.set_xferport(ntohs(bindaddr->sin_port));
    id.set_infoport(0);
    id.set_ipcport(0);

    // Save namenode address and connect
    make_sockaddr(namenode.c_str(), &namenode_addr);
    if (!ConnectToNN()) {
        delete ec;
        ec = NULL;
        return -1;
    }

    // Register on namenode
    if (!RegisterOnNamenode()) {
        delete ec;
        ec = NULL;
        close(namenode_conn);
        namenode_conn = -1;
        return -1;
    }

    /* Add your initialization here */
    NewThread(this, &DataNode::KeepSendingHeartbeat);

    return 0;
}

void DataNode::KeepSendingHeartbeat() {
    while (true) {
        SendHeartbeat();
        sleep(1);
    }
}

bool DataNode::ReadBlock(blockid_t bid, uint64_t offset, uint64_t len, string &buf) {
    string block_str;
    assert(ec->read_block(bid, block_str) == extent_protocol::OK);
    if (offset > block_str.size())
        buf = "";
    else
        buf = block_str.substr(offset, len);
    return true;
}

bool DataNode::WriteBlock(blockid_t bid, uint64_t offset, uint64_t len, const string &buf) {
    string write_buf;
    assert(ec->read_block(bid, write_buf) == extent_protocol::OK);;
    write_buf = write_buf.substr(0, offset) + buf + write_buf.substr(offset + len);
    assert(ec->write_block(bid, write_buf) == extent_protocol::OK);
    return true;
}

