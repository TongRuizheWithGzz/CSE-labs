#include "namenode.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sys/stat.h>
#include <unistd.h>
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

void NameNode::init(const string &extent_dst, const string &lock_dst) {
    ec = new extent_client(extent_dst);
    lc = new lock_client_cache(lock_dst);
    yfs = new yfs_client(extent_dst, lock_dst);

    /* Add your init logic here */
    heart_beats = 0;
    NewThread(this, &NameNode::beat);
}

void NameNode::beat() {
    while (true) {
        this->heart_beats++;
        sleep(1);
    }
}

list<NameNode::LocatedBlock> NameNode::GetBlockLocations(yfs_client::inum ino) {
    list<NameNode::LocatedBlock> l;
    uint64_t size = 0;
    list<blockid_t> block_ids;
    ec->get_block_ids(ino, block_ids);
    extent_protocol::attr attr;
    ec->getattr(ino, attr);
    unsigned i = 0;
    for (auto item : block_ids) {
        i++;
        LocatedBlock lb(item, size, i < block_ids.size() ? BLOCK_SIZE : (attr.size - size), GetDatanodes());
        l.push_back(lb);
        size += BLOCK_SIZE;
    }
    return l;
}

bool NameNode::Complete(yfs_client::inum ino, uint32_t new_size) {
    assert(ec->complete(ino, new_size) == extent_protocol::OK);
    lc->release(ino);
    return true;
}

NameNode::LocatedBlock NameNode::AppendBlock(yfs_client::inum ino) {
    blockid_t bid;
    extent_protocol::attr attr;
    ec->getattr(ino, attr);
    ec->append_block(ino, bid);
    LocatedBlock lb(bid, attr.size, (attr.size % BLOCK_SIZE) ? attr.size % BLOCK_SIZE : BLOCK_SIZE,
                    GetDatanodes());
    dirty_blocks.insert(bid);
    return lb;
}

bool
NameNode::Rename(yfs_client::inum src_dir_ino, string src_name, yfs_client::inum dst_dir_ino, string dst_name) {
    string sbuf, dbuf;
    bool sfound;
    std::string append;
    yfs_client::inum sino_out;
    string buf;
    yfs->__lookup_dir(src_dir_ino, src_name.c_str(), sfound, sino_out);

    if (sfound) {
        assert(yfs->rmdir(src_dir_ino, src_name.c_str()) == yfs_client::OK);
        ec->get(dst_dir_ino, dbuf);

        struct yfs_client::dir_entry new_dirent;
        new_dirent.inum = sino_out;
        new_dirent.file_name_length = (unsigned short) dst_name.size();
        memcpy(new_dirent.file_name, dst_name.c_str(), new_dirent.file_name_length);
        append.assign((char *) (&new_dirent), sizeof(struct yfs_client::dir_entry));
        dbuf += append;
        ec->put(dst_dir_ino, dbuf);
    }
    return sfound;
}


bool NameNode::Mkdir(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
    bool result = yfs->mkdir(parent, name.c_str(), mode, ino_out) == yfs_client::OK;
    return result;
}

bool NameNode::Create(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
    bool result = yfs->create(parent, name.c_str(), mode, ino_out) == yfs_client::OK;
    if (result) lc->acquire(ino_out);
    return result;
}

bool NameNode::Isfile(yfs_client::inum ino) {
    extent_protocol::attr a;
    assert(ec->getattr(ino, a) == extent_protocol::OK);
    return a.type == extent_protocol::T_FILE;
}

bool NameNode::Isdir(yfs_client::inum ino) {
    extent_protocol::attr a;
    assert(ec->getattr(ino, a) == extent_protocol::OK);
    return a.type == extent_protocol::T_DIR;
}

bool NameNode::Getfile(yfs_client::inum ino, yfs_client::fileinfo &info) {
    extent_protocol::attr a;
    if (ec->getattr(ino, a) != extent_protocol::OK) return false;
    info.atime = a.atime;
    info.mtime = a.mtime;
    info.ctime = a.ctime;
    info.size = a.size;
    return true;
}

bool NameNode::Getdir(yfs_client::inum ino, yfs_client::dirinfo &info) {
    extent_protocol::attr a;
    if (ec->getattr(ino, a) != extent_protocol::OK)return false;
    info.atime = a.atime;
    info.mtime = a.mtime;
    info.ctime = a.ctime;
    return true;
}

bool NameNode::Readdir(yfs_client::inum ino, std::list<yfs_client::dirent> &dir) {
    std::list<yfs_client::dir_entry> entries;
    yfs->__list_dir(ino, entries);
    std::list<yfs_client::dir_entry>::iterator it = entries.begin();
    for (; it != entries.end(); it++) {
        struct yfs_client::dirent dirent;
        dirent.inum = it->inum;
        dirent.name.assign(it->file_name, it->file_name_length);
        dir.push_back(dirent);
    }
    return true;

}

bool NameNode::Unlink(yfs_client::inum parent, string name, yfs_client::inum ino) {
    yfs->rmdir(parent, name.c_str());
    return true;
}

void NameNode::DatanodeHeartbeat(DatanodeIDProto id) {
    data_nodes[id] = this->heart_beats;
}

void NameNode::RegisterDatanode(DatanodeIDProto id) {
    for (auto b : dirty_blocks) {
        ReplicateBlock(b, master_datanode, id);
    }
    data_nodes.insert(make_pair(id, this->heart_beats));
}

list<DatanodeIDProto> NameNode::GetDatanodes() {
    list<DatanodeIDProto> live_data_nodes;
    for (auto i : data_nodes) {
        if (i.second >= this->heart_beats - 3) {
            live_data_nodes.push_back(i.first);
        }
    }
    return live_data_nodes;
}
