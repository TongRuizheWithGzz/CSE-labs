#include "namenode.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sys/stat.h>
#include <unistd.h>
#include "threader.h"

using namespace std;

void NameNode::init(const string &extent_dst, const string &lock_dst) {
    ec = new extent_client(extent_dst);
    lc = new lock_client_cache(lock_dst);
    yfs = new yfs_client(extent_dst, lock_dst);

    /* Add your init logic here */
}

list<NameNode::LocatedBlock> NameNode::GetBlockLocations(yfs_client::inum ino) {
    return list<LocatedBlock>();
}

bool NameNode::Complete(yfs_client::inum ino, uint32_t new_size) {
    assert(ec->complete(ino, new_size));
    lc->release(ino);
}

NameNode::LocatedBlock NameNode::AppendBlock(yfs_client::inum ino) {
    blockid_t bid;
    extent_protocol::attr attr;
    ec->getattr(ino, attr);
    ec->append_block(ino, bid);
    LocatedBlock lb(bid, attr.size, (attr.size % BLOCK_SIZE) ? attr.size % BLOCK_SIZE : BLOCK_SIZE, master_datanode);
    return lb;
}

bool NameNode::Rename(yfs_client::inum src_dir_ino, string src_name, yfs_client::inum dst_dir_ino, string dst_name) {
    bool res = !yfs->mkdir(src_dir_ino, src_name.c_str(), mode, ino_out);
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
}

void NameNode::RegisterDatanode(DatanodeIDProto id) {
}

list<DatanodeIDProto> NameNode::GetDatanodes() {
    return list<DatanodeIDProto>();
}
