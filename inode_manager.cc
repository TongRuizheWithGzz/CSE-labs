#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk() {
    bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf) {
    if (id < 0 || id >= BLOCK_NUM || buf == NULL)
        return;

    memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf) {
    if (id < 0 || id >= BLOCK_NUM || buf == NULL)
        return;

    memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

blockid_t
block_manager::alloc_block() {
    /*
     * your code goes here.
     * note: you should mark the corresponding bit in block bitmap when alloc.
     * you need to think about which block you can start to be allocated.
     */

    return 0;
}

void
block_manager::free_block(uint32_t id) {
    /*
     * your code goes here.
     * note: you should unmark the corresponding bit in the block bitmap when free.
     */

    return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager() {
    d = new disk();

    // format the disk
    sb.size = BLOCK_SIZE * BLOCK_NUM;
    sb.nblocks = BLOCK_NUM;
    sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf) {
    d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf) {
    d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager() {
    bm = new block_manager();
    uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
    if (root_dir != 1) {
        printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
        exit(0);
    }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type) {
    /*
     * your code goes here.
     * note: the normal inode block should begin from the 2nd inode block.
     * the 1st is used for root_dir, see inode_manager::inode_manager().
     */
    return 1;
}

void
inode_manager::free_inode(uint32_t inum) {
    /*
     * your code goes here.
     * note: you need to check if the inode is already a freed one;
     * if not, clear it, and remember to write back to disk.
     */

    return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode *
inode_manager::get_inode(uint32_t inum) {
    struct inode *ino, *ino_disk;
    char buf[BLOCK_SIZE];

    printf("\tim: get_inode %d\n", inum);

    if (inum < 0 || inum >= INODE_NUM) {
        printf("\tim: inum out of range\n");
        return NULL;
    }

    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    // printf("%s:%d\n", __FILE__, __LINE__);

    ino_disk = (struct inode *) buf + inum % IPB;
    if (ino_disk->type == 0) {
        printf("\tim: inode not exist\n");
        return NULL;
    }

    ino = (struct inode *) malloc(sizeof(struct inode));
    *ino = *ino_disk;

    return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino) {
    char buf[BLOCK_SIZE];
    struct inode *ino_disk;

    printf("\tim: put_inode %d\n", inum);
    if (ino == NULL)
        return;

    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    ino_disk = (struct inode *) buf + inum % IPB;
    *ino_disk = *ino;
    bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a, b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size) {
    /*
     * your code goes here.
     * note: read blocks related to inode number inum,
     * and copy them to buf_Out
     */

    return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size) {
    /*
     * your code goes here.
     * note: write buf to blocks of inode inum.
     * you need to consider the situation when the size of buf
     * is larger or smaller than the size of original inode
     */

    return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a) {
    /*
     * your code goes here.
     * note: get the attributes of inode inum.
     * you can refer to "struct attr" in extent_protocol.h
     */

    return;
}

void
inode_manager::remove_file(uint32_t inum) {
    /*
     * your code goes here
     * note: you need to consider about both the data block and inode of the file
     */

    return;
}

void
inode_manager::append_block(uint32_t inum, blockid_t &bid) {


}

void
inode_manager::get_block_ids(uint32_t inum, std::list<blockid_t> &block_ids) {
    struct inode *ino = get_inode(inum);
    uint32_t size = ino->size;
    uint32_t block_num = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;
    uint32_t start = 0;
    for (; start < block_num; start++)
        block_ids.push_back(__get_nth_blockid(ino, start));
    free(ino);
}

void
inode_manager::read_block(blockid_t id, char buf[BLOCK_SIZE]) {
    bm->read_block(id, buf);
}

void
inode_manager::write_block(blockid_t id, const char buf[BLOCK_SIZE]) {
    bm->write_block(id, buf);
}

void
inode_manager::complete(uint32_t inum, uint32_t size) {
    struct inode *ino = get_inode(inum);
    ino->atime = (unsigned) std::time(0);
    ino->mtime = (unsigned) std::time(0);
    ino->ctime = (unsigned) std::time(0);
    ino->size = size;
    put_inode(inum, ino);
    free(ino);

}

blockid_t inode_manager::__get_nth_blockid(struct inode *ino, uint32_t nth) {
    assert(ino);
    uint32_t size = ino->size;
    assert(size != 0);
    uint32_t block_num = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;
    assert(nth < block_num);
    if (nth < NDIRECT)
        return ino->blocks[nth];
    blockid_t inblock_id = ino->blocks[NDIRECT];
    char inblock[BLOCK_SIZE];
    bm->read_block(inblock_id, inblock);
    return (((blockid_t *) inblock)[nth - NDIRECT]);
}

void inode_manager::__alloc_nth_block(struct inode *ino, uint32_t nth, std::string &buf, bool to_write) {
    assert(ino);
    blockid_t bl_id = bm->alloc_block();
    if (to_write)
        bm->write_block(bl_id, buf.data());
    if (nth < NDIRECT)
        ino->blocks[nth] = bl_id;
    else {
        blockid_t inblock_id = ino->blocks[NDIRECT];
        char inblock[BLOCK_SIZE];
        bm->read_block(inblock_id, inblock);
        (((blockid_t *) inblock)[nth - NDIRECT]) = bl_id;
        bm->write_block(inblock_id, inblock);
    }

}

void inode_manager::__free_nth_block(struct inode *ino, uint32_t nth) {

    blockid_t id = __get_nth_blockid(ino, nth);
    bm->free_block(id);

}

void inode_manager::__read_nth_block(struct inode *ino, uint32_t nth, std::string &buf) {

    blockid_t bl_id = __get_nth_blockid(ino, nth);

    char content[BLOCK_SIZE];

    bm->read_block(bl_id, content);

    buf.assign(content, BLOCK_SIZE);

}

void inode_manager::__write_nth_block(struct inode *ino, uint32_t nth, std::string &buf) {
    blockid_t bl_id = __get_nth_blockid(ino, nth);

    assert(buf.size() == BLOCK_SIZE);

    bm->write_block(bl_id, buf.data());
}

