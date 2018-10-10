#include <string>
#include <ctime>

#include "inode_manager.h"
// The disk size is 1024*1024*16 bytes, and one block is 512 bytes.
// So blockNumber = diskSize/blockSize = 32768, meaning that we have 32768 blocks in total.

// One byte can store 8 bits recording whether a block is in use.
// So we need blockNumber/BPB = 4096 bytes to store the free block bitmap, that is, 8 blocks.



#define max(a, b)  ((a)>(b)?(a):(b))
#define min(a, b)  ((a)<(b)?(a):(b))
#define _DEBUG_PART2_E_X

#define _DEBUG 1
#define debug_log(...)                               \
    do                                               \
    {                                                \
        if (_DEBUG)                                  \
        {                                            \
              printf("Line %d ",__LINE__);           \
              printf(__VA_ARGS__);                   \
        }                                            \
    } while (0)

// disk layer -----------------------------------------


disk::disk() {
    bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf) {

    // buf should be allocated on stack or heap.
    if (!buf) {
        debug_log("disk::read_block ERROR: 2st arg buf is NULL, will exit\n");
        exit(0);
    }

    //block id out of range
    if (id >= BLOCK_NUM) {
        debug_log("disk::read_block ERROR: block id out of range, will exit\n");
        exit(0);
    }

    memcpy(buf, blocks[id], BLOCK_SIZE);


}

void
disk::write_block(blockid_t id, const char *buf) {

    //buf can not be NULL
    if (!buf) {
        debug_log("disk::write_block ERROR: 2st arg buf is NULL, will exit\n");
        exit(0);
    }

    //block id out of range
    if (id >= BLOCK_NUM) {
        debug_log("disk::read_block ERROR: block id out of range, will exit\n");
        exit(0);
    }

    memcpy(blocks[id], buf, BLOCK_SIZE);

}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block() {

    // According to the layout of block area: |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
    // The start block taken into account should be next to the inode table

    blockid_t hover = next_block;
    blockid_t start = IBLOCK(INODE_NUM, sb.nblocks) + 1;
    for (; hover < BLOCK_NUM; hover++) {
        int is_used = using_blocks[hover];
        if (is_used == 0) {
            using_blocks[hover] = 1;
            next_block = (hover + 1) == BLOCK_NUM ? start : hover + 1;
            return hover;
        }
    }

    for (hover = start; hover < next_block; hover++) {
        int is_used = using_blocks[hover];
        if (is_used == 0) {
            using_blocks[hover] = 1;
            next_block = (hover + 1) == BLOCK_NUM ? start : hover + 1;
            return hover;
        }
    }

    // Control flow could't reach here except that blocks are run out of.
    debug_log("block_manager::alloc_block error: Blocks are run out of.\n");
    exit(0);
}

void
block_manager::free_block(uint32_t id) {
    /*
     * your code goes here.
     * note: you should unmark the corresponding bit in the block bitmap when free.
     */

    if (using_blocks[id] == 0) {
        debug_log("bm::free_block ERROR: free unused block, will exit\n");
        exit(0);
    }

    using_blocks[id] = 0;
    return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager() {
    d = new disk();

    // format the disk
    next_block = IBLOCK(INODE_NUM, sb.nblocks) + 1;
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
    struct inode root;
    root.type = extent_protocol::T_DIR;
    root.size = 0;
    root.atime = (unsigned) std::time(0);
    root.mtime = (unsigned) std::time(0);
    root.ctime = (unsigned) std::time(0);
    put_inode(1, &root);
    next_inum = 2;
}


uint32_t
inode_manager::alloc_inode(uint32_t type) {
    bool found = false;
    struct inode *ino = NULL;
    uint32_t hover = next_inum;
    for (; hover <= INODE_NUM; hover++) {
        ino = get_inode(hover);
        if (ino->type == 0) {
            found = true;
            break;
        } else free(ino);
    }
    if (!found) {
        hover = 2;
        for (; hover < next_inum; hover++) {
            ino = get_inode(hover);
            if (ino->type == 0) {
                found = true;
                break;
            } else free(ino);
        }
    }
    if (!found) {
        debug_log("im::alloc_inode ERROR: cannot allocate inode, will exit\n");
        exit(0);
    }

    next_inum = (hover + 1) > INODE_NUM ? 2 : (hover + 1);
    ino->type = (short) type;
    ino->size = 0;
    ino->atime = (unsigned) std::time(0);
    ino->mtime = (unsigned) std::time(0);
    ino->ctime = (unsigned) std::time(0);
    put_inode(hover, ino);
    free(ino);
    return hover;
}

void
inode_manager::free_inode(uint32_t inum) {

    struct inode *ino = get_inode(inum);
    if (!ino) {
        debug_log("im::free_inode ERROR: inode returned by get_inode() is NULL, will exit\n");
        exit(0);
    }

    //The inode is already a freed one
    if (ino->type == 0) {
        debug_log("im::free_inode WARNING: the inode is already a freed one\n");
        exit(0);
    }

    ino->type = 0;
    put_inode(inum, ino);
    free(ino);
    return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode *
inode_manager::get_inode(uint32_t inum) {
    struct inode *ino, *ino_disk;
    char buf[BLOCK_SIZE];
    if (inum <= 0 || inum > INODE_NUM) {
        debug_log("im::get_inode ERROR: inum %u out of range\n", inum);
        exit(0);
    }
    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    ino_disk = (struct inode *) buf + inum % IPB;


    ino = (struct inode *) malloc(sizeof(struct inode));
    *ino = *ino_disk;

    return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino) {
    char buf[BLOCK_SIZE];
    struct inode *ino_disk;

    if (ino == NULL) {
        debug_log("im::put_indoe ERROR: ino is null, will exit\n");
        return;
    }

    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    ino_disk = (struct inode *) buf + inum % IPB;
    *ino_disk = *ino;
    bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}


/* Get all the data of a file by inum.
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buLf_out, int *size) {

    struct inode *ino = get_inode(inum);    //read the file's inode

    std::string content;
    *size = ino->size;
    if (ino->size == 0)
        return;
    if (ino->size > MAXFILE * BLOCK_SIZE)
        exit(0);


    uint32_t block_num = ((ino->size - 1) / BLOCK_SIZE + 1);
    char *rv = (char *) malloc(BLOCK_NUM * BLOCK_SIZE);
    assert(rv);

    for (uint32_t nth = 0; nth < block_num; nth++) {
        __read_nth_block(ino, nth, content);
        memcpy(rv + nth * BLOCK_SIZE, content.data(), BLOCK_SIZE);
    }

    *buLf_out = rv;

    ino->atime = (unsigned) std::time(0);
    put_inode(inum, ino);
    free(ino);


}


/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size) {


    if (size < 0) {
        debug_log("im: write)file: the size arg < 0, will exit\n");
        exit(0);
    }

    //read the file's inode
    struct inode *ino = get_inode(inum);
    if (!ino) {
        debug_log("im::write_file ERROR: inode returned by get_inode() is NULL, will exit\n");
        free(ino);
        exit(0);
        return;
    }

    // Corner case: Size larger than MAXFILE, exit
    if ((uint32_t) size > MAXFILE * BLOCK_SIZE) {
        debug_log("im::write_file ERROR: size larger than MAXFILE, will exit\n");
        free(ino);
        exit(0);
    }


    debug_log("im::write_file: from { inum:%u, size:%u, type:%d, block_num:%d}, to {size:%u, block_num:%d},\n", inum,
              ino->size, ino->type,
              ino->size == 0 ? 0 : ((ino->size - 1) / BLOCK_SIZE + 1), size,
              size == 0 ? 0 : ((size - 1) / BLOCK_SIZE + 1));

    //original file size
    uint32_t original_size = ino->size;

    //the number of blocks the file occupies
    uint32_t block_num = original_size == 0 ? 0 : ((original_size - 1) / BLOCK_SIZE + 1);

    //index for traverse direct blocks
    uint32_t direct_block_index = 0;

    //local variable for storing block_id
    blockid_t blockid;

    //If the inode need indirect block,
    //left_blocks_num is the number of blocks except NINDIRECT blocks
    uint32_t left_blocks_num = 0;

    //index for traverse indirect blocks
    uint32_t indirect_block_index = 0;

    //local variable to store the indirect block_id;
    blockid_t indirect_block_id;

    //load the indirect block
    char *indirect_block;

    //size % BLOCK_SIZE if left, and the buf is cut
    char padding_block[BLOCK_SIZE];

    //free all the blocks of the file
    for (direct_block_index = 0; direct_block_index < min(block_num, NDIRECT); direct_block_index++) {
        blockid = ino->blocks[direct_block_index];
#ifdef  _DEBUG_PART2_E
        debug_log("im::write_file: FREE BLOCK[%u]\n", blockid);
#endif
        bm->free_block(blockid);
    }

    //block_num > NDIRECT, indirect blocks exists
    if (block_num > NDIRECT) {
        left_blocks_num = block_num - NDIRECT;

        //This is the indirect index block id
        indirect_block_id = ino->blocks[NDIRECT];

        //allocate memory to read the indirect index block
        indirect_block = (char *) malloc(BLOCK_SIZE);
        assert(indirect_block);         //check for robustness


        bm->read_block(indirect_block_id, indirect_block);
        for (indirect_block_index = 0; indirect_block_index < left_blocks_num; indirect_block_index++) {
            blockid = ((blockid_t *) indirect_block)[indirect_block_index];
#ifdef  _DEBUG_PART2_E
            debug_log("im::write_file: FREE BLOCK[%u]\n", blockid);
#endif
            bm->free_block(blockid);
        }
#ifdef  _DEBUG_PART2_E
        debug_log("im::write_file: FREE BLOCK[%u]\n", indirect_block_id);
#endif
        bm->free_block(indirect_block_id); //Remember to free the indirect index block too!
        free(indirect_block);   // avoid memory leak
    }


    //update the size stored in the inode, corner cases checked
    ino->size = (unsigned int) size;

    //update the block num, corner cases checked
    block_num = (unsigned int) (size == 0 ? 0 : (size - 1) / BLOCK_SIZE + 1);



//  <-------   write the data to  the blocks of the file ------->

    for (direct_block_index = 0; direct_block_index < min(block_num, NDIRECT); direct_block_index++) {
        blockid = bm->alloc_block();
        ino->blocks[direct_block_index] = blockid;


        //When write the last block, remember to align the buf
        if ((direct_block_index + 1) * BLOCK_SIZE > (uint32_t) size) {
            uint32_t padding_bytes = (direct_block_index + 1) * BLOCK_SIZE - size;
            memcpy(padding_block, buf + direct_block_index * BLOCK_SIZE, BLOCK_SIZE - padding_bytes);
            memset(padding_block + BLOCK_SIZE - padding_bytes, 0, padding_bytes);
            bm->write_block(blockid, padding_block);
            continue;
        }
        bm->write_block(blockid, buf + direct_block_index * BLOCK_SIZE);
    }


    //block_num > NDIRECT, indirect blocks exists
    if (block_num > NDIRECT) {
        left_blocks_num = block_num - NDIRECT;
        indirect_block_id = bm->alloc_block();
        ino->blocks[NDIRECT] = indirect_block_id;
        uint32_t MAX_DIRECT_BLOCK_SIZE = NDIRECT * BLOCK_SIZE;
        char *indirect_block = (char *) malloc(BLOCK_SIZE);
        assert(indirect_block);

        for (indirect_block_index = 0; indirect_block_index < left_blocks_num; indirect_block_index++) {

            blockid = bm->alloc_block();
            ((blockid_t *) indirect_block)[indirect_block_index] = blockid;

            if ((indirect_block_index + 1) * BLOCK_SIZE + MAX_DIRECT_BLOCK_SIZE > (uint32_t) size) {
                uint32_t padding_bytes = (indirect_block_index + 1) * BLOCK_SIZE - size + MAX_DIRECT_BLOCK_SIZE;
                memcpy(padding_block, buf + MAX_DIRECT_BLOCK_SIZE + indirect_block_index * BLOCK_SIZE,
                       BLOCK_SIZE - padding_bytes);
                memset(padding_block + BLOCK_SIZE - padding_bytes, 0, padding_bytes);
                bm->write_block(blockid, padding_block);
                continue;
            }
            bm->write_block(blockid, buf + MAX_DIRECT_BLOCK_SIZE + BLOCK_SIZE * indirect_block_index);
        }

        bm->write_block(indirect_block_id, indirect_block);
        free(indirect_block);   // avoid memory leak
    }

    //Put the inode into disk
    ino->atime = (unsigned) std::time(0);
    ino->mtime = (unsigned) std::time(0);
    ino->ctime = (unsigned) std::time(0);


    put_inode(inum, ino);
#ifdef  _DEBUG_PART2_E
    debug_log("===== WRITE FILE PUT INODE CHECK =====\n");
    ino = get_inode(inum);
    printf("The file contains blocks [");
    for (int i = 0; i < block_num; i++) {
        printf("%u,", ino->blocks[i]);
    }
    printf("]\n");
    debug_log("===== WRITE FILE PUT INODE CHECK END=====\n");

#endif
    //REMEMBER to free the  inode allocated by get_inode
    free(ino);
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a) {
    /*
     * your code goes here.
     * note: get the attributes of inode inum.
     * you can refer to "struct attr" in extent_protocol.h
     */
    struct inode *ino = get_inode(inum);
    if (!ino) {
        debug_log("im::getattr WARNING: inode %u is NULL from get inode\n", inum);
        return;
    }

    //TODO: if structure of inode is modified in PART2, REMEMBER to do something here
    a.type = (uint32_t) ino->type;
    a.atime = ino->atime;
    a.ctime = ino->ctime;
    a.mtime = ino->mtime;
    a.size = ino->size;

//    caller of get_inode need to fre
    free(ino);

}

void
inode_manager::remove_file(uint32_t inum) {
    /*
     * your code goes here
     * note: you need to consider about both the data block and inode of the file
     */
    //read the file's inode
    struct inode *ino = get_inode(inum);
    if (!ino) {
        debug_log("im::remove ERROR: inode returned by get_inode() is NULL, will exit\n");
        exit(0);
        return;
    }

    debug_log("im::remove_file:  { size: %u, type: %d, block_num: %d}\n", ino->size, ino->type,
              ino->size == 0 ? 0 : ((ino->size - 1) / BLOCK_SIZE + 1));

    //original file size
    uint32_t size = ino->size;

    //the number of blocks the file occupies
    uint32_t block_num = size == 0 ? 0 : ((size - 1) / BLOCK_SIZE + 1);


    //index for traverse direct blocks
    uint32_t direct_block_index = 0;

    //local variable for storing block_id
    blockid_t blockid;

    //If the inode need indirect block,
    //left_blocks_num is the number of blocks except NINDIRECT blocks
    uint32_t left_blocks_num = 0;

    //index for traverse indirect blocks
    uint32_t indirect_block_index = 0;

    //local variable to store the indirect block_id;
    blockid_t indirect_block_id;

    //load the indirect block
    char *indirect_block;


    //free all the blocks of the file
    for (direct_block_index = 0; direct_block_index < min(block_num, NDIRECT); direct_block_index++) {
        blockid = ino->blocks[direct_block_index];
        bm->free_block(blockid);
    }

    //block_num > NDIRECT, indirect blocks exists
    if (block_num > NDIRECT) {
        left_blocks_num = block_num - NDIRECT;

        //This is the indirect index block id
        indirect_block_id = ino->blocks[NDIRECT];

        //allocate memory to read the indirect index block
        indirect_block = (char *) malloc(BLOCK_SIZE);
        assert(indirect_block);         //check for robustness


        bm->read_block(indirect_block_id, indirect_block);
        for (indirect_block_index = 0; indirect_block_index < left_blocks_num; indirect_block_index++) {
            blockid = ((blockid_t *) indirect_block)[indirect_block_index];
            bm->free_block(blockid);
        }

        bm->free_block(indirect_block_id); //Remember to free the indirect index block too!
        free(indirect_block);   // avoid memory leak
    }

    free_inode(inum);
    free(ino);

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


void inode_manager::__alloc_nth_block(struct inode *ino, uint32_t nth, std::string &buf, bool to_write) {
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

blockid_t inode_manager::__get_nth_blockid(struct inode *ino, uint32_t nth) {

    assert(ino);
    uint32_t size = ino->size;
    assert(size != 0);

    uint32_t block_num = ((size - 1) / BLOCK_SIZE + 1);
    assert(nth < block_num);

    if (nth < NDIRECT)
        return ino->blocks[nth];

    blockid_t inblock_id = ino->blocks[NDIRECT];
    char inblock[BLOCK_SIZE];
    bm->read_block(inblock_id, inblock);

    return (((blockid_t *) inblock)[nth - NDIRECT]);


}



