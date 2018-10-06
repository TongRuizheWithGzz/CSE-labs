// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <list>

#define _DEBUG 1
#define _DEBUG_PART2_A_X
#define debug_log(...)                               \
    do                                               \
    {                                                \
        if (_DEBUG)                                  \
        {                                            \
              printf("Line %d ",__LINE__);           \
              printf(__VA_ARGS__);                   \
        }                                            \
    } while (0)


yfs_client::yfs_client() {

    ec = new extent_client();

}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst) {
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

yfs_client::inum
yfs_client::n2i(std::string n) {
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum) {
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum) {
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    }
    printf("isfile: %lld is not a dir\n", inum);
    return false;
}

/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
yfs_client::isdir(inum inum) {
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isfile: %lld is a dir\n", inum);
        return true;
    }
    printf("isfile: %lld is not a dir\n", inum);
    return false;
}

bool
yfs_client::issymlink(inum inum) {
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_SYMLINK) {
        printf("isfile: %lld is a symlink\n", inum);
        return true;
    }
    printf("isfile: %lld is not a symlink\n", inum);
    return false;
}


int
yfs_client::getfile(inum inum, fileinfo &fin) {
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

    release:
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din) {
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

    release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size) {
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */

    std::string buf;
    r = ec->get(ino, buf);
    if (r != OK) {
        printf("setattr: file not exist\n");
        return r;
    }

    buf.resize(size);

    r = ec->put(ino, buf);
    if (r != OK) {
        printf("setattr: update failed\n");
        return r;
    }

    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out) {

#ifdef _DEBUG_PART2_A
    debug_log("====== CREATE FILE ==========\n");
    debug_log("yc::create,try to create->{parent:%llu, name:%s}\n", parent, name);
#endif


    int r = OK;

    //file name length shouldn't be longer than MAX_FILENAME_LENGTH
    assert(strlen(name) <= MAX_FILENAME_LENGTH);
    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    bool found;
    lookup(parent, name, found, ino_out);
    if (found)
        return EXIST;


    ec->create(extent_protocol::T_FILE, ino_out);
    //TODO :set attribute of this file

#ifdef _DEBUG_PART2_A
    extent_protocol::attr a;
#endif

#ifdef _DEBUG_PART2_A
    ec->getattr(ino_out, a);
    assert(a.type == extent_protocol::T_FILE);
    assert(a.size == 0);
#endif


    std::string buf;
    std::string append;

    // Assume the parent exists, or else exit
    if (ec->get(parent, buf) != extent_protocol::OK) {
        debug_log("yc::create ERROR: ec->get(parent,buf) error, will exit\n");
        exit(0);
        return IOERR;
    }

#ifdef _DEBUG_PART2_A
    ec->getattr(parent, a);
    unsigned int parent_original_size = a.size;
    assert(a.type == extent_protocol::T_DIR);
    assert(buf.size() == parent_original_size);
#endif

    struct dir_entry new_dirent;
    new_dirent.inum = ino_out;

    //This type cast has been validated in the beginning of th function
    new_dirent.file_name_length = (unsigned short) strlen(name);

    //avoid integer truncate
    assert(new_dirent.file_name_length == strlen(name));

    //copy the name to the entry
    memcpy(new_dirent.file_name, name, new_dirent.file_name_length);

    //Cast the entry to meet the interface constraint
    append.assign((char *) (&new_dirent), sizeof(struct dir_entry));

    buf += append;

    assert(append.size() == sizeof(struct dir_entry));

    ec->put(parent, buf);

#ifdef _DEBUG_PART2_A
    ec->getattr(parent, a);
    assert(a.size == (parent_original_size + sizeof(struct dir_entry)));
#endif

#ifdef _DEBUG_PART2_A
    std::string __parent;
    inum retrive_inum;
    debug_log("\t\t ~~~~~ RETRIEVE PARENT FOR CHECKING ~~~\t\t\n");
    ec->get(parent, __parent);

    assert(__parent.size() == a.size);

    lookup(parent, name, found, retrive_inum);


    assert(found);

    debug_log("\t\t ~~~~~ END RETRIEVE ~~~~~\t\t\n");
    debug_log("===== CREATE FILE END =====\n\n");

#endif
    return r;

}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out) {
    int r = OK;
    //file name length shouldn't be longer than MAX_FILENAME_LENGTH
    assert(strlen(name) <= MAX_FILENAME_LENGTH);

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    bool found;
    lookup(parent, name, found, ino_out);
    if (found)
        return EXIST;

    std::string buf, append;

    // Assume the parent exists, or else exit
    if (ec->get(parent, buf) != extent_protocol::OK) {
        debug_log("yc::create ERROR: ec->get(parent,buf) error, will exit\n");
        exit(0);
        return IOERR;
    }

    //create an directory, simply call ec->create()
    ec->create(extent_protocol::T_DIR, ino_out);


    struct dir_entry dirent;
    dirent.inum = ino_out;

    //This type cast has been validated in the beginning of th function
    dirent.file_name_length = (unsigned short) strlen(name);

    //copy the name to the entry
    memcpy(dirent.file_name, name, dirent.file_name_length);

    //Cast the entry to meet the interface constraint
    append.assign((char *) (&dirent), sizeof(struct dir_entry));
    buf += append;
    ec->put(parent, buf);
    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out) {

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
#ifdef _DEBUG_PART2_A
    debug_log("yc::lookup: try to find-> {parent:%llu, name:%s}\n", parent, name);
#endif
    int r = OK;
    std::list<dirent> entries;

    //If readdir() fails, the program will exit
    //So error handling is unnecessary here
    readdir(parent, entries);

    std::string string_name;
    while (entries.size() != 0) {
        dirent dir_ent = entries.front();
        entries.pop_front();

#ifdef _DEBUG_PART2_A
        debug_log("yc::lookup: find an entry-> {inum:%llu, name:%s}\n", dir_ent.inum, dir_ent.name.c_str());
#endif

        if (dir_ent.name == string_name.assign(name, strlen(name))) {
            found = true;
            ino_out = dir_ent.inum;
            return r;
        }
    }

    found = false;
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list) {
    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    int r = OK;
    std::string buf;

    //the dir's attribute stored in inode
    extent_protocol::attr attr;




    // Assume the dir exists, or else exit
    if (ec->get(dir, buf) != extent_protocol::OK) {
        debug_log("yc::lookup ERROR: ec->get(parent,buf) error, will exit\n");
        exit(0);
        return IOERR;
    }

    //If the file content can be successsfully retrieved, so does attr
    ec->getattr(dir, attr);

    // dir isn't a directory, exit!
    if (attr.type != extent_protocol::T_DIR) {
        debug_log("yc::lookup ERROR: dir isn't a directory, will exit\n");
        exit(0);
    }

    //Set the access time
    attr.atime = (unsigned int) time(0);
    //TODO::set attribute

    //The content of the directory
    const char *cbuf = buf.c_str();

    //The size of the directory
    unsigned int size = (unsigned int) buf.size();

    //the number of entries the directory contains
    unsigned int n_entries = size / (sizeof(dir_entry));


    //the directory file length must be aligned to sizeof(dir_entry)
    assert(size % sizeof(dir_entry) == 0);

#ifdef  _DEBUG_PART2_A
    debug_log("yc::readdir, try to read dir[%llu], content-length[%lu], n_entries[%u]\n", dir, buf.size(), n_entries);

#endif

    //traverse the entries in the directory
    for (uint32_t i = 0; i < n_entries; i++) {
        struct dir_entry dir_entry;// = ((struct dir_entry *) cbuf)[i];

        memcpy(&dir_entry, cbuf + i * sizeof(struct dir_entry), sizeof(struct dir_entry));

        struct dirent dirent;
        dirent.inum = dir_entry.inum;
        dirent.name.assign(dir_entry.file_name, dir_entry.file_name_length);

#ifdef  _DEBUG_PART2_A
        debug_log("yc::readdir, constructed an entry, inum[%llu], name[", dirent.inum);
        std::cout << dirent.name << "]\n";

#endif

        list.push_back(dirent);
    }

    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data) {
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    std::string content;
    ec->get(ino, content);

    if ((unsigned int) off >= content.size()) {
        data.erase();
        return r;
    }

    data = content.substr(off, size);
    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
                  size_t &bytes_written) {
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    std::string content;
    ec->get(ino, content);
    std::string buf;
    buf.assign(data, size);

    if ((unsigned int) off <= content.size()) {
        content.replace(off, size, buf);
        bytes_written = size;
    } else {
        size_t old_size = content.size();
        content.resize(size + off, '\0');
        content.replace(off, size, buf);
        bytes_written = size + off - old_size;
    }
    ec->put(ino, content);

    return r;
}

int yfs_client::unlink(inum parent, const char *name) {

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    return rmdir(parent, name);
}

int yfs_client::rmdir(inum parent, const char *name) {
    int r = OK;
    std::list<dirent> dir_entries;
    std::string string_name;
    string_name.assign(name, strlen(name));
    bool found = false;

    readdir(parent, dir_entries);
    std::list<dirent>::iterator it = dir_entries.begin();

    for (; it != dir_entries.end(); ++it) {
        if (it->name == string_name) {
            found = true;
            break;
        }
    }

    if (!found)
        return NOENT;

    //erase the entry
    dir_entries.erase(it);

    //buf ready to be written to disk
    std::string buf;


    for (it = dir_entries.begin(); it != dir_entries.end(); ++it) {
        std::string append;
        dir_entry dir_entry_disk;
        dir_entry_disk.inum = it->inum;
        dir_entry_disk.file_name_length = (unsigned short) it->name.size();
        memcpy(dir_entry_disk.file_name, it->name.data(), dir_entry_disk.file_name_length);
        append.assign((char *) (&dir_entry_disk), sizeof(struct dir_entry));
        buf += append;

    }

    ec->put(parent, buf);
    return r;
}

int
yfs_client::readlink(inum ino, std::string &data) {
    int r = OK;
    std::string buf;
    r = ec->get(ino, buf);
    data = buf;

    return r;
}

int
yfs_client::symlink(inum parent, const char *name, const char *link, inum &ino_out) {
    int r = OK;

    std::string parent_content, parent_add;
    r = ec->get(parent, parent_content);
    bool found;
    inum id;
    lookup(parent, name, found, id);
    if (found)
        return EXIST;

    r = ec->create(extent_protocol::T_SYMLINK, ino_out);
    r = ec->put(ino_out, std::string(link));


    struct dir_entry dirent;
    dirent.inum = ino_out;
    dirent.file_name_length = (unsigned short) strlen(name);
    memcpy(dirent.file_name, name, dirent.file_name_length);
    parent_add.assign((char *) (&dirent), sizeof(struct dir_entry));

    parent_content += parent_add;
    ec->put(parent, parent_content);

    return r;
}

