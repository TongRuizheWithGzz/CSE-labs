#ifndef yfs_client_h
#define yfs_client_h

#define MAX_FILENAME_LENGTH 64

#include <string>

#include "lock_protocol.h"
#include "lock_client.h"

//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>
#include <list>

class yfs_client {
    extent_client *ec;
    lock_client *lc;

public:
    typedef unsigned long long inum;
    enum xxstatus {
        OK,
        RPCERR,
        NOENT,
        IOERR,
        EXIST
    };
    typedef int status;

    struct fileinfo {
        unsigned long long size;
        unsigned long atime;
        unsigned long mtime;
        unsigned long ctime;
    };
    struct dirinfo {
        unsigned long atime;
        unsigned long mtime;
        unsigned long ctime;
    };
    struct dirent {
        std::string name;
        yfs_client::inum inum;
    };

    struct dir_entry {
        char file_name[MAX_FILENAME_LENGTH];
        yfs_client::inum inum;
        unsigned short file_name_length;
    };

private:
    static std::string filename(inum);

    static inum n2i(std::string);

    int __list_dir(inum, std::list<dir_entry> &);

    int __lookup_dir(inum, const char *, bool &, inum &);

public:

    yfs_client(std::string, std::string);

    bool issymlink(inum);

    bool isfile(inum);

    bool isdir(inum);

    int getfile(inum, fileinfo &);

    int getdir(inum, dirinfo &);

    int setattr(inum, size_t);

    int lookup(inum, const char *, bool &, inum &);

    int create(inum, const char *, mode_t, inum &);

    int readdir(inum, std::list<dirent> &);

    int write(inum, size_t, off_t, const char *, size_t &);

    int read(inum, size_t, off_t, std::string &);

    int unlink(inum, const char *);

    int mkdir(inum, const char *, mode_t, inum &);

    int rmdir(inum, const char *);

    /** you may need to add symbolic link related methods here.*/
    int symlink(inum parent, const char *name, const char *link, inum &ino_out);

    int readlink(inum ino, std::string &data);


};

#endif
