// chfs client.  implements FS operations using extent and lock server
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "chfs_client.h"
#include "extent_client.h"

/* 
 * Your code here for Lab2A:
 * Here we treat each ChFS operation(especially write operation such as 'create', 
 * 'write' and 'symlink') as a transaction, your job is to use write ahead log 
 * to achive all-or-nothing for these transactions.
 */

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client(extent_dst);
    lc = new lock_client(lock_dst);
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
chfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isfile: %lld is a dir\n", inum);
        return true;
    } 
    return false;
}

int
chfs_client::getfile(inum inum, fileinfo &fin)
{
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
chfs_client::getdir(inum inum, dirinfo &din)
{
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
// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    printf("setattr %016llx size %ld\n",ino, size);
    std::string buf;
    uint64_t tx_id;
    ec->begin(tx_id);
    lc->acquire(ino);
    if (ec->get(ino, buf) != extent_protocol::OK){
        r = IOERR;
        return r;
    }
    buf.resize(size);

    if (ec->put(ino, buf) != extent_protocol::OK){
        r = IOERR;
        return r;
    }
    ec->commit(tx_id);
    lc->release(ino);
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    printf("create in dir %016llx %s\n", parent, name);
    std::string buf;
    bool found;

    uint64_t tx_id;
    lc->acquire(parent);
    if (!isdir(parent)) {
        printf("not a dir\n");
        r = NOENT;
        lc->release(parent);
        return r;
    }

    lookup(parent, name, found, ino_out);
    if(found){
        printf("file %s already exists\n", name);
        r = EXIST;
        lc->release(parent);
        return r;
    }

    ec->begin(tx_id);
    if (ec->create(extent_protocol::T_FILE, ino_out) != extent_protocol::OK){
        r = IOERR;
        lc->release(parent);
        return r;
    }

    if (ec->get(parent, buf) != extent_protocol::OK){
        r = IOERR;
        lc->release(parent);
        return r;
    }

    buf = buf + name +'\0' + filename(ino_out) + '\0';

    if (ec->put(parent, buf) != extent_protocol::OK){
        r = IOERR;
        lc->release(parent);
        return r;
    }
    ec->commit(tx_id);
    lc->release(parent);
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    printf("mkdir %016llx %s\n", parent, name);
    std::string buf;
    bool found;

    uint64_t tx_id;
    ec->begin(tx_id);
    lc->acquire(parent);
    if (!isdir(parent)) {
        printf("not a dir\n");
        r = NOENT;
        lc->release(parent);
        return r;
    }

    lookup(parent, name, found, ino_out);
    if(found){
        printf("file %s already exists\n", name);
        r = EXIST;
        lc->release(parent);
        return r;
    }

    if (ec->create(extent_protocol::T_DIR, ino_out) != extent_protocol::OK){
        r = IOERR;
        lc->release(parent);
        return r;
    }

    if (ec->get(parent, buf) != extent_protocol::OK){
        r = IOERR;
        lc->release(parent);
        return r;
    }
    // std::cout<<"before mkdir: "<<buf<<"buf size: "<<buf.size()<<'\n';
    buf = buf + name +'\0' + filename(ino_out) + '\0';
    // std::cout<<"after mkdir: "<<buf<<"buf size: "<<buf.size()<<'\n';
    if (ec->put(parent, buf) != extent_protocol::OK){
        r = IOERR;
        lc->release(parent);
        return r;
    }
    ec->commit(tx_id);
    lc->release(parent);
    return r;
}

int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    printf("lookup in dir %016llx %s\n", parent, name);
    std::list<dirent> list;
    if (!isdir(parent)) {
        printf("not a dir\n");
        r = NOENT;
        return r;
    }
    readdir(parent, list);
    for (auto it = list.begin(); it != list.end(); ++it){ 
        // printf("lookup find: %s \n",(*it).name.c_str());
        if((*it).name == std::string(name)){
            found = true;
            ino_out = (*it).inum;
            return r;
        }
    }
    found = false;
    return r;
}

int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    printf("readdir in dir %016llx\n", dir);
    std::string buf;
    if (!isdir(dir)) {
        printf("not a dir\n");
        r = NOENT;
        return r;
    }
    if (ec->get(dir, buf) != extent_protocol::OK){
        r = IOERR;
        return r;
    }
    const char *pos = buf.data();
    const char *end = pos + buf.length();
    while(pos < end){
        dirent tmp;
        tmp.name = pos;
        pos += strlen(pos) + 1;
        tmp.inum = n2i(pos);
        pos += strlen(pos) + 1;
        list.push_back(tmp);
    }
    printf("file num: %ld \n", list.size());
   
    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    printf("read %016llx size %ld off %ld\n", ino, size, off);
    if (!isfile(ino)) {
        printf("not a file\n");
        r = NOENT;
        return r;
    }
    std::string buf;
    if (ec->get(ino, buf) != extent_protocol::OK){
        r = IOERR;
        return r;
    }
    if((size_t)off <= buf.length()){
        data = buf.substr(off, size);
    }
    printf("read result: %s", data.c_str());
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    uint64_t tx_id;
    ec->begin(tx_id);
    lc->acquire(ino);
    printf("write %016llx size %ld off %ld string %s\n", ino, size, off, data);
    if (!isfile(ino)) {
        printf("not a file\n");
        r = NOENT;
        lc->release(ino);
        return r;
    }
    std::string buf;
    if (ec->get(ino, buf) != extent_protocol::OK){
        r = IOERR;
        lc->release(ino);
        return r;
    }
    if(off + size > buf.length())
        buf.resize(off + size);
    buf.replace(off, size, data, size);
    if (ec->put(ino, buf) != extent_protocol::OK){
        r = IOERR;
        lc->release(ino);
        return r;
    }
    bytes_written = size;
    ec->commit(tx_id);
    lc->release(ino);
    return r;
}


// Your code here for Lab2A: add logging to ensure atomicity
int chfs_client::unlink(inum parent, const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    std::string buf;
    uint64_t tx_id;
    ec->begin(tx_id);
    lc->acquire(parent);
    if (ec->get(parent, buf) != extent_protocol::OK){
        r = IOERR;
        lc->release(parent);
        return r;
    }
    const char *pos = buf.data();
    const char *end = pos + buf.length();
    bool found = false;
    inum ino; 
    int pos_cnt = 0;
    while(pos < end){
        std::string s_name(pos);
        pos += s_name.length() + 1;
        std::string s_inum(pos);
        pos += s_inum.length() + 1;
        if(s_name.compare(name) == 0){
            found = true;
            ino = n2i(s_inum);
            buf.erase(pos_cnt, s_name.length() + s_inum.length() + 2);
            break;
        }
        pos_cnt += s_name.length() + s_inum.length() + 2;
    }

    if(!found){
        r = NOENT;
        lc->release(parent);
        return r;
    }

    if(ec->remove(ino) != OK){
        r = IOERR;
        lc->release(parent);
        return r;
    }
    if(ec->put(parent, buf) != OK){
        r = IOERR;
        lc->release(parent);
        return r;    
    }

    ec->commit(tx_id);
    lc->release(parent);
    return r;
}

int chfs_client::readlink(inum ino, std::string &data)
{
    int r = OK;
    if (ec->get(ino, data) != extent_protocol::OK){
        r = IOERR;
        return r;
    }
    return r;

}

int chfs_client::symlink(inum parent, const char * link, const char * name, inum &ino_out)
{
    int r = OK;

    std::string buf;
    uint64_t tx_id;
    ec->begin(tx_id); 
    lc->acquire(parent);
    if (ec->get(parent, buf) != extent_protocol::OK){
        r = IOERR;
        lc->release(parent);
        return r;
    }
    bool found;

    lookup(parent, name, found, ino_out);
    if(found){
        printf("file %s already exists\n", name);
        r = EXIST;
        lc->release(parent);
        return r;
    }

    if (ec->create(extent_protocol::T_SYM, ino_out) != extent_protocol::OK){
        r = IOERR;
        lc->release(parent);
        return r;
    }

    if (ec->put(ino_out, link) != extent_protocol::OK){
        r = IOERR;
        lc->release(parent);
        return r;
    }

    buf = buf + name +'\0' + filename(ino_out) + '\0';

    if (ec->put(parent, buf) != extent_protocol::OK){
        r = IOERR;
        lc->release(parent);
        return r;
    }
    ec->commit(tx_id);
    lc->release(parent);
    return r;
}

