#ifndef persister_h
#define persister_h

#include <fcntl.h>
#include <mutex>
#include <iostream>
#include <fstream>
#include "rpc.h"
#include <unistd.h>
#include <sys/types.h>
#define MAX_LOG_SZ 1024

/*
 * Your code here for Lab2A:
 * Implement class chfs_command, you may need to add command types such as
 * 'create', 'put' here to represent different commands a transaction requires. 
 * 
 * Here are some tips:
 * 1. each transaction in ChFS consists of several chfs_commands.
 * 2. each transaction in ChFS MUST contain a BEGIN command and a COMMIT command.
 * 3. each chfs_commands contains transaction ID, command type, and other information.
 * 4. you can treat a chfs_command as a log entry.
 */
class chfs_command {
public:
    typedef unsigned long long txid_t;
    enum cmd_type {
        CMD_BEGIN = 0,
        CMD_COMMIT,
        CMD_CREATE,
        CMD_PUT,
        CMD_REMOVE

    };

    cmd_type type;
    txid_t id;
    uint64_t inode_id;
    std::string content;

    // constructor
    chfs_command():type(CMD_BEGIN),id(0),inode_id(0),content(""){};

    chfs_command(cmd_type tp, txid_t id, uint64_t inode_id, const std::string& content): type(tp), id(id), inode_id(inode_id), content(content){};
    
    chfs_command(const chfs_command &cmd): type(cmd.type), id(cmd.id), inode_id(cmd.inode_id), content(cmd.content){} ;

    void serialize(char* buf){
        memcpy(buf, &type, sizeof(cmd_type));
        memcpy(buf + sizeof(cmd_type), &id, sizeof(txid_t));
        memcpy(buf + sizeof(cmd_type) + sizeof(txid_t), &inode_id, sizeof(uint64_t));
        int content_size = content.size();
        memcpy(buf + sizeof(cmd_type) + sizeof(txid_t) + sizeof(uint64_t), &content_size, 4);
        memcpy(buf + sizeof(cmd_type) + sizeof(txid_t) + sizeof(uint64_t) + 4, content.c_str(), content_size);
    };

    void deserialize(const char* buf){
        memcpy(&type, buf, sizeof(cmd_type));
        memcpy(&id, buf + sizeof(cmd_type), sizeof(txid_t));
        memcpy(&inode_id, buf + sizeof(cmd_type) + sizeof(txid_t), sizeof(uint64_t));
        int content_size;
        memcpy(&content_size, buf + sizeof(cmd_type) + sizeof(txid_t) + sizeof(uint64_t), 4);
        content.resize(content_size);
        memcpy(&content[0], buf + sizeof(cmd_type) + sizeof(txid_t) + sizeof(uint64_t) + 4, content_size);
    };

    uint32_t size() const {
        uint32_t s = 4 + sizeof(cmd_type) + sizeof(txid_t)+ sizeof(uint64_t) + content.length();
        return s;
    }
};

/*
 * Your code here for Lab2A:
 * Implement class persister. A persister directly interacts with log files.
 * Remember it should not contain any transaction logic, its only job is to 
 * persist and recover data.
 * 
 * P.S. When and how to do checkpoint is up to you. Just keep your logfile size
 *      under MAX_LOG_SZ and checkpoint file size under DISK_SIZE.
 */
template<typename command>
class persister {

public:
    persister(const std::string& file_dir);
    ~persister();

    // persist data into solid binary file
    // You may modify parameters in these functions
    void append_log(command& log);
    void checkpoint();

    // restore data from solid binary file
    // You may modify parameters in these functions
    void restore_logdata();
    void restore_checkpoint();
    void restore_disk();
    std::vector<command> log_entries;

private:
    std::mutex mtx;
    std::string file_dir;
    std::string file_path_checkpoint;
    std::string file_path_logfile;
    std::string file_path_disk;
    int ckpt_fd;
    int log_fd;
    // restored log data
    
};

template<typename command>
persister<command>::persister(const std::string& dir){
    // DO NOT change the file names here
    file_dir = dir;
    file_path_checkpoint = file_dir + "/checkpoint.bin";
    // file_path_logfile = file_dir + "/logdata.bin";
    // log_fd = open(file_path_logfile.c_str(), O_CREAT|O_RDWR, S_IRUSR | S_IWUSR);
    ckpt_fd = open(file_path_checkpoint.c_str(), O_CREAT|O_RDWR, S_IRUSR | S_IWUSR);
}

template<typename command>
persister<command>::~persister() {
    // Your code here for lab2A
    close(ckpt_fd);
}

template<typename command>
void persister<command>::append_log(command& log) {
    // Your code here for lab2A
    mtx.lock();
    char* buf;
    uint32_t size = log.size();
    write(ckpt_fd, &size, sizeof(uint32_t));
    buf = new char [size];
    log.serialize(buf);
    write(ckpt_fd, buf, size);
    mtx.unlock();
}

template<typename command>
void persister<command>::checkpoint() {
    // Your code here for lab2A

}

template<typename command>
void persister<command>::restore_logdata() {
    // Your code here for lab2A

};

template<typename command>
void persister<command>::restore_checkpoint() {
    // Your code here for lab2A

};

template<typename command>
void persister<command>::restore_disk() {
    // Your code here for lab2A
    mtx.lock();
    int n;
    uint32_t size;
    while((n = read(ckpt_fd, &size, sizeof(uint32_t))) == sizeof(uint32_t)){
        char* buf = new char [size];
        if(read(ckpt_fd, buf, size)!= size)
            printf("Read error\n");
        else{
            command cmd;
            cmd.deserialize(buf);
            log_entries.push_back(cmd);
        }
    }
    mtx.unlock();
};

using chfs_persister = persister<chfs_command>;

#endif // persister_h