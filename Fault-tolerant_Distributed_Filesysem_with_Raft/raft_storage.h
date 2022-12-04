#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>

template <typename command>
class raft_storage {
public:
    raft_storage(const std::string &file_dir);
    ~raft_storage();
    // Lab3: Your code here
    void persistMeta(int currentTerm, int voteFor);
    void persistLog(log_entry<command> log, int index);
    void restore(std::vector<log_entry<command>> &logs);
    int currentTerm_;
    int voteFor_;

private:
    std::mutex mtx;
    // Lab3: Your code here
    int meta_fd;
    int log_fd;

};

template <typename command>
raft_storage<command>::raft_storage(const std::string &dir) {
    // Lab3: Your code here    
    meta_fd = open((dir + "/meta.txt").c_str(), O_CREAT|O_RDWR, S_IRUSR | S_IWUSR);
    log_fd = open((dir + "/log.txt").c_str(), O_CREAT|O_RDWR, S_IRUSR | S_IWUSR);
}

template <typename command>
raft_storage<command>::~raft_storage() {
    // Lab3: Your code here
    close(meta_fd);
    close(log_fd);
}

template<typename command>
void raft_storage<command>::persistMeta(int currentTerm, int voteFor){
    mtx.lock();
    lseek(meta_fd, 0, SEEK_SET);
    write(meta_fd, &currentTerm, sizeof(int));
    write(meta_fd, &voteFor, sizeof(int));
    mtx.unlock();
}

template<typename command>
void raft_storage<command>::persistLog(log_entry<command> log, int index){
    mtx.lock();
    write(log_fd, &index, sizeof(int));
    write(log_fd, &log.term, sizeof(int));
    int size = log.cmd.size();
    write(log_fd, &size, sizeof(int));
    char *buf = new char [size];
    log.cmd.serialize(buf, size);
    write(log_fd, buf, size);
    delete buf;
    mtx.unlock();
}

template<typename command>
void raft_storage<command>::restore(std::vector<log_entry<command>> &logs){
    mtx.lock();
    log_entry<command> empty;
    empty.term = 0;
    logs.push_back(empty);
    int n;
    if((n = read(meta_fd, &currentTerm_, sizeof(int))) == sizeof(int)){
        read(meta_fd, &voteFor_, sizeof(int));
    }
    else{
        currentTerm_ = 0;
        voteFor_ = -1;
        mtx.unlock();
        return;
    }
    int index;
    int size;
    int term;
    char *buf;
    while((n = read(log_fd, &index, sizeof(int))) == sizeof(int)){
        logs.resize(index + 1);
        read(log_fd, &term, sizeof(int));
        read(log_fd, &size, sizeof(int));
        buf = new char [size];
        if(read(log_fd, buf, size) != size)
            printf("Read error\n");
        log_entry<command> log;
        log.cmd.deserialize(buf, size);
        log.term = term;
        logs[index] = log;
    }
    mtx.unlock();
}
#endif // raft_storage_h