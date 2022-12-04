#include <iostream>
#include <fcntl.h>
#include <unistd.h> 
int main(){
    int fd = open("./raft_temp/raft_storage_2/log.txt", O_CREAT|O_RDWR, S_IRUSR | S_IWUSR);
    int n;
    int index = 0;
    int term;
    int size;
    int value;
    char buf[4];
    read(fd, &index, sizeof(int));
    std::cout << index;
    while((n = read(fd, &index, sizeof(int))) == sizeof(int)){
        read(fd, &term, sizeof(int));
        read(fd, &size, sizeof(int));
        read(fd, &buf, sizeof(int));
        value = (buf[0] & 0xff) << 24;
        value |= (buf[1] & 0xff) << 16;
        value |= (buf[2] & 0xff) << 8;
        value |= buf[3] & 0xff;
        std::cout << index << '\t' << term << '\t' << size << '\t' << value << '\n';
    }
    return 0;
}