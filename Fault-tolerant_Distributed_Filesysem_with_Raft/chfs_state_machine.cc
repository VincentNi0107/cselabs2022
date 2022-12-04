#include "chfs_state_machine.h"

chfs_command_raft::chfs_command_raft() {
    // Lab3: Your code here
    res = std::make_shared<result>();
    res->start = std::chrono::system_clock::now();
    res->done = false;
}

chfs_command_raft::chfs_command_raft(const chfs_command_raft &cmd) :
    cmd_tp(cmd.cmd_tp), type(cmd.type),  id(cmd.id), buf(cmd.buf), res(cmd.res) {
    // Lab3: Your code here
}
chfs_command_raft::~chfs_command_raft() {
    // Lab3: Your code here
}

int chfs_command_raft::size() const{ 
    // Lab3: Your code here
    return sizeof(command_type) + sizeof(uint32_t) + sizeof(extent_protocol::extentid_t) + buf.length() + 4;
}

void chfs_command_raft::serialize(char *buf_out, int size) const {
    // Lab3: Your code here
    size = buf.length();
    memcpy(buf_out, &cmd_tp, sizeof(command_type));
    memcpy(buf_out + sizeof(command_type), &type, sizeof(uint32_t));
    memcpy(buf_out + sizeof(command_type) + sizeof(uint32_t), &id, sizeof(extent_protocol::extentid_t));
    memcpy(buf_out + sizeof(command_type) + sizeof(uint32_t) + sizeof(extent_protocol::extentid_t), &size, sizeof(int));
    memcpy(buf_out + sizeof(command_type) + sizeof(uint32_t) + sizeof(extent_protocol::extentid_t) + sizeof(int), buf.c_str(), size);
    return;
}

void chfs_command_raft::deserialize(const char *buf_in, int size) {
    // Lab3: Your code here
    memcpy(&cmd_tp, buf_in, sizeof(command_type));
    memcpy(&type, buf_in + sizeof(command_type), sizeof(uint32_t));
    memcpy(&id, buf_in + sizeof(command_type) + sizeof(uint32_t), sizeof(extent_protocol::extentid_t));
    memcpy(&size, buf_in + sizeof(command_type) + sizeof(uint32_t) + sizeof(extent_protocol::extentid_t), sizeof(int));
    buf.resize(size);
    memcpy(&buf[0], buf_in + sizeof(command_type) + sizeof(uint32_t) + sizeof(extent_protocol::extentid_t) + sizeof(int), size);
    return;
}

marshall &operator<<(marshall &m, const chfs_command_raft &cmd) {
    // Lab3: Your code here
    return m << (int)cmd.cmd_tp << cmd.type << cmd.id << cmd.buf;
}

unmarshall &operator>>(unmarshall &u, chfs_command_raft &cmd) {
    // Lab3: Your code here
    int cmd_tp;
    u >> cmd_tp >> cmd.type >> cmd.id >> cmd.buf;
    cmd.cmd_tp = static_cast<chfs_command_raft::command_type>(cmd_tp);
    return u;
}

void chfs_state_machine::apply_log(raft_command &cmd) {
    chfs_command_raft &chfs_cmd = dynamic_cast<chfs_command_raft &>(cmd);
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx);
    int a;
    switch (chfs_cmd.cmd_tp)
    {
    case chfs_command_raft::CMD_CRT:
    {
        es.create(chfs_cmd.type, chfs_cmd.res->id);
        break;
    }
    case chfs_command_raft::CMD_PUT:
    {
        es.put(chfs_cmd.id, chfs_cmd.buf, a);
        break;
    }
    case chfs_command_raft::CMD_GET:
    {
        es.get(chfs_cmd.id, chfs_cmd.res->buf);
        break;
    }
    case chfs_command_raft::CMD_GETA:
    {
        es.getattr(chfs_cmd.id, chfs_cmd.res->attr);
        break;
    }
    case chfs_command_raft::CMD_RMV:
    {
        es.remove(chfs_cmd.id, a);
        break;
    }
    default:
        break;
    }
    chfs_cmd.res->done = true;
    chfs_cmd.res->cv.notify_all();
    return;
}


