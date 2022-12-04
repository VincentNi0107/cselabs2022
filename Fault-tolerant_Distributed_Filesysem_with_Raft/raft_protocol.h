#ifndef raft_protocol_h
#define raft_protocol_h

#include "rpc.h"
#include "raft_state_machine.h"

enum raft_rpc_opcodes {
    op_request_vote = 0x1212,
    op_append_entries = 0x3434,
    op_install_snapshot = 0x5656
};

enum raft_rpc_status {
    OK,
    RETRY,
    RPCERR,
    NOENT,
    IOERR
};

class request_vote_args {
public:
    // Lab3: Your code here
    int candidate_id;
    int lastLogTerm;
    int lastLogIndex;
    int term;
};

marshall &operator<<(marshall &m, const request_vote_args &args);
unmarshall &operator>>(unmarshall &u, request_vote_args &args);

class request_vote_reply {
public:
    // Lab3: Your code here
    int term;
    bool vote_granted;
};

marshall &operator<<(marshall &m, const request_vote_reply &reply);
unmarshall &operator>>(unmarshall &u, request_vote_reply &reply);

template <typename command>
class log_entry {
public:
    // Lab3: Your code here
    command cmd;
    int term;
};

template <typename command>
marshall &operator<<(marshall &m, const log_entry<command> &entry) {
    // Lab3: Your code here
    return m << entry.cmd << entry.term;
}

template <typename command>
unmarshall &operator>>(unmarshall &u, log_entry<command> &entry) {
    // Lab3: Your code here
    return u >> entry.cmd >> entry.term;
}

template <typename command>
class append_entries_args {
public:
    // Your code here
    bool heartbeat;
    int leaderCommitIndex;
    int term;
    int prevLogIndex;
    int prevLogTerm;
    std::vector<log_entry<command>> entries;
};

template <typename command>
marshall &operator<<(marshall &m, const append_entries_args<command> &args) {
    // Lab3: Your code here
    return m << args.heartbeat << args.leaderCommitIndex << args.term << args.prevLogIndex << args.prevLogTerm << args.entries;
}

template <typename command>
unmarshall &operator>>(unmarshall &u, append_entries_args<command> &args) {
    // Lab3: Your code here
    return u >> args.heartbeat >> args.leaderCommitIndex >> args.term >> args.prevLogIndex >> args.prevLogTerm >> args.entries;
}

class append_entries_reply {
public:
    // Lab3: Your code here
    bool success;
};

marshall &operator<<(marshall &m, const append_entries_reply &reply);
unmarshall &operator>>(unmarshall &m, append_entries_reply &reply);

class install_snapshot_args {
public:
    // Lab3: Your code here
};

marshall &operator<<(marshall &m, const install_snapshot_args &args);
unmarshall &operator>>(unmarshall &m, install_snapshot_args &args);

class install_snapshot_reply {
public:
    // Lab3: Your code here
};

marshall &operator<<(marshall &m, const install_snapshot_reply &reply);
unmarshall &operator>>(unmarshall &m, install_snapshot_reply &reply);

#endif // raft_protocol_h