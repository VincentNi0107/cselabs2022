#include "raft_protocol.h"

marshall &operator<<(marshall &m, const request_vote_args &args) {
    // Lab3: Your code here
    return m << args.candidate_id << args.lastLogIndex << args.lastLogTerm << args.term;
}
unmarshall &operator>>(unmarshall &u, request_vote_args &args) {
    // Lab3: Your code here
    return u >> args.candidate_id >> args.lastLogIndex >> args.lastLogTerm >> args.term;
}

marshall &operator<<(marshall &m, const request_vote_reply &reply) {
    // Lab3: Your code here
    return m << reply.term << reply.vote_granted;
}

unmarshall &operator>>(unmarshall &u, request_vote_reply &reply) {
    // Lab3: Your code here
    return u >> reply.term >> reply.vote_granted;
}

marshall &operator<<(marshall &m, const append_entries_reply &args) {
    // Lab3: Your code here
    return m << args.success;
}

unmarshall &operator>>(unmarshall &m, append_entries_reply &args) {
    // Lab3: Your code here
    return m >> args.success;
}

marshall &operator<<(marshall &m, const install_snapshot_args &args) {
    // Lab3: Your code here
    return m;
}

unmarshall &operator>>(unmarshall &u, install_snapshot_args &args) {
    // Lab3: Your code here
    return u;
}

marshall &operator<<(marshall &m, const install_snapshot_reply &reply) {
    // Lab3: Your code here
    return m;
}

unmarshall &operator>>(unmarshall &u, install_snapshot_reply &reply) {
    // Lab3: Your code here
    return u;
}