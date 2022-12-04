#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

inline long long getCurrentTime() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
}

template <typename state_machine, typename command>
class raft {
    static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
    static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");

    friend class thread_pool;


#define RAFT_LOG(fmt, args...)                                                                                   \
    do {                                                                                                         \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, currentTerm, ##args); \
    } while (0);

public:
    raft(
        rpcs *rpc_server,
        std::vector<rpcc *> rpc_clients,
        int idx,
        raft_storage<command> *storage,
        state_machine *state);
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node.
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped().
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false.
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx; // A big lock to protect the whole data structure
    ThrPool *thread_pool;
    raft_storage<command> *storage; // To persist the raft log
    state_machine *state;           // The state machine that applies the raft log, e.g. a kv store

    rpcs *rpc_server;                // RPC server to recieve and handle the RPC requests
    std::vector<rpcc *> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                       // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int leader_id;

    std::thread *background_election;
    std::thread *background_ping;
    std::thread *background_commit;
    std::thread *background_apply;

    // Your code here:
    int receive_vote_cnt;
    int node_num;
    long long last_received_RPC_time;
    /* ----Persistent state on all server----  */
    int voteFor;
    int currentTerm;
    std::vector<log_entry<command>> logs;
    /* ---- Volatile state on all server----  */
    int commitIndex;
    int lastApplied;
    /* ---- Volatile state on leader----  */
    std::vector<int> nextIndex;
    std::vector<int> matchIndex;
private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply &reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply &reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply &reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command> &arg, const append_entries_reply &reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args &arg, const install_snapshot_reply &reply);

private:
    bool is_stopped();
    int num_nodes() {
        return rpc_clients.size();
    }

    // background workers
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:
};

template <typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    stopped(false),
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    storage(storage),
    state(state),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr),
    role(follower),
    receive_vote_cnt(0),
    commitIndex(0),
    lastApplied(0) {
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here:
    // Do the initialization
    node_num = num_nodes();
    storage->restore(logs);
    currentTerm = storage->currentTerm_;
    voteFor = storage->voteFor_;
    RAFT_LOG("recover, currentTerm: %d, voteFor: %d, log size: %d", currentTerm, voteFor, logs.size());
    last_received_RPC_time = getCurrentTime();
    // log_entry<command> emp;
    // emp.term = 0;
    // logs.push_back(emp);
    nextIndex.resize(node_num);
    matchIndex.resize(node_num);
}

template <typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = currentTerm;
    return role == leader;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Lab3: Your code here

    RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx);
    if (role != leader) return false;
    log_entry<command> new_log;
    new_log.cmd = cmd;
    new_log.term = currentTerm;
    index = logs.size();
    storage->persistLog(new_log, index);
    logs.push_back(new_log);
    term = currentTerm;
    RAFT_LOG("receive new log index: %d, term %d, log size %d", index, term, logs.size());
    return true;
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Lab3: Your code here
    return true;
}

/******************************************************************

                         RPC Related

*******************************************************************/
template <typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply &reply) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx);
    // RAFT_LOG("receive request_vote from %d term %d", args.candidate_id, args.term);
    if(args.term > currentTerm){
        RAFT_LOG("request_vote, update term, become follower");
        storage->persistMeta(args.term, -1);
        currentTerm = args.term;
        voteFor = -1;
        role = follower;
    }
    if(role == follower &&
        args.term == currentTerm &&
        (voteFor == -1 ||voteFor == args.candidate_id) &&
        (args.lastLogTerm > logs.back().term || 
        (args.lastLogTerm == logs.back().term && args.lastLogIndex >= logs.size() - 1)
        )
        ){
        reply.vote_granted = true;
        storage->persistMeta(currentTerm, args.candidate_id);
        voteFor = args.candidate_id;
        last_received_RPC_time = getCurrentTime();
        RAFT_LOG("vote for %d", args.candidate_id);
    }
    else{
        reply.vote_granted = false;
    }
    reply.term = currentTerm;
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx);
    // if(role == candidate) RAFT_LOG("leader or follower handling request vote");
    if(reply.term < currentTerm) RAFT_LOG("reply.term < currentTerm in handling request vote");
    if(reply.term == currentTerm && reply.vote_granted && role == candidate){
        RAFT_LOG("receive vote");
        ++receive_vote_cnt;
    }
    if(reply.term > currentTerm){
        RAFT_LOG("handle_request_vote_reply, become follower");
        storage->persistMeta(reply.term, -1);
        currentTerm = reply.term;
        role = follower;
        voteFor = -1;
        receive_vote_cnt = 0;
    }
    if(role == candidate && receive_vote_cnt > num_nodes() / 2){
        RAFT_LOG("win selection");
        role = leader;
        storage->persistMeta(currentTerm, -1);
        voteFor = -1;
        receive_vote_cnt = 0;
        append_entries_args<command> ping;
        ping.term = currentTerm;
        for(int i = 0; i < num_nodes(); ++i){
            if(i == my_id) continue;
            thread_pool->addObjJob(this, &raft::send_append_entries, i, ping);
        }
        for(int i = 0; i < node_num; ++i){
            matchIndex[i] = 0;
            nextIndex[i] = logs.size();
        }
    }
    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx);
    if(arg.term < currentTerm){
        reply.success = false;
        return 0;
    }
    if(arg.term > currentTerm){
        storage->persistMeta(arg.term, voteFor);
        currentTerm = arg.term;
    }
    if(role != follower){
        role = follower;
        storage->persistMeta(currentTerm, -1);
        voteFor = -1;
        RAFT_LOG("append_entries become follower");
    }
    last_received_RPC_time = getCurrentTime();
    if(arg.heartbeat){
        if(arg.leaderCommitIndex > commitIndex && arg.leaderCommitIndex < logs.size()){
            commitIndex = arg.leaderCommitIndex;
            RAFT_LOG("follower commit to log %d", commitIndex);
        }
        return 0;
    }
    if(arg.prevLogIndex >= logs.size() || logs[arg.prevLogIndex].term != arg.prevLogTerm){
        reply.success = false;
        RAFT_LOG("log mismatch, prevLogIndex: %d , log size: %d , LogTerm %d -> %d", arg.prevLogIndex, logs.size(), arg.prevLogTerm, logs[arg.prevLogIndex].term);
        return 0;
    }
    logs.resize(arg.prevLogIndex + 1);
    for(log_entry<command> log : arg.entries){
        storage->persistLog(log, logs.size());
        logs.push_back(log);
    }
    RAFT_LOG("log match, update log to %d", logs.size() - 1);
    reply.success = true;
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int node, const append_entries_args<command> &arg, const append_entries_reply &reply) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx);
    if(arg.heartbeat) return;
    if(reply.success){
        int new_size = arg.prevLogIndex + arg.entries.size();
        nextIndex[node] = new_size + 1;
        matchIndex[node] = new_size;
        RAFT_LOG("node %d successfully match index %d", node, new_size);
    }
    else{
        nextIndex[node] = arg.prevLogIndex;
    }
    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply) {
    // Lab3: Your code here
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int node, const install_snapshot_args &arg, const install_snapshot_reply &reply) {
    // Lab3: Your code here
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.

    
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here
        mtx.lock();
        if(role == follower && getCurrentTime() - last_received_RPC_time > 300 + 200 * my_id / node_num){
            RAFT_LOG("start_election");
            last_received_RPC_time = getCurrentTime();
            role = candidate;
            ++currentTerm;
            voteFor = my_id;
            storage->persistMeta(currentTerm, voteFor);
            receive_vote_cnt = 1;
            request_vote_args args;
            args.candidate_id = my_id;
            args.term = currentTerm;
            args.lastLogTerm = logs.back().term;
            args.lastLogIndex = logs.size() - 1;
            for(int i = 0; i < node_num; ++i){
                if(i == my_id) continue;
                thread_pool->addObjJob(this, &raft::send_request_vote, i, args);
            }
        }
        if(role == candidate && getCurrentTime() - last_received_RPC_time > 1000){
            role = follower;
            storage->persistMeta(currentTerm, -1);
            voteFor = -1;
        }
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }    
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    
        while (true) {
            if (is_stopped()) return;
            // Lab3: Your code here
            mtx.lock();
            if(role == leader){
                for(int i = 0; i < node_num; ++i){
                    if(i != my_id && matchIndex[i] < logs.size() - 1){
                        append_entries_args<command> args;
                        args.entries = std::vector <log_entry<command>> (logs.begin() + nextIndex[i], logs.end());
                        args.heartbeat = false;
                        args.term = currentTerm;
                        args.prevLogIndex = nextIndex[i] - 1;
                        args.prevLogTerm = logs[args.prevLogIndex].term;
                        RAFT_LOG("send append log to %d, prevLogIndex: %d, prevLogTerm: %d", i, args.prevLogIndex,  args.prevLogTerm);
                        thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
                    }
                }
                int lastCommitCandidateIndex = commitIndex + 1;
                for(; lastCommitCandidateIndex < logs.size(); ++lastCommitCandidateIndex){
                    int commitNodeNum = 1;
                    for(int i = 0; i < node_num; ++i){
                        if(i != my_id && matchIndex[i] >= lastCommitCandidateIndex) ++commitNodeNum;
                    }
                    if(commitNodeNum <= node_num / 2){
                        break;
                    }
                }
                --lastCommitCandidateIndex;
                if(lastCommitCandidateIndex > commitIndex && logs[lastCommitCandidateIndex].term == currentTerm){
                    commitIndex = lastCommitCandidateIndex;
                    RAFT_LOG("leader commit logs to %d", commitIndex);
                }
            }
            mtx.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
        }    
        

    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
        mtx.lock();
        if(lastApplied < commitIndex){
            for(int i = lastApplied + 1; i <= commitIndex; ++i){
                state->apply_log(logs[i].cmd);
            }
            lastApplied = commitIndex;
            RAFT_LOG("apply logs to %d", lastApplied);
        }
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }    
    
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
        if(role == leader){       
            for(int i = 0; i < num_nodes(); ++i){
                if(i == my_id) continue;
                append_entries_args<command> ping;
                ping.term = currentTerm;
                ping.leaderCommitIndex = matchIndex[i] >= commitIndex ? commitIndex : 0;
                thread_pool->addObjJob(this, &raft::send_append_entries, i, ping);
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    return;
}

/******************************************************************

                        Other functions

*******************************************************************/

#endif // raft_h