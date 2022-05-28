#include "lock/lock_manager.h"

#include "common/constants.h"
#include "common/graph.h"
#include "common/macros.h"
#include "common/types.h"
#include "transaction/transaction.h"
#include "transaction/transaction_manager.h"

#include <mutex>
#include <queue>
#include <thread>
#include <unordered_set>

namespace naivedb::lock {
    bool LockManager::lock_shared(transaction::Transaction *txn, tuple_id_t tuple_id) {
        // TODO(Project-2 Part-B): Insert statements that add txn to the wait list and remove it from the wait list.

        // hold the latch
        std::unique_lock latch(latch_);
        // return false immediately if txn is not in growing phase
        if (txn->state() != transaction::TransactionState::Growing) {
            return false;
        }
        auto &lock_list = lock_table_[tuple_id];
        auto txn_id = txn->transaction_id();
        // check double lock
        if (lock_list.shared_locks_.find(txn_id) != lock_list.shared_locks_.end()) {
            return false;
        }
        lock_list.wait_list_.emplace(std::pair<txn_id_t,LockMode>(txn_id,LockMode::Shared));
        // if the exclusive lock is held by another transaction, block this thread
        if (lock_list.exclusive_lock_ != INVALID_TXN_ID) {
            // cv_.wait() must be put in the loop body since the condition may change before it holds the latch again.
            while (lock_list.exclusive_lock_ != INVALID_TXN_ID) {
                // release the latch and block the thread
                lock_list.cv_.wait(latch);
                // now the thread is awakened and the latch is held
                // return false if the transaction is aborted
                if (txn->state() == transaction::TransactionState::Aborted) {
                    lock_list.wait_list_.erase(txn_id);
                    return false;
                }
            }
        }
        lock_list.wait_list_.erase(txn_id);
        // now we can hold the shared lock
        lock_list.shared_locks_.emplace(txn_id);
        // add tuple_id to the txn's shared lock set
        txn->shared_lock_set().emplace(tuple_id);
        return true;
    }

    bool LockManager::lock_exclusive(transaction::Transaction *txn, tuple_id_t tuple_id) {
        // TODO(Project-2 Part-A, B): Implement this method
//    UNIMPLEMENTED;
        // hold the latch, to preserve the inner structure (lock lock)
        std::unique_lock latch(latch_);
        // return false immediately if txn is not in growing phase
        if (txn->state() != transaction::TransactionState::Growing) {
            return false;
        }
        LockManager::LockList &lock_list = lock_table_[tuple_id];
        txn_id_t txn_id = txn->transaction_id();
        // check double lock
        if (lock_list.exclusive_lock_ == txn_id) {
            return false;
        }
        // if the shared_lock_ is held by more than zero transaction or
        // the exclusive lock is held by another transaction,block this thread
        lock_list.wait_list_.emplace(std::pair<txn_id_t,LockMode>(txn_id,LockMode::Exclusive));
        if (not lock_list.shared_locks_.empty() || lock_list.exclusive_lock_ != INVALID_TXN_ID) {
            // cv_.wait() must be put in the loop body since the condition may change before it holds the latch again.
            while (not lock_list.shared_locks_.empty() || lock_list.exclusive_lock_ != INVALID_TXN_ID) {
                // release the latch and block the thread
                lock_list.cv_.wait(latch);
                // now the thread is awakened and the latch is held
                // return false if the transaction is aborted
                if (txn->state() == transaction::TransactionState::Aborted) {
                    lock_list.wait_list_.erase(txn_id);
                    return false;
                }
            }
        }
        lock_list.wait_list_.erase(txn_id);
        // now we can hold the exclusive lock
        lock_list.exclusive_lock_ = txn_id;
        // add tuple_id to the txn's shared lock set
        txn->exclusive_lock_set().emplace(tuple_id);
        return true;
    }

    bool LockManager::lock_convert(transaction::Transaction *txn, tuple_id_t tuple_id) {
        // TODO(Project-2 Part-A, B): Implement this method
//        UNIMPLEMENTED;
        // hold the latch, to preserve the inner structure (lock lock)
        std::unique_lock latch(latch_);
        // return false immediately if txn is not in growing phase
        if (txn->state() != transaction::TransactionState::Growing) {
            return false;
        }
        LockManager::LockList &lock_list = lock_table_[tuple_id];
        txn_id_t txn_id = txn->transaction_id();
        // check the transaction if holding the shared lock
        if (lock_list.shared_locks_.find(txn_id) == lock_list.shared_locks_.end()) {
            return false;
        }
        // check if another transaction wait conversion
        if (lock_list.wait_conversion_) {
            return false;
        }
        // wait conversion
        lock_list.wait_conversion_ = true;
        //JUST LIKE APPLY EXCLUSIVE LOCK
        // if the shared_lock_ is held by more than one transaction(can only contain itself) or
        // the exclusive lock is held by another transaction,block this thread
        lock_list.wait_list_.emplace(std::pair<txn_id_t,LockMode>(txn_id,LockMode::Exclusive));
        if (lock_list.shared_locks_.size() > 1 || lock_list.exclusive_lock_ != INVALID_TXN_ID) {
            // cv_.wait() must be put in the loop body since the condition may change before it holds the latch again.
            while (lock_list.shared_locks_.size() > 1 || lock_list.exclusive_lock_ != INVALID_TXN_ID) {
                // release the latch and block the thread
                lock_list.cv_.wait(latch);
                // now the thread is awakened and the latch is held
                // return false if the transaction is aborted (still reserved)
                if (txn->state() == transaction::TransactionState::Aborted) {
                    lock_list.wait_list_.erase(txn_id);
                    return false;
                }
            }
        }
        //release the shared lock
        lock_list.shared_locks_.erase(txn_id);
        txn->shared_lock_set().erase(tuple_id);

        lock_list.wait_list_.erase(txn_id);
        lock_list.wait_conversion_ = false;
        // now we can hold the exclusive lock
        lock_list.exclusive_lock_ = txn_id;
        // add tuple_id to the txn's shared lock set
        txn->exclusive_lock_set().emplace(tuple_id);
        return true;
    }

    bool LockManager::unlock(transaction::Transaction *txn, tuple_id_t tuple_id) {
        // TODO(Project-2 Part-A): Implement this method
//        UNIMPLEMENTED;
        // hold the latch
        std::unique_lock latch(latch_);
        // return false immediately if txn is not in growing phase
        if (txn->state() == transaction::TransactionState::Growing) {
            return false;
        }
        auto &lock_list = lock_table_[tuple_id];
        auto txn_id = txn->transaction_id();
        // is exclusive lock
        if (lock_list.exclusive_lock_ == txn_id) {
            lock_list.exclusive_lock_ = INVALID_TXN_ID; // release the exclusive lock
            txn->exclusive_lock_set().erase(tuple_id); // delete from the lock set
            lock_list.cv_.notify_all();
        }
            // is shared lock
        else if (lock_list.shared_locks_.find(txn_id) != lock_list.shared_locks_.end()) {
            lock_list.shared_locks_.erase(txn_id);
            txn->shared_lock_set().erase(tuple_id);
            lock_list.cv_.notify_all();
        } else {
            // not hold lock
            return false;
        }
        return true;
    }

    Graph<txn_id_t> LockManager::build_graph() const {
        // TODO(Project-2 Part-B): Implement this method
        // build the wait-for graph according to the lock table
        // no need to hold the latch
//        UNIMPLEMENTED;
        Graph<txn_id_t> graph = Graph<txn_id_t>();

        for (auto &tp:lock_table_) {
            const LockManager::LockList &ll = tp.second;
            for (auto &wait_lock:ll.wait_list_) {
                if (!graph.has_vertex(wait_lock.first)) graph.add_vertex(wait_lock.first);
                switch (wait_lock.second) {
                    case LockMode::Shared: {
                        //if shared lock, only wait the exclusive lock
                        if (ll.exclusive_lock_ != INVALID_TXN_ID) {
                            if (!graph.has_vertex(ll.exclusive_lock_)) graph.add_vertex(ll.exclusive_lock_);
                            graph.add_edge(wait_lock.first, ll.exclusive_lock_);
                        } else {
                            UNREACHABLE;
                        }
                        break;
                    }
                    case LockMode::Exclusive: {
                        //if exclusive lock, wait the exclusive lock and all the shared lock
                        if (ll.exclusive_lock_ != INVALID_TXN_ID) {
                            if (!graph.has_vertex(ll.exclusive_lock_)) graph.add_vertex(ll.exclusive_lock_);
                            graph.add_edge(wait_lock.first, ll.exclusive_lock_);
                        }
                        for (auto &&sl:ll.shared_locks_) {
                            //no self-circle(for the case when the tx waiting for its shared lock
                            // convert to exclusive lock)
                            if (sl == wait_lock.first)continue;
                            if (!graph.has_vertex(sl)) graph.add_vertex(sl);
                            graph.add_edge(wait_lock.first, sl);
                        }
                        break;
                    }
                    default:
                        UNREACHABLE;
                }
            }
        }

        return graph;
    }

    txn_id_t LockManager::has_cycle(const Graph<txn_id_t> &graph) const {
        // TODO(Project-2 Part-B): Implement this method
        // use any algorithm you like
        // no need to hold the latch
//        UNIMPLEMENTED;
        auto &vertices = graph.vertices();
        std::unordered_map<txn_id_t, txn_id_t> visited_pre;
        std::vector<txn_id_t> stack;
        txn_id_t now_v;
        txn_id_t max_v = INVALID_TXN_ID;
        for (auto &v:vertices) {
            if (visited_pre.find(v) != visited_pre.end())
                continue;
            stack.push_back(v);
            visited_pre.emplace(std::pair<txn_id_t, txn_id_t>(v, INVALID_TXN_ID));
            while (!stack.empty()) {
                now_v = stack.back();
                stack.pop_back();
                for (auto &n:graph.outgoing_neighbors(now_v)) {
                    if (visited_pre.find(n) != visited_pre.end()) {
                        max_v = n;
                        while (now_v != n && now_v != INVALID_TXN_ID) {
                            if (now_v > max_v) {
                                max_v = now_v;
                            }
                            now_v = visited_pre[now_v];
                        }
                        if (now_v == INVALID_TXN_ID) {
                            max_v = now_v;
                        } else {
                            break;
                        }
                    } else {
                        stack.push_back(n);
                        visited_pre.emplace(std::pair<txn_id_t, txn_id_t>(n, now_v));
                    }
                }
                if (max_v != INVALID_TXN_ID)break;
            }
            if (max_v != INVALID_TXN_ID)break;
        }
        return max_v;
    }

    void LockManager::deadlock_detection() {
        while (!stop_deadlock_detector_.load()) {
            std::unique_lock latch(latch_);
            // TODO(Project-2 Part-B): Implement this method
            // 1. build the wait-for graph
            Graph<txn_id_t> graph = build_graph();
            // 2. check whether there is a cycle in the graph
            txn_id_t victim = has_cycle(graph);

            if(victim!=INVALID_TXN_ID)
            {
                // 3. if a cycle is found, get the victim transaction
                transaction::Transaction* txn = transaction::TransactionManager::get_transaction(victim);
                // 4. set the state of the victim transaction to aborted and wake it up
                txn->set_state(transaction::TransactionState::Aborted);
                for(auto& lock_list:lock_table_)
                {
                    for(auto& wl:lock_list.second.wait_list_)
                    {
                        if(wl.first==victim)lock_list.second.cv_.notify_all();
                    }
                }
            }
            // unlock the latch before sleeping
            latch.unlock();
            std::this_thread::sleep_for(std::chrono::duration<size_t, std::milli>(DEADLOCK_DETECTION_INTERVAL));
        }
    }
}  // namespace naivedb::lock