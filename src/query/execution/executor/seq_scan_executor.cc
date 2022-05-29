#include "query/execution/executor/seq_scan_executor.h"

#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "common/utils.h"
#include "lock/lock_manager.h"
#include "storage/table/table_heap.h"
#include "storage/tuple/tuple.h"
#include "transaction/transaction.h"

namespace naivedb::query {
    void SeqScanExecutor::init() {
        // TODO(Project-1): Implement this method
        //    UNIMPLEMENTED;
        ExecutorContext &ctx = context();
        catalog::Catalog *ctg = ctx.catalog();
        buffer::BufferManager *bm = ctx.buffer_manager();

        txn = ctx.transaction();
        lock_manager_ = ctx.lock_manager();

        table_id_t id = plan_->table_id();
        if (id == INVALID_TABLE_ID) {
            UNREACHABLE; /*always valid id so there can't be reached*/
        }

        catalog::TableInfo tinfo = ctg->get_table_info(id);
        /* init each member */
        table_heap_ = std::make_unique<storage::TableHeap>(bm, tinfo.root_page_id(), ctx.log_manager());
        table_iter_ = table_heap_->begin();


    }

    std::vector<storage::Tuple> SeqScanExecutor::next() {
        // TODO(Project-1): Implement this method
        if (table_iter_ == table_heap_->end()) {
//            if(lock_manager_&&txn)
//            {
//                txn->set_state(transaction::TransactionState::Committed);
//                while(!txn->shared_lock_set().empty())
//                    if (!lock_manager_->unlock(txn, *(txn->shared_lock_set().begin()))) {
//                        throw TransactionAbortException(txn->transaction_id());
//                    }
//            }


            return {};
        } else {
            if(lock_manager_&&txn)
            {
                tuple_id_t tid = table_iter_.tuple_id();
                if(!txn->is_exclusive_locked(tid)&&!txn->is_shared_locked(tid))
                {
                    if (!lock_manager_->lock_shared(txn, tid)) {
                        throw TransactionAbortException(txn->transaction_id());
                    }
                }

            }


            storage::Tuple t = *(table_iter_++);

            return make_vector(std::move(t)); /* move to make it rvalue */
        }
    }
}  // namespace naivedb::query