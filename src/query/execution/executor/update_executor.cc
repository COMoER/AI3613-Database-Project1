#include "query/execution/executor/update_executor.h"

#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "lock/lock_manager.h"
#include "storage/table/table_heap.h"
#include "storage/tuple/tuple.h"
#include "transaction/transaction.h"
#include "type/value.h"

namespace naivedb::query {
void UpdateExecutor::init() {
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
    table_heap_ = std::make_unique<storage::TableHeap>(bm, tinfo.root_page_id(),ctx.log_manager());
    table_iter_ = table_heap_->begin();
    table_schema_ = tinfo.schema();
}

std::vector<storage::Tuple> UpdateExecutor::next() {
    // TODO(Project-1): Implement this method
    //    UNIMPLEMENTED;
    const Expr *expr = plan_->predicate();
    while (table_iter_ != table_heap_->end()) {
        tuple_id_t id = table_iter_.tuple_id();
        storage::Tuple t = *(table_iter_++);
        if (expr) {
            if (!expr->evaluate(t, table_schema_).as<bool>())
                continue;
        }
        if(lock_manager_&&txn)
        {
            tuple_id_t tid = table_iter_.tuple_id();
            if(txn->is_shared_locked(tid))
            {
                if (!lock_manager_->lock_convert(txn, tid)) {
                    throw TransactionAbortException(txn->transaction_id());
                }
            }
            else if(!txn->is_exclusive_locked(tid))
            {
                if (!lock_manager_->lock_exclusive(txn, tid)) {
                    throw TransactionAbortException(txn->transaction_id());
                }
            }

        }
        for (const std::pair<naivedb::column_id_t, type::Value> &p : plan_->update_columns()) {
            t.set_value_at(table_schema_, p.first, p.second);
        }
        table_heap_->update_tuple(id, t,txn);
    }
//    if(lock_manager_&&txn)
//    {
//        trans
//        txn->set_state(transaction::TransactionState::Committed);
//        while(!txn->exclusive_lock_set().empty())
//            if (!lock_manager_->unlock(txn, *(txn->exclusive_lock_set().begin()))) {
//                throw TransactionAbortException(txn->transaction_id());
//            }
//    }
    return {};
}
}  // namespace naivedb::query