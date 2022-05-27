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

    table_id_t id = plan_->table_id();
    if (id == INVALID_TABLE_ID) {
        UNREACHABLE; /*always valid id so there can't be reached*/
    }

    catalog::TableInfo tinfo = ctg->get_table_info(id);
    /* init each member */
    table_heap_ = std::make_unique<storage::TableHeap>(bm, tinfo.root_page_id());
    table_iter_ = table_heap_->begin();
}

std::vector<storage::Tuple> SeqScanExecutor::next() {
    // TODO(Project-1): Implement this method
    if (table_iter_ == table_heap_->end())
        return {};
    else {
        storage::Tuple t = *(table_iter_++);
        return make_vector(std::move(t)); /* move to make it rvalue */
    }
}
}  // namespace naivedb::query