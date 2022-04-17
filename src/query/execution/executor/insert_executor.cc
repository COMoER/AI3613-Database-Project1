#include "query/execution/executor/insert_executor.h"

#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "storage/table/table_heap.h"
#include "storage/tuple/tuple.h"

namespace naivedb::query {
void InsertExecutor::init() {
    // TODO(Project-1): Implement this method
    //    UNIMPLEMENTED;
    ExecutorContext &ctx = context();
    catalog::Catalog *ctg = ctx.catalog();
    buffer::BufferManager *bm = ctx.buffer_manager();
    table_id_t id = plan_->table_id();
    if (id == INVALID_TABLE_ID) {
        UNREACHABLE;
    }
    catalog::TableInfo tinfo = ctg->get_table_info(id);
    table_heap_ = std::make_unique<storage::TableHeap>(bm, tinfo.root_page_id());
}

std::vector<storage::Tuple> InsertExecutor::next() {
    // TODO(Project-1): Implement this method
    //    UNIMPLEMENTED;
    for (const std::vector<type::Value> &v : plan_->raw_values()) {
        table_heap_->insert_tuple(v);
    }
    return {};
}
}  // namespace naivedb::query