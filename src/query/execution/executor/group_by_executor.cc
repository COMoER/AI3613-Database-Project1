#include "query/execution/executor/group_by_executor.h"

#include "storage/tuple/tuple.h"

namespace naivedb::query {
void GroupByExecutor::init() {
    // TODO(Project-1): Implement this method
    //    UNIMPLEMENTED;
    child_at(0)->init();
    column_id_t cid = plan_->column_id();
    std::vector<storage::Tuple> child;
    while (!(child = child_at(0)->next()).empty()) {
        type::Value v = child[0].value_at(plan_->child_at(0)->output_schema(), cid);
        if (buckets_.find(v) == buckets_.end()) {
            buckets_[v] = make_vector(std::move(child[0]));
        } else {
            buckets_[v].emplace_back(std::move(child[0]));
        }
    }
    buckets_iter_ = buckets_.begin();
}

std::vector<storage::Tuple> GroupByExecutor::next() {
    // TODO(Project-1): Implement this method
    //    UNIMPLEMENTED;
    if (buckets_iter_ == buckets_.end())
        return {};
    return std::move((buckets_iter_++)->second);
    //    return {};
}
}  // namespace naivedb::query