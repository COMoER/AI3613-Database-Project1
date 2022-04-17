#include "query/execution/executor/hash_join_executor.h"

#include "common/utils.h"
#include "storage/tuple/tuple.h"
#include "type/value.h"

namespace naivedb::query {
void HashJoinExecutor::init() {
    // TODO(Project-1): Implement this method
    //    UNIMPLEMENTED;
    child_at(0)->init();
    child_at(1)->init();

    while (!(right_ = child_at(1)->next()).empty()) {
        type::Value v = plan_->right_key_expr()->evaluate(right_[0], plan_->right_plan()->output_schema());
        hash_table_[v] = std::move(right_[0]);
    }
}

std::vector<storage::Tuple> HashJoinExecutor::next() {
    // TODO(Project-1): Implement this method
    //    UNIMPLEMENTED;
    std::vector<storage::Tuple> left, t;
    while (t.empty()) {
        left = child_at(0)->next();
        if (left.empty())
            return {};
        type::Value v = plan_->left_key_expr()->evaluate(left[0], plan_->left_plan()->output_schema());
        auto target = hash_table_.find(v);
        if (target != hash_table_.end()) {
            std::vector<type::Value> tvalue = left[0].values(plan_->left_plan()->output_schema());
            for (type::Value vt : (*target).second.values(plan_->right_plan()->output_schema())) {
                tvalue.emplace_back(std::move(vt));
            }
            storage::Tuple tuple(tvalue);
            t.emplace_back(std::move(tuple));
        }
    }
    return t;
}
}  // namespace naivedb::query