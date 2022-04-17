#include "query/execution/executor/aggregate_executor.h"

#include "common/macros.h"
#include "common/utils.h"
#include "storage/tuple/tuple.h"
#include "type/value.h"

namespace naivedb::query {
void AggregateExecutor::init() {
    // TODO(Project-1): Implement this method
    //    UNIMPLEMENTED;
    child_at(0)->init();
}

std::vector<storage::Tuple> AggregateExecutor::next() {
    // TODO(Project-1): Implement this method
    //    UNIMPLEMENTED;
    std::vector<storage::Tuple> child, t;
    if ((child = child_at(0)->next()).empty())
        return {};
    std::vector<type::Value> tvalue;
    for (auto &expr : plan_->aggregate_exprs()) {
        type::Value v = expr->evaluate_aggregate(child, child_at(0)->output_schema());
        tvalue.emplace_back(std::move(v));
    }
    storage::Tuple tuple(tvalue);
    return make_vector(std::move(tuple));
}
}  // namespace naivedb::query