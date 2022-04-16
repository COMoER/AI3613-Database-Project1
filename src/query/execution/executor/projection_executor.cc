#include "query/execution/executor/projection_executor.h"

#include "query/expr/expr.h"
#include "storage/tuple/tuple.h"
#include "type/value.h"

namespace naivedb::query {
void ProjectionExecutor::init() {
    // TODO(Project-1): Implement this method
//    UNIMPLEMENTED;
    child_at(0)->init();
}

std::vector<storage::Tuple> ProjectionExecutor::next() {
    // TODO(Project-1): Implement this method
    std::vector<storage::Tuple> t;
    std::vector<storage::Tuple> current;
    const std::vector<std::unique_ptr<const Expr>> & vexpr = plan_->project_exprs();

    /* execute the child executor*/

    while (!(current = child_at(0)->next()).empty()) {
        std::vector<type::Value> tvalue;
        for(const std::unique_ptr<const Expr>& expr_pt:vexpr) {
            type::Value v = expr_pt->evaluate(current[0], child_at(0)->output_schema());
            tvalue.emplace_back(std::move(v));
        }
        storage::Tuple tuple(tvalue);
        t.emplace_back(std::move(tuple));
    }
    return t;
}
}  // namespace naivedb::query