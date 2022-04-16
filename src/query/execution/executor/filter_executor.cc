#include "query/execution/executor/filter_executor.h"

#include "storage/tuple/tuple.h"
#include "type/value.h"

namespace naivedb::query {
void FilterExecutor::init() {
    // TODO(Project-1): Implement this method
//    UNIMPLEMENTED;
    child_at(0)->init();
}

std::vector<storage::Tuple> FilterExecutor::next() {
    // TODO(Project-1): Implement this method
//    UNIMPLEMENTED;
    if(!plan_->predicate()){
        UNREACHABLE;
    }
    std::vector<storage::Tuple> t;
    std::vector<storage::Tuple> current;

    /* execute the child executor*/
    while (!(current = child_at(0)->next()).empty()) {
        if(plan_->predicate()->evaluate(current[0],child_at(0)->output_schema()).as<bool>())
        {
            t.emplace_back(std::move(current[0]));
        }
    }
    return t;
}
}  // namespace naivedb::query