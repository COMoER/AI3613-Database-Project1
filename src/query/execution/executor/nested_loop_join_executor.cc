#include "query/execution/executor/nested_loop_join_executor.h"

#include "storage/tuple/tuple.h"
#include "type/value.h"

namespace naivedb::query {
void NestedLoopJoinExecutor::init() {
    // TODO(Project-1): Implement this method
//    UNIMPLEMENTED;
    child_at(0)->init();
    child_at(1)->init();
}

std::vector<storage::Tuple> NestedLoopJoinExecutor::next() {
    // TODO(Project-1): Implement this method
    if(left_.empty())left_ = child_at(0)->next();
    std::vector<storage::Tuple> c2, t;
    while(t.empty()) /* execute should yield exactly one possible joined tuple once*/
    {
        c2 = child_at(1)->next();
        if(c2.empty())
        {
            child_at(1)->init();/*back to first*/
            c2 = child_at(1)->next();
            if(c2.empty())return {}; /* size of right is zero*/
            else{
                left_ = child_at(0)->next();/*next left*/
                if(left_.empty())return {};
            }
        }
        std::vector<storage::Tuple>& c1 = left_;
        if(plan_->predicate())
        {
            if(!(plan_->predicate()->evaluate_join(c1[0],c2[0],
                                                   plan_->left_plan()->output_schema(),
                                                   plan_->right_plan()->output_schema()).as<bool>()))
                continue;
        }
        std::vector<type::Value> tvalue = c1[0].values(plan_->left_plan()->output_schema());
        for(type::Value v:c2[0].values(plan_->right_plan()->output_schema()))
        {
            tvalue.emplace_back(std::move(v));
        }
        storage::Tuple tuple(tvalue);
        t.emplace_back(std::move(tuple));
    }
    return t;

}
}  // namespace naivedb::query