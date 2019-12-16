#include "operator_join_predicate.hpp"

#include "expression/abstract_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"

namespace opossum {

std::optional<OperatorJoinPredicate> OperatorJoinPredicate::from_expression(const AbstractExpression& predicate,
                                                                            const AbstractLQPNode& left_input,
                                                                            const AbstractLQPNode& right_input) {  // TODO is this still used?
  const auto* abstract_predicate_expression = dynamic_cast<const AbstractPredicateExpression*>(&predicate);
  if (!abstract_predicate_expression) return std::nullopt;

  switch (abstract_predicate_expression->predicate_condition) {
    case PredicateCondition::Equals:
    case PredicateCondition::NotEquals:
    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals:
    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals:
      break;
    default:
      return std::nullopt;
  }

  Assert(abstract_predicate_expression->arguments.size() == 2u, "Expected two arguments");

  const auto left_in_left = left_input.find_column_id(*abstract_predicate_expression->arguments[0]);
  const auto left_in_right = right_input.find_column_id(*abstract_predicate_expression->arguments[0]);
  const auto right_in_left = left_input.find_column_id(*abstract_predicate_expression->arguments[1]);
  const auto right_in_right = right_input.find_column_id(*abstract_predicate_expression->arguments[1]);

  // std::cout << "left_in_left: " << (left_in_left ? *left_in_left : ColumnID{666}) << std::endl;
  // std::cout << "left_in_right: " << (left_in_right ? *left_in_right : ColumnID{666}) << std::endl;
  // std::cout << "right_in_left: " << (right_in_left ? *right_in_left : ColumnID{666}) << std::endl;
  // std::cout << "right_in_right: " << (right_in_right ? *right_in_right : ColumnID{666}) << std::endl;

  auto predicate_condition = abstract_predicate_expression->predicate_condition;

  if (left_in_left && right_in_right) {
    return OperatorJoinPredicate{{*left_in_left, *right_in_right}, predicate_condition};
  }

  if (right_in_left && left_in_right) {
    predicate_condition = flip_predicate_condition(predicate_condition);
    return OperatorJoinPredicate{{*right_in_left, *left_in_right}, predicate_condition};
  }

  return std::nullopt;
}

std::optional<OperatorJoinPredicate> OperatorJoinPredicate::from_expression(const AbstractExpression& predicate,
                                                                            const AbstractLQPNode& node) {
  // std::cout << "start OJP " << predicate << " on " << node << std::endl;
  const auto* abstract_predicate_expression = dynamic_cast<const AbstractPredicateExpression*>(&predicate);
  if (!abstract_predicate_expression) return std::nullopt;

  DebugAssert(abstract_predicate_expression->arguments.size() == 2u, "Expected two arguments");
  DebugAssert(node.type == LQPNodeType::Join, "Expected JoinNode");

  const auto& join_node = static_cast<const JoinNode&>(node);
  if (!join_node.disambiguate) {
    // std::cout << "no disambiguation" << std::endl;
    return from_expression(predicate, *join_node.left_input(), *join_node.right_input());
  }

  // TODO dedup
  const auto disambiguate_inplace = [](auto& expression, const auto& Xjoin_node) {
    std::optional<LQPInputSide> disambiguated_input_side;
    auto disambiguation_failed = false;

    visit_expression(expression, [&](auto& sub_expression) {
      if (disambiguation_failed) return ExpressionVisitation::DoNotVisitArguments;

      if (const auto column_expression = dynamic_cast<LQPColumnExpression*>(&*sub_expression)) {
        auto column_reference = column_expression->column_reference;
        if (column_reference.lineage.empty()) return ExpressionVisitation::VisitArguments;

        const auto last_lineage_step = column_reference.lineage.back();
        if (&*last_lineage_step.first.lock() != &Xjoin_node) return ExpressionVisitation::VisitArguments;

        if (disambiguated_input_side && *disambiguated_input_side == last_lineage_step.second) {
          // failed resolving
          disambiguation_failed = true;
          disambiguated_input_side = std::nullopt;
          return ExpressionVisitation::DoNotVisitArguments;
        }

        disambiguated_input_side = last_lineage_step.second;

        // Remove Xjoin_node from the lineage information of sub_expression
        column_reference.lineage.pop_back();
        sub_expression = std::make_shared<LQPColumnExpression>(column_reference);

        // visit_expression ends here
      }
      return ExpressionVisitation::VisitArguments;
    });

    return disambiguated_input_side;
  };

  auto first_argument = abstract_predicate_expression->arguments[0]->deep_copy();
  auto second_argument = abstract_predicate_expression->arguments[1]->deep_copy();

  auto first_argument_side = disambiguate_inplace(first_argument, join_node);
  auto second_argument_side = disambiguate_inplace(second_argument, join_node);

  // if (first_argument_side) std::cout << "first_argument_side: " << (first_argument_side == LQPInputSide::Left ? "left" : "right") << std::endl; else std::cout << "first_argument_side unknown" << std::endl;
  // if (first_argument_side) std::cout << "second_argument_side: " << (second_argument_side == LQPInputSide::Left ? "left" : "right") << std::endl; else std::cout << "second_argument_side unknown" << std::endl;

  DebugAssert(first_argument_side && second_argument_side, "Join predicate is partially missing disambiguation information");
  DebugAssert(*first_argument_side != *second_argument_side, "Join predicate was corrupted");

  auto first_arg_column_id = join_node.input(*first_argument_side)->find_column_id(*first_argument);
  auto second_arg_column_id = join_node.input(*second_argument_side)->find_column_id(*second_argument);

  // std::cout << "from_expression " << *abstract_predicate_expression << std::endl;
  // std::cout << "\tfirst" << (first_arg_column_id ? " " : " not ") << "found" << std::endl;
  // std::cout << "\tsecond" << (second_arg_column_id ? " " : " not ") << "found" << std::endl;

  if (!first_arg_column_id || !second_arg_column_id) return std::nullopt;

  // const auto num_left_column_expressions =
  //     static_cast<ColumnID::base_type>(join_node.left_input()->column_expressions().size());

  // if (*first_argument_side == LQPInputSide::Right) *first_arg_column_id += num_left_column_expressions;
  // if (*second_argument_side == LQPInputSide::Right) *second_arg_column_id += num_left_column_expressions;

  auto predicate_condition = abstract_predicate_expression->predicate_condition;

  if (*first_argument_side == LQPInputSide::Left) {
    return OperatorJoinPredicate{
        {*first_arg_column_id,
         *second_arg_column_id},
        predicate_condition};
  } else {
    predicate_condition = flip_predicate_condition(predicate_condition);
    return OperatorJoinPredicate{
        {*second_arg_column_id,
         *first_arg_column_id},
        predicate_condition};
  }
}

OperatorJoinPredicate::OperatorJoinPredicate(const ColumnIDPair& column_ids,
                                             const PredicateCondition predicate_condition)
    : column_ids(column_ids), predicate_condition(predicate_condition) {}

void OperatorJoinPredicate::flip() {
  std::swap(column_ids.first, column_ids.second);
  predicate_condition = flip_predicate_condition(predicate_condition);
}

bool operator<(const OperatorJoinPredicate& l, const OperatorJoinPredicate& r) {
  return std::tie(l.column_ids, l.predicate_condition) < std::tie(r.column_ids, r.predicate_condition);
}

bool operator==(const OperatorJoinPredicate& l, const OperatorJoinPredicate& r) {
  return std::tie(l.column_ids, l.predicate_condition) == std::tie(r.column_ids, r.predicate_condition);
}

}  // namespace opossum
