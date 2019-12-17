#include "join_node.hpp"

#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "constant_mappings.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "operators/operator_join_predicate.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

JoinNode::JoinNode(const JoinMode join_mode) : AbstractLQPNode(LQPNodeType::Join), join_mode(join_mode) {
  Assert(join_mode == JoinMode::Cross, "Only Cross Joins can be constructed without predicate");
}

JoinNode::JoinNode(const JoinMode join_mode, const std::shared_ptr<AbstractExpression>& join_predicate)
    : JoinNode(join_mode, std::vector<std::shared_ptr<AbstractExpression>>{join_predicate}) {}

JoinNode::JoinNode(const JoinMode join_mode, const std::vector<std::shared_ptr<AbstractExpression>>& join_predicates)
    : AbstractLQPNode(LQPNodeType::Join, join_predicates), join_mode(join_mode) {
  Assert(join_mode != JoinMode::Cross, "Cross Joins take no predicate");
  Assert(!join_predicates.empty(), "Non-Cross Joins require predicates");
}

std::string JoinNode::description(const DescriptionMode mode) const {
  const auto expression_mode = _expression_description_mode(mode);

  std::stringstream stream;
  stream << "[Join] Mode: " << join_mode;

  for (const auto& predicate : join_predicates()) {
    stream << " [" << predicate->description(expression_mode) << "]";
  }

  if (disambiguate) stream << " - with lineage";

  return stream.str();
}

const std::vector<std::shared_ptr<AbstractExpression>>& JoinNode::column_expressions() const {
  _column_expressions = _column_expressions_impl(false);
  return _column_expressions;
}

std::vector<std::shared_ptr<AbstractExpression>> JoinNode::all_column_expressions() const {
  return _column_expressions_impl(true);
}

std::vector<std::shared_ptr<AbstractExpression>> JoinNode::_column_expressions_impl(const bool always_include_right_side) const {
  Assert(left_input() && right_input(), "Both inputs need to be set to determine a JoinNode's output expressions");

  std::vector<std::shared_ptr<AbstractExpression>> result;

  /**
   * Update the JoinNode's output expressions every time they are requested. An overhead, but keeps the LQP code simple.
   * Previously, we propagated _input_changed() calls through the LQP every time a node changed and that required a lot
   * of feeble code.
   */

  const auto& left_expressions = left_input()->column_expressions();
  const auto& right_expressions = right_input()->column_expressions();

  const auto output_both_inputs = always_include_right_side ||
      (join_mode != JoinMode::Semi && join_mode != JoinMode::AntiNullAsTrue && join_mode != JoinMode::AntiNullAsFalse);

  if (output_both_inputs) {
    result.resize(left_expressions.size() + right_expressions.size());

    auto right_begin = std::copy(left_expressions.begin(), left_expressions.end(), result.begin());
    std::copy(right_expressions.begin(), right_expressions.end(), right_begin);
  } else {
    result = left_expressions;
  }

  if (!disambiguate) return result;  // TODO rename to lineage?

  // TODO Doc that we decided against only adding lineage to ambiguous nodes as disambiguation on a previous node would otherwise affect what is considered ambiguous
  // TODO remove "ambiguous" terminology
  // TODO Test join with combination of ambiguous and non-ambiguous expressions

  for (auto left_expression_iter = result.begin();
       left_expression_iter != result.begin() + left_expressions.size(); ++left_expression_iter) {
    auto expression_copy = (*left_expression_iter)->deep_copy();
    auto replacement_occured = false;
    const auto lambda = [&](auto& sub_expression) {
      if (const auto column_expression = dynamic_cast<LQPColumnExpression*>(&*sub_expression)) {
        auto disambiguated_column_reference = column_expression->column_reference;
        disambiguated_column_reference.lineage.emplace_back(shared_from_this(), LQPInputSide::Left);
        auto disambiguated_expression = std::make_shared<LQPColumnExpression>(disambiguated_column_reference);
        sub_expression = disambiguated_expression;

        replacement_occured = true;
      }
      return ExpressionVisitation::VisitArguments;
    };
    visit_expression<std::remove_reference_t<decltype(expression_copy)>, decltype(lambda)>(expression_copy, lambda);
    if (replacement_occured) *left_expression_iter = expression_copy;
  }

  // Will not be executed for semi / anti joins
  for (auto right_expression_iter = result.begin() + left_expressions.size();
       right_expression_iter != result.end(); ++right_expression_iter) {
    auto expression_copy = (*right_expression_iter)->deep_copy();
    auto replacement_occured = false;
    const auto lambda = [&](auto& sub_expression) {
      if (const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(sub_expression)) {
        auto disambiguated_column_reference = column_expression->column_reference;
        disambiguated_column_reference.lineage.emplace_back(shared_from_this(), LQPInputSide::Right);
        auto disambiguated_expression = std::make_shared<LQPColumnExpression>(disambiguated_column_reference);
        sub_expression = disambiguated_expression;

        replacement_occured = true;
      }
      return ExpressionVisitation::VisitArguments;
    };
    visit_expression<std::remove_reference_t<decltype(expression_copy)>, decltype(lambda)>(expression_copy, lambda);
    if (replacement_occured) *right_expression_iter = expression_copy;
  }

  return result;
}

bool JoinNode::is_column_nullable(const ColumnID column_id) const {
  Assert(left_input() && right_input(), "Need both inputs to determine nullability");

  const auto left_input_column_count = left_input()->column_expressions().size();
  const auto column_is_from_left_input = column_id < left_input_column_count;

  if (join_mode == JoinMode::Left && !column_is_from_left_input) {
    return true;
  }

  if (join_mode == JoinMode::Right && column_is_from_left_input) {
    return true;
  }

  if (join_mode == JoinMode::FullOuter) {
    return true;
  }

  if (column_is_from_left_input) {
    return left_input()->is_column_nullable(column_id);
  } else {
    ColumnID right_column_id =
        static_cast<ColumnID>(column_id - static_cast<ColumnID::base_type>(left_input_column_count));
    return right_input()->is_column_nullable(right_column_id);
  }
}

const std::vector<std::shared_ptr<AbstractExpression>>& JoinNode::join_predicates() const { return node_expressions; }

std::optional<ColumnID> JoinNode::find_column_id(const AbstractExpression& expression) const {  // TODO is this reeeeeally necessary?
  std::cout << "JoinNode::find_column_id(" << expression << " on " << this << ")\n";
  // std::optional<ColumnID> column_id_on_left;
  // std::optional<ColumnID> column_id_on_right;

  if (!disambiguate) return AbstractLQPNode::find_column_id(expression);

  // We might need to disambiguate the expression using the lineage information in the LQPColumnReferences.
  std::optional<LQPInputSide> disambiguated_input_side;
  auto disambiguation_failed = false;
  auto disambiguated_expression = expression.deep_copy();

  visit_expression(disambiguated_expression, [&](auto& sub_expression) {
    if (const auto column_expression = dynamic_cast<LQPColumnExpression*>(&*sub_expression)) {
      auto column_reference = column_expression->column_reference;
      if (column_reference.lineage.empty()) return ExpressionVisitation::VisitArguments;

      const auto last_lineage_step = column_reference.lineage.back();
      if (&*last_lineage_step.first.lock() != this) return ExpressionVisitation::VisitArguments;

      if (disambiguated_input_side && *disambiguated_input_side == last_lineage_step.second) {
        // failed resolving
        disambiguation_failed = true;
        disambiguated_input_side = std::nullopt;
        return ExpressionVisitation::DoNotVisitArguments;
      }

      disambiguated_input_side = last_lineage_step.second;

      // Remove this node from the lineage information of sub_expression
      column_reference.lineage.pop_back();
      sub_expression = std::make_shared<LQPColumnExpression>(column_reference);

      // visit_expression ends here
    }
    return ExpressionVisitation::VisitArguments;
  });

  if (!disambiguated_input_side || disambiguation_failed) {
    return std::nullopt;
  }

  auto column_id = input(*disambiguated_input_side)->find_column_id(*disambiguated_expression);
  if (!column_id) return std::nullopt;

  if (*disambiguated_input_side == LQPInputSide::Right) {
    *column_id += left_input()->column_expressions().size();
  }
  return column_id;
}

//   // TODO pseudo-ambiguity could also come from literal columns on both sides. Test that those also work

size_t JoinNode::_on_shallow_hash() const { return boost::hash_value(join_mode); }

std::shared_ptr<AbstractLQPNode> JoinNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  if (!join_predicates().empty()) {
    const auto copy = JoinNode::make(join_mode, join_predicates());
    copy->disambiguate = disambiguate;

    // Join predicates may self-reference the join as part of their LQPColumnReference's lineage. We need to first
    // create the JoinNode so that we have its address, then we can update the lineage information.
    node_mapping.emplace(shared_from_this(), copy);
    for (auto& join_predicate : copy->node_expressions) {
      join_predicate = expression_copy_and_adapt_to_different_lqp(*join_predicate, node_mapping);
    }
    return copy;
  } else {
    const auto copy = JoinNode::make(join_mode);
    copy->disambiguate = disambiguate;
    return copy;
  }
}

bool JoinNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& join_node = static_cast<const JoinNode&>(rhs);
  if (join_mode != join_node.join_mode) return false;
  if (disambiguate != join_node.disambiguate) return false;
  return expressions_equal_to_expressions_in_different_lqp(join_predicates(), join_node.join_predicates(),
                                                           node_mapping);
}

}  // namespace opossum
