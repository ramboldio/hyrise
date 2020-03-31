#include "dependent_group_by_reduction_rule.hpp"

#include <unordered_map>

#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace {
using namespace opossum;  // NOLINT

/**
 * This function reduces the group-by columns of @param aggregate_node for the inputs @param stored_table_node and
 * @param table_constraint. @param group_by_columns is passed as it already stores the ColumnID's which are used for
 * intersection instead of retrieving them from @param aggregate_node again.
 *
 * @returns   Boolean value denoting whether at the group-by list of @param aggregate_node changed.
 */
bool reduce_group_by_columns_for_constraint(const ExpressionsConstraintDefinition& constraint, const std::vector<FunctionalDependency>& fds, ExpressionUnorderedSet& group_by_columns, AggregateNode& aggregate_node) {
  auto group_by_list_changed = false;
  const auto& constraint_columns = constraint.column_expressions;

  // The constraint's columns have to be part of the group-by list
  if(!std::all_of(constraint_columns.cbegin(), constraint_columns.cend(), [&group_by_columns](std::shared_ptr<AbstractExpression> constraint_col_expr) {
    return group_by_columns.contains(constraint_col_expr);
  })) {
    return false;
  }

  // Find functionally dependent columns
  auto fd_columns = ExpressionUnorderedSet();
  for(const auto& fd : fds) {
    if (std::all_of(constraint_columns.begin(), constraint_columns.end(), [&fd](const auto constraint_column) {
      return fd.first.contains(constraint_column);
    })) {
      fd_columns.insert(fd.second.begin(), fd.second.end());
    }
  }
  if(fd_columns.empty()) return false;

  // Every column that is functionally dependent on the column(s) of the unique constraint gets moved from
  // the group-by list to the aggregate list. For this purpose it is wrapped in an ANY() expression.
  for (const auto& group_by_column : group_by_columns) {
    if (constraint_columns.contains(group_by_column)) {
      // The constraint's columns remain in the group-by list.
      continue;
    }

    if (fd_columns.contains(group_by_column)) {
      // Remove column from group-by list.
      // Further, decrement the aggregate's index which denotes the end of group-by expressions.
      const auto begin_idx_before = aggregate_node.aggregate_expressions_begin_idx;
      aggregate_node.node_expressions.erase(
          std::remove_if(aggregate_node.node_expressions.begin(), aggregate_node.node_expressions.end(),
                         [&](const auto node_expression) {
                           if (*node_expression == *group_by_column) {
                             // Adjust the number of group by expressions.
                             --aggregate_node.aggregate_expressions_begin_idx;
                             group_by_list_changed = true;
                             return true;
                           }
                           return false;
                         }),
          aggregate_node.node_expressions.end());
      Assert(aggregate_node.aggregate_expressions_begin_idx
             < begin_idx_before, "Failed to remove column from group-by list.");

      // Add the ANY() aggregate to the list of aggregate columns.
      const auto aggregate_any_expression = any_(group_by_column);
      aggregate_node.node_expressions.emplace_back(aggregate_any_expression);
    }
  }

  return group_by_list_changed;
}

}  // namespace

namespace opossum {

void DependentGroupByReductionRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root_lqp) const {
  visit_lqp(root_lqp, [&](const auto& node) {
    if (node->type != LQPNodeType::Aggregate) {
      return LQPVisitation::VisitInputs;
    }
    auto& aggregate_node = static_cast<AggregateNode&>(*node);

    // Early exit if there are no constraints
    const auto input_constraints = aggregate_node.left_input()->constraints();
    if(input_constraints->empty()) return LQPVisitation::VisitInputs;

    // --- Preparation ---
    // Store copy of the aggregate's column expressions to later check if this order is the query's final column order
    const auto initial_aggregate_column_expressions = aggregate_node.column_expressions();
    auto group_by_list_changed = false;

    // Collect non-nullable group-by columns
    ExpressionUnorderedSet group_by_columns_non_nullable;
    group_by_columns_non_nullable.reserve(aggregate_node.aggregate_expressions_begin_idx + 1);
    for (auto expression_idx = size_t{0}; expression_idx < aggregate_node.aggregate_expressions_begin_idx;
         ++expression_idx) {
      const auto& expression = aggregate_node.node_expressions[expression_idx];
      // Check that group by columns are not nullable. Unique columns can generally store NULLs while previous
      // operators (e.g., outer joins) might have added NULLs to a primary key column. For now, we take the safe route
      // and ignore all cases where group-by columns are nullable.
      if (expression->is_nullable_on_lqp(aggregate_node)) {
        continue;
      }
      group_by_columns_non_nullable.insert(expression);
    }

    // Sort the constraints by column count to start with the shortest constraint in hope that the shorter
    // one will later form the group-by clause.
    auto input_constraints_sorted = std::vector<ExpressionsConstraintDefinition>{input_constraints->cbegin(), input_constraints->cend()};
    std::sort(input_constraints_sorted.begin(), input_constraints_sorted.end(),
              [](const auto& left, const auto& right) {
                return left.column_expressions.size() < right.column_expressions.size();
    });

    // Fetch functional dependencies
    const auto fds = aggregate_node.functional_dependencies();

    // --- Main ---
    // Try to reduce the group-by list one constraint at a time, starting with the shortest constraint. Multiple
    // constraints might result in different group-by lists (depending on the number of columns of the constraint).
    // We stop as soon as one constraint successfully reduced the group-by list. There is no advantage in a second reduction if the first reduction was successful, because no further columns will be removed. Hence, as soon as one reduction took
    // place, we can ignore the remaining constraints.
    for(size_t constraint_idx{0}; constraint_idx < input_constraints_sorted.size(); ++constraint_idx) {
      const auto& constraint = input_constraints_sorted[constraint_idx];

      group_by_list_changed |= reduce_group_by_columns_for_constraint(constraint, fds, group_by_columns_non_nullable,
          aggregate_node);
      if (group_by_list_changed) break;
    }

    // --- Finish ---
    // In case the initial query plan root returned the same columns in the same column order and was not a projection,
    // it is likely that the result of the current aggregate was either the root itself or only operators followed that
    // do not modify the column order (e.g., sort or limit). In this case, we need to restore the initial column order
    // by adding a projection with the initial column_references since we changed the column order by moving columns
    // from the group-by list to the aggregations.
    const auto root_column_expressions = root_lqp->column_expressions();
    if (group_by_list_changed && initial_aggregate_column_expressions == root_column_expressions &&
        root_lqp->type != LQPNodeType::Projection) {
      const auto projection_node = std::make_shared<ProjectionNode>(root_column_expressions);
      lqp_insert_node(root_lqp, LQPInputSide::Left, projection_node);
    }

    return LQPVisitation::VisitInputs;
  });
}

}  // namespace opossumww
