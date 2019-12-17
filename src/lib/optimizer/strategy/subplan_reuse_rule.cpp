#include "subplan_reuse_rule.hpp"

#include <unordered_map>

#include <termcolor/termcolor.hpp>

#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

using namespace termcolor;

namespace {

using namespace opossum;  // NOLINT
using namespace opossum::expression_functional;  // NOLINT

std::unordered_set<std::shared_ptr<LQPColumnExpression>> get_column_expressions(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions) {
  auto column_expressions = std::unordered_set<std::shared_ptr<LQPColumnExpression>>{};
  for (const auto& expression : expressions) {
    visit_expression(expression, [&](const auto& sub_expression) {
      if (const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(sub_expression)) {
        column_expressions.emplace(std::move(column_expression));
      }
      return ExpressionVisitation::VisitArguments;
    });
  }
  return column_expressions;
}

using ColumnReplacementMappings = ExpressionUnorderedMap<std::shared_ptr<LQPColumnExpression>>;

void add_to_column_mapping(const std::shared_ptr<AbstractExpression>& from_expression,
                           const std::shared_ptr<AbstractExpression>& to_expression,
                           ColumnReplacementMappings& mappings) {
  Assert(from_expression->type == to_expression->type, "Expected same type");

  // TODO test that LQPColumnExpressions hidden in an argument (e.g., SUM(x) or AND(x, y)) are also detected
  // TODO test COUNT(*), also nested and some LQP nodes away

  if (const auto from_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(from_expression)) {
    const auto to_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(to_expression);
    mappings.emplace(from_column_expression, to_column_expression);
  } else {
    auto from_expressions_arguments_iter = from_expression->arguments.begin();
    auto from_expressions_arguments_end_iter = from_expression->arguments.end();
    auto to_expressions_arguments_iter = to_expression->arguments.begin();

    DebugAssert(from_expressions_arguments_end_iter - from_expressions_arguments_iter ==
                    to_expression->arguments.end() - to_expressions_arguments_iter,
                "Mismatching number of expression arguments");

    while (from_expressions_arguments_iter != from_expressions_arguments_end_iter) {
      add_to_column_mapping(*from_expressions_arguments_iter, *to_expressions_arguments_iter, mappings);
      ++from_expressions_arguments_iter;
      ++to_expressions_arguments_iter;
    }
  }
}

ColumnReplacementMappings create_column_mapping(const std::shared_ptr<AbstractLQPNode>& from_node, const std::shared_ptr<AbstractLQPNode>& to_node) {
  auto mapping = ColumnReplacementMappings{};

  const auto& from_expressions = from_node->column_expressions();
  const auto& to_expressions = to_node->column_expressions();

  Assert(from_expressions.size() == to_expressions.size(), "Expected same number of expressions");

  for (auto column_id = ColumnID{0}; column_id < from_expressions.size(); ++column_id) {
    add_to_column_mapping(from_expressions[column_id], to_expressions[column_id], mapping);
  }

  // Add COUNT(*)s
  for (const auto& [from, to] : mapping) {
    const auto from_column_expression = std::dynamic_pointer_cast<const LQPColumnExpression>(from);
    const auto to_column_expression = std::dynamic_pointer_cast<const LQPColumnExpression>(to);
    DebugAssert(from_column_expression && to_column_expression, "Expected ColumnReplacementMappings to contain only LQPColumnExpressions");

    const auto& from_column_reference = from_column_expression->column_reference;
    auto adapted_from = LQPColumnReference{from_column_reference.original_node(), INVALID_COLUMN_ID};
    adapted_from.lineage = from_column_reference.lineage;

    const auto& to_column_reference = to_column_expression->column_reference;
    auto adapted_to = LQPColumnReference{to_column_reference.original_node(), INVALID_COLUMN_ID};
    adapted_to.lineage = to_column_reference.lineage;

    mapping.emplace(std::make_shared<LQPColumnExpression>(adapted_from), std::make_shared<LQPColumnExpression>(adapted_to));
  }

  return mapping;
}

// TODO test multiple self joins to force multi-step lineage - e.g., SELECT * FROM (SELECT t1.id FROM id_int_int_int_100 t1 JOIN id_int_int_int_100 t2 ON t1.id + 1 = t2.id) AS s1, id_int_int_int_100 t3 WHERE s1.id + 5 = t3.id

std::optional<LQPInputSide> find_lineage(const LQPColumnExpression& expression, const std::shared_ptr<AbstractLQPNode>& node) {
  const auto& lineage = expression.column_reference.lineage;
  const auto lineage_iter = std::find_if(lineage.begin(), lineage.end(), [&node](const auto& step) {
    DebugAssert(step.first.lock(), "Via node should not have expired");
    return step.first.lock() == node;
  });
  if (lineage_iter == lineage.end()) return std::nullopt;
  return lineage_iter->second;
}

// Yes, taking mapping by value
void apply_column_replacement_mappings_upwards(
    const std::shared_ptr<AbstractLQPNode>& node, ColumnReplacementMappings mapping, const LQPInputSide side) {
  // std::cout << "\n\n\n" << *node << std::endl;
  // std::cout << "Initial mappings:" << std::endl;
  // for (const auto& [from, to] : mapping) { std::cout << "\t" << *from << " => " << *to << std::endl; }

  if (const auto join_node = std::dynamic_pointer_cast<JoinNode>(node)) {
    // std::cout << "Initial mappings adapted by JoinNode:" << std::endl;

    for (const auto& [from, to] : mapping) {
      const auto from_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(from);
      const auto to_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(to);
      DebugAssert(from_column_expression && to_column_expression, "Expected ColumnReplacementMappings to contain only LQPColumnExpressions");

      if (find_lineage(*to_column_expression, join_node)) continue;

      auto adapted_column_reference = to_column_expression->column_reference;
      adapted_column_reference.lineage.emplace_back(join_node, side);

      if (from_column_expression->column_reference == adapted_column_reference) continue;
      mapping[from] = std::make_shared<LQPColumnExpression>(adapted_column_reference);
      // std::cout << "\t" << *from << " => " << *mapping[from] << " (old: " << *to_column_expression << ")" << std::endl;
    }

    const auto& opposite_expressions = join_node->input(side == LQPInputSide::Left ? LQPInputSide::Right : LQPInputSide::Left)->column_expressions();  // TODO opposite_side()
    for (const auto& opposite_column_expression : get_column_expressions(opposite_expressions)) {
      if (find_lineage(*opposite_column_expression, join_node)) continue;

      auto adapted_column_reference = opposite_column_expression->column_reference;
      adapted_column_reference.lineage.emplace_back(join_node, (side == LQPInputSide::Left ? LQPInputSide::Right : LQPInputSide::Left));

      if (opposite_column_expression->column_reference == adapted_column_reference) continue;
      mapping[opposite_column_expression] = std::make_shared<LQPColumnExpression>(adapted_column_reference);
      // std::cout << "\t" << *opposite_column_expression << " => " << *mapping[opposite_column_expression] << " (on opposite side)" << std::endl;
    }

    // This JoinNode might have already been used as a disambiguation node - add corresponding replacements:
    if (join_node->disambiguate) {
      // We must not modify mapping during iteration, thus, we store new mappings here
      auto new_mappings = ColumnReplacementMappings{};
      // std::cout << "Corresponding replacements added by JoinNode:" << std::endl;
      for (const auto& [from, to] : mapping) {
        const auto from_column_expression = std::dynamic_pointer_cast<const LQPColumnExpression>(from);
        const auto to_column_expression = std::dynamic_pointer_cast<const LQPColumnExpression>(to);
        DebugAssert(from_column_expression && to_column_expression, "Expected ColumnReplacementMappings to contain only LQPColumnExpressions");

        for (const auto adapted_side : {LQPInputSide::Left, LQPInputSide::Right}) {
          // std::cout << "\ttest " << *from_column_expression << " => " << *to_column_expression << std::endl;
          const auto existing_lineage_side = find_lineage(*to_column_expression, join_node);

          auto adapted_from_column_reference = from_column_expression->column_reference;
          adapted_from_column_reference.lineage.emplace_back(join_node, adapted_side);
          auto adapted_to_column_reference = to_column_expression->column_reference;
          if (!existing_lineage_side) {
            adapted_to_column_reference.lineage.emplace_back(join_node, adapted_side);
          } else {
            adapted_to_column_reference.lineage.back() = {join_node, *existing_lineage_side};
          }

          if (adapted_from_column_reference == adapted_to_column_reference) {
            // std::cout << "\t\tcont2" << std::endl;
            continue;
          }

          // TODO mapping vs mappings
          const auto adapted_from_column_expression = std::make_shared<LQPColumnExpression>(adapted_from_column_reference);
          const auto adapted_to_column_expression = std::make_shared<LQPColumnExpression>(adapted_to_column_reference);
          new_mappings[adapted_from_column_expression] = adapted_to_column_expression;

          // std::cout << "\t" << *adapted_from_column_expression << " => " << *adapted_to_column_expression << std::endl;
        }
      }
      mapping.merge(new_mappings);
    } else {
      // We need to make sure that ALL column_expressions leaving this join node are disambiguated. We already did this for the opposite side above.
      for (const auto& column_expression : get_column_expressions(join_node->input(side)->column_expressions())) {
        if (mapping.contains(column_expression)) continue;

        auto adapted_column_reference = column_expression->column_reference;
        adapted_column_reference.lineage.emplace_back(join_node, side);

        mapping[column_expression] = std::make_shared<LQPColumnExpression>(adapted_column_reference);
        // std::cout << "\t" << *column_expression << " => " << *mapping[column_expression] << " (newly disambiguated)" << std::endl;
      }

      for (const auto& column_expression : get_column_expressions(join_node->input(side == LQPInputSide::Left ? LQPInputSide::Right : LQPInputSide::Left)->column_expressions())) {
        DebugAssert(mapping.contains(column_expression), "Should have been disambiguated");
      }

    }

    join_node->disambiguate = true;
  }

  // std::cout << "Applied replacements:" << std::endl;
  for (auto& node_expression : node->node_expressions) {
    // std::cout << "\tInput expression: " << *node_expression << std::endl;

    // The inner expressions of node_expression might be used by equivalent expressions in other nodes as well. As such, we need to deep-copy the expression before manipulating it.
    // TODO really?
    auto updated_node_expression = node_expression->deep_copy();
    visit_expression(updated_node_expression, [&](auto& sub_expression) {
      if (mapping.contains(sub_expression)) {
        // std::cout << "\t\tSub expression " << *sub_expression << " => " << *mapping[sub_expression] << std::endl;
        sub_expression = mapping[sub_expression];
        return ExpressionVisitation::DoNotVisitArguments;
      }
      return ExpressionVisitation::VisitArguments;
    });
    node_expression = updated_node_expression;

    // std::cout << "\tOutput expression: " << *node_expression << std::endl;
  }

  // For SemiJoins, overwrite lineage to the only side that can be an output
  if (const auto join_node = std::dynamic_pointer_cast<JoinNode>(node); join_node && (join_node->join_mode == JoinMode::Semi || join_node->join_mode == JoinMode::AntiNullAsFalse || join_node->join_mode == JoinMode::AntiNullAsTrue)) {
    for (auto& [from, to] : mapping) {
      auto new_reference = to->column_reference;
      // TODO should be at back
      for (auto& [via_node, via_side] : new_reference.lineage) {
        if (via_node.lock() != join_node) continue;
        via_side = LQPInputSide::Left;
      }
      if (new_reference == to->column_reference) continue;
      // std::cout << "!!changed " << *to << " to ";
      to = std::make_shared<LQPColumnExpression>(static_cast<const LQPColumnExpression&>(*from).column_reference);
      // std::cout << *to << "\n";
    }
  }

  // TODO If the node does not produce any expressions that are subject to replacement, we can stop here, thus avoiding diamond blowups
  for (const auto& [output, input_side] : node->output_relations()) {
    apply_column_replacement_mappings_upwards(output, mapping, input_side);
  }
}

}  // namespace

// TODO Test the things from the whiteboard

namespace opossum {

void SubplanReuseRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
  Assert(root->type == LQPNodeType::Root, "SubplanReuseRule needs root to hold onto");

  bool found_match = false;
  do {
    LQPNodeUnorderedSet primary_subplans;
    found_match = false;
    visit_lqp(root, [&](const auto& node) {
      // Do one replacement at a time - break here and wait for the next iteration in the do loop
      if (found_match) return LQPVisitation::DoNotVisitInputs;

      // Do not do anything to UnionPosition nodes, as we will be in a world of hell (mismatching StoredTableNodes)
      // TODO(anyone): continue below the union node
      if (node->type == LQPNodeType::Union) {
        const auto& union_node = static_cast<UnionNode&>(*node);
        if (union_node.union_mode == UnionMode::Positions) return LQPVisitation::DoNotVisitInputs;
      }

      const auto [primary_subplan_iter, is_primary_subplan] = primary_subplans.emplace(node);
      if (is_primary_subplan) return LQPVisitation::VisitInputs;

      // We have seen this plan before and can reuse it
      const auto& primary_subplan = *primary_subplan_iter;

      auto mapping = create_column_mapping(node, primary_subplan);

      for (const auto& [output, input_side] : node->output_relations()) {
        apply_column_replacement_mappings_upwards(output, mapping, input_side);
        output->set_input(input_side, primary_subplan);
        // std::cout << yellow << *node << reset << std::endl;
      }

      found_match = true;

      return LQPVisitation::DoNotVisitInputs;
    });

    std::cout << "\n\n\n\n===SubplanReuseRule temp result===" << std::endl;
    std::cout << green << *root << reset << std::endl;
  } while (found_match);
}

}  // namespace opossum
