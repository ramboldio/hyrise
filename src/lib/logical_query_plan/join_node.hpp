#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "types.hpp"

#include "abstract_lqp_node.hpp"
#include "expression/abstract_expression.hpp"
#include "lqp_column_reference.hpp"

namespace opossum {

/**
 * This node type is used to represent any type of Join, including cross products.
 */
class JoinNode : public EnableMakeForLQPNode<JoinNode>, public AbstractLQPNode {
 public:
  // Constructor for Cross Joins. join_mode has to be JoinMode::Cross
  explicit JoinNode(const JoinMode join_mode);

  // Utility constructor that just calls the multi predicated constructor
  JoinNode(const JoinMode join_mode, const std::shared_ptr<AbstractExpression>& join_predicate);

  // Constructor for multi predicated joins
  JoinNode(const JoinMode join_mode, const std::vector<std::shared_ptr<AbstractExpression>>& join_predicates);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;
  const std::vector<std::shared_ptr<AbstractExpression>>& column_expressions() const override;

  // Similar to column_expressions, but returns expressions for left and right side even for semi/anti joins
  std::vector<std::shared_ptr<AbstractExpression>> all_column_expressions() const;   // TODO do we really need this?

  bool is_column_nullable(const ColumnID column_id) const override;

  const std::vector<std::shared_ptr<AbstractExpression>>& join_predicates() const;

  // TODO doc
  std::optional<ColumnID> find_column_id(const AbstractExpression& expression) const override;

  mutable JoinMode join_mode;  // TODO
  mutable bool disambiguate{false};

 protected:
  std::vector<std::shared_ptr<AbstractExpression>> _column_expressions_impl(const bool always_include_right_side) const;

  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;

 private:
  mutable std::vector<std::shared_ptr<AbstractExpression>> _column_expressions;
};

}  // namespace opossum
