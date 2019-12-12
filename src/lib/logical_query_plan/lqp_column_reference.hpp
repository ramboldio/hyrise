#pragma once

#include <memory>

#include "abstract_lqp_node.hpp"
#include "expression/abstract_expression.hpp"
#include "types.hpp"

namespace opossum {

class JoinNode;

// TODO update comment: LQPColumnReferences DO NOT point to aggregate / projection nodes, add lineage
/**
 * Used for identifying a Column in an LQP by the Node and the ColumnID in that node in which it was created.
 * Currently this happens in StoredTableNode (which creates all of its columns), AggregateNode (which creates all
 * aggregate columns) and ProjectionNode (which creates all columns containing arithmetics)
 */
class LQPColumnReference final {
 public:
  LQPColumnReference() = default;
  LQPColumnReference(const std::shared_ptr<const AbstractLQPNode>& original_node, ColumnID original_column_id);

  std::shared_ptr<const AbstractLQPNode> original_node() const;
  ColumnID original_column_id() const;

  bool operator==(const LQPColumnReference& rhs) const;
  bool operator!=(const LQPColumnReference& rhs) const;

  std::string description() const;


  // TODO doc that lineage is not added for every join, only if ambiguity exists - even for the same original_node / original_column_id pair, ambiguity does not exist if one side already has lineage information
  std::vector<std::pair<std::weak_ptr<const AbstractLQPNode>, LQPInputSide>> lineage{};

  // Indirection through AbstractExpression will be gone with #1893
  std::string description(AbstractExpression::DescriptionMode mode) const;

 private:
  // Needs to be weak since Nodes can hold ColumnReferences referring to themselves
  std::weak_ptr<const AbstractLQPNode> _original_node;
  ColumnID _original_column_id{INVALID_COLUMN_ID};
};

std::ostream& operator<<(std::ostream& os, const LQPColumnReference& column_reference);
}  // namespace opossum

namespace std {

template <>
struct hash<opossum::LQPColumnReference> {
  size_t operator()(const opossum::LQPColumnReference& column_reference) const;
};

template <>
struct hash<const opossum::LQPColumnReference> {
  size_t operator()(const opossum::LQPColumnReference& column_reference) const;
};

}  // namespace std
