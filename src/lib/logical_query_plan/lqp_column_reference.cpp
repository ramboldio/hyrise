#include "lqp_column_reference.hpp"

#include "boost/functional/hash.hpp"

#include "abstract_lqp_node.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/static_table_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

LQPColumnReference::LQPColumnReference(const std::shared_ptr<const AbstractLQPNode>& original_node,
                                       ColumnID original_column_id)
    : _original_node(original_node), _original_column_id(original_column_id) {}

std::shared_ptr<const AbstractLQPNode> LQPColumnReference::original_node() const { return _original_node.lock(); }

ColumnID LQPColumnReference::original_column_id() const { return _original_column_id; }

bool LQPColumnReference::operator==(const LQPColumnReference& rhs) const {
  if (_original_column_id != rhs._original_column_id) return false;
  if (lineage.size() != rhs.lineage.size()) return false;
  if (_original_node.owner_before(rhs._original_node)) return false;
  if (rhs._original_node.owner_before(_original_node)) return false;

  for (auto lineage_iter = lineage.begin(), rhs_lineage_iter = rhs.lineage.begin(); lineage_iter != lineage.end();
       ++lineage_iter, ++rhs_lineage_iter) {
    if (lineage_iter->second != rhs_lineage_iter->second) return false;

    // http://open-std.org/JTC1/SC22/WG21/docs/papers/2019/p1901r0.html
    if (lineage_iter->first.owner_before(rhs_lineage_iter->first)) return false;
    if (rhs_lineage_iter->first.owner_before(lineage_iter->first)) return false;
  }

  return true;
}

bool LQPColumnReference::operator!=(const LQPColumnReference& rhs) const { return !(*this == rhs); }

std::string LQPColumnReference::description(AbstractExpression::DescriptionMode mode) const {
  const auto locked_original_node = original_node();
  Assert(locked_original_node, "OriginalNode has expired");

  std::stringstream output;
  if (_original_column_id == INVALID_COLUMN_ID) {
    output << "INVALID_COLUMN_ID";
    return output.str();
  }

  switch (locked_original_node->type) {
    case LQPNodeType::StoredTable: {
      const auto stored_table_node = std::static_pointer_cast<const StoredTableNode>(locked_original_node);
      const auto table = Hyrise::get().storage_manager.get_table(stored_table_node->table_name);

      if (mode == AbstractExpression::DescriptionMode::ColumnName) {
        output << table->column_name(original_column_id());
      } else {
        output << '"' << table->column_name(original_column_id())
           << " from " << original_node();
        for (const auto& step : lineage) {
          output << " via " << step.first.lock() << "(" << (step.second == LQPInputSide::Left ? "left" : "right") << ")";
        }
        output << '"';
      }
    } break;
    case LQPNodeType::Mock: {
      const auto mock_node = std::static_pointer_cast<const MockNode>(locked_original_node);
      output << mock_node->column_definitions().at(_original_column_id).second;
    } break;
    case LQPNodeType::StaticTable: {
      const auto static_table_node = std::static_pointer_cast<const StaticTableNode>(locked_original_node);
      const auto& table = static_table_node->table;
      output << table->column_name(_original_column_id);
    } break;
    default:
      Fail("Unexpected original_node for LQPColumnReference");
  }

  return output.str();
}

std::ostream& operator<<(std::ostream& os, const LQPColumnReference& column_reference) {
  os << column_reference.description(AbstractExpression::DescriptionMode::Detailed);
  return os;
}
}  // namespace opossum

namespace std {

size_t hash<opossum::LQPColumnReference>::operator()(const opossum::LQPColumnReference& column_reference) const {
  // It is important not to combine the pointer of the original_node with the hash code as it was done before #1795.
  // If this pointer is combined with the return hash code, equal LQP nodes that are not identical and that have
  // LQPColumnExpressions or child nodes with LQPColumnExpressions would have different hash codes.

  // We could include `column_reference.original_node()->hash()` in the hash, but since hashing an LQP node has a
  // certain cost, we allow those collisions and rely on operator== to sort it out.

  // DebugAssert(column_reference.original_node(), "OriginalNode has expired");
  // TODO reactivate
  return column_reference.original_column_id();

  // TODO include lineage?
}

size_t hash<const opossum::LQPColumnReference>::operator()(const opossum::LQPColumnReference& column_reference) const {
  // TODO see above
  return column_reference.original_column_id();
}

}  // namespace std
