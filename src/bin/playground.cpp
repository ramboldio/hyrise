#include <iostream>
#include <synthetic_table_generator.hpp>

#include "types.hpp"

using namespace opossum;  // NOLINT

int main() {
  auto column_specs = std::vector<ColumnSpecification>();
  column_specs.emplace_back(ColumnSpecification(ColumnDataDistribution::make_uniform_config(0.0, 1000.0), DataType::Null, EncodingType::Unencoded, "yo"));
  const auto table = SyntheticTableGenerator().generate_table(column_specs, 10, 10, UseMvcc::Yes);

  return 0;
}
