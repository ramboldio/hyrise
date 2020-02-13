#include "storage_manager.hpp"

#include <boost/container/small_vector.hpp>
#include <fstream>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>
#include <utility>
#include <vector>

#include "hyrise.hpp"
#include "import_export/file_type.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "operators/export.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/print.hpp"
#include "scheduler/job_task.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/segment_iterate.hpp"
#include "utils/assert.hpp"
#include "utils/meta_table_manager.hpp"
#include "utils/timer.hpp"

namespace opossum {

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  Assert(_tables.find(name) == _tables.end(), "A table with the name " + name + " already exists");
  Assert(_views.find(name) == _views.end(), "Cannot add table " + name + " - a view with the same name already exists");

  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); chunk_id++) {
    // We currently assume that all tables stored in the StorageManager are mutable and, as such, have MVCC data. This
    // way, we do not need to check query plans if they try to update immutable tables. However, this is not a hard
    // limitation and might be changed into more fine-grained assertions if the need arises.
    Assert(table->get_chunk(chunk_id)->has_mvcc_data(), "Table must have MVCC data.");
  }

  table->set_table_statistics(TableStatistics::from_table(*table));
  _tables.emplace(name, std::move(table));
}

void StorageManager::drop_table(const std::string& name) {
  const auto num_deleted = _tables.erase(name);
  Assert(num_deleted == 1, "Error deleting table " + name + ": _erase() returned " + std::to_string(num_deleted) + ".");
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  if (MetaTableManager::is_meta_table_name(name)) {
    return Hyrise::get().meta_table_manager.generate_table(name.substr(MetaTableManager::META_PREFIX.size()));
  }

  const auto iter = _tables.find(name);
  Assert(iter != _tables.end(), "No such table named '" + name + "'");

  return iter->second;
}

bool StorageManager::has_table(const std::string& name) const {
  if (MetaTableManager::is_meta_table_name(name)) {
    const auto& meta_table_names = Hyrise::get().meta_table_manager.table_names();
    return std::binary_search(meta_table_names.begin(), meta_table_names.end(),
                              name.substr(MetaTableManager::META_PREFIX.size()));
  }
  return _tables.count(name);
}

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> table_names;
  table_names.reserve(_tables.size());

  for (const auto& table_item : _tables) {
    table_names.emplace_back(table_item.first);
  }

  return table_names;
}

const std::map<std::string, std::shared_ptr<Table>>& StorageManager::tables() const { return _tables; }

void StorageManager::add_view(const std::string& name, const std::shared_ptr<LQPView>& view) {
  std::unique_lock lock(*_view_mutex);

  Assert(_tables.find(name) == _tables.end(),
         "Cannot add view " + name + " - a table with the same name already exists");
  Assert(_views.find(name) == _views.end(), "A view with the name " + name + " already exists");

  _views.emplace(name, view);
}

void StorageManager::drop_view(const std::string& name) {
  std::unique_lock lock(*_view_mutex);

  const auto num_deleted = _views.erase(name);
  Assert(num_deleted == 1, "Error deleting view " + name + ": _erase() returned " + std::to_string(num_deleted) + ".");
}

std::shared_ptr<LQPView> StorageManager::get_view(const std::string& name) const {
  std::shared_lock lock(*_view_mutex);

  const auto iter = _views.find(name);
  Assert(iter != _views.end(), "No such view named '" + name + "'");

  return iter->second->deep_copy();
}

bool StorageManager::has_view(const std::string& name) const {
  std::shared_lock lock(*_view_mutex);

  return _views.count(name);
}

std::vector<std::string> StorageManager::view_names() const {
  std::shared_lock lock(*_view_mutex);

  std::vector<std::string> view_names;
  view_names.reserve(_views.size());

  for (const auto& view_item : _views) {
    view_names.emplace_back(view_item.first);
  }

  return view_names;
}

const std::map<std::string, std::shared_ptr<LQPView>>& StorageManager::views() const { return _views; }

void StorageManager::add_prepared_plan(const std::string& name, const std::shared_ptr<PreparedPlan>& prepared_plan) {
  Assert(_prepared_plans.find(name) == _prepared_plans.end(),
         "Cannot add prepared plan " + name + " - a prepared plan with the same name already exists");

  _prepared_plans.emplace(name, prepared_plan);
}

std::shared_ptr<PreparedPlan> StorageManager::get_prepared_plan(const std::string& name) const {
  const auto iter = _prepared_plans.find(name);
  Assert(iter != _prepared_plans.end(), "No such prepared plan named '" + name + "'");

  return iter->second;
}

bool StorageManager::has_prepared_plan(const std::string& name) const {
  return _prepared_plans.find(name) != _prepared_plans.end();
}

void StorageManager::drop_prepared_plan(const std::string& name) {
  const auto iter = _prepared_plans.find(name);
  Assert(iter != _prepared_plans.end(), "No such prepared plan named '" + name + "'");

  _prepared_plans.erase(iter);
}

const std::map<std::string, std::shared_ptr<PreparedPlan>>& StorageManager::prepared_plans() const {
  return _prepared_plans;
}

void StorageManager::export_all_tables_as_csv(const std::string& path) {
  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
  tasks.reserve(_tables.size());

  for (auto& pair : _tables) {
    auto job_task = std::make_shared<JobTask>([pair, &path]() {
      const auto& name = pair.first;
      auto& table = pair.second;

      auto table_wrapper = std::make_shared<TableWrapper>(table);
      table_wrapper->execute();

      auto export_csv = std::make_shared<Export>(table_wrapper, path + "/" + name + ".csv", FileType::Csv);  // NOLINT
      export_csv->execute();
    });
    tasks.push_back(job_task);
    job_task->schedule();
  }

  Hyrise::get().scheduler()->wait_for_tasks(tasks);
}

std::ostream& operator<<(std::ostream& stream, const StorageManager& storage_manager) {
  stream << "==================" << std::endl;
  stream << "===== Tables =====" << std::endl << std::endl;

  for (auto const& table : storage_manager.tables()) {
    stream << "==== table >> " << table.first << " <<";
    stream << " (" << table.second->column_count() << " columns, " << table.second->row_count() << " rows in "
           << table.second->chunk_count() << " chunks)";
    stream << std::endl;
  }

  stream << "==================" << std::endl;
  stream << "===== Views ======" << std::endl << std::endl;

  for (auto const& view : storage_manager.views()) {
    stream << "==== view >> " << view.first << " <<";
    stream << std::endl;
  }

  stream << "==================" << std::endl;
  stream << "= PreparedPlans ==" << std::endl << std::endl;

  for (auto const& prepared_plan : storage_manager.prepared_plans()) {
    stream << "==== prepared plan >> " << prepared_plan.first << " <<";
    stream << std::endl;
  }

  return stream;
}

void StorageManager::apply_partitioning() {
  std::ifstream file("partitioning.json");
  nlohmann::json json;
  file >> json;

  for (const auto& entry : json.items()) {
    const auto& table_name = entry.key();
    const auto& table = Hyrise::get().storage_manager.get_table(table_name);
    std::cout << "Partitioning " << table_name << std::endl;
    const auto& dimensions = entry.value();

    auto partition_by_row_idx = std::vector<size_t>(table->row_count());
    auto row_id_by_row_idx = std::vector<RowID>(table->row_count());

    auto total_num_partitions = size_t{1};

    for (auto dimension_id = size_t{0}; dimension_id < dimensions.size(); ++dimension_id) {
      const auto& dimension = dimensions[dimension_id];
      const auto partition_count = static_cast<size_t>(dimension["partitions"]);
      total_num_partitions *= partition_count;

      std::cout << "\tCalculating boundaries for " << dimension["column_name"] << " with " << partition_count << " partitions" << std::endl;
      Timer timer;

      Assert(partition_count < table->row_count(), "Partition count must be smaller than table size");

      const auto column_id = table->column_id_by_name(dimension["column_name"]);
      resolve_data_type(table->column_data_type(column_id), [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;

        auto materialized = std::vector<std::pair<ColumnDataType, size_t>>{};
        materialized.reserve(table->row_count());

        {
          auto row_idx = size_t{0};
          for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
            const auto& chunk = table->get_chunk(chunk_id);
            const auto& segment = chunk->get_segment(column_id);

            segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
              Assert(!position.is_null(), "Partitioning on NULL values not yet supported");
              materialized.emplace_back(std::pair<ColumnDataType, size_t>{position.value(), row_idx});
              row_id_by_row_idx[row_idx] = RowID{chunk_id, position.chunk_offset()};

              ++row_idx;
            });
          }
        }

        std::sort(materialized.begin(), materialized.end(), [](const auto& lhs, const auto& rhs) {
          return lhs < rhs;
        });

        const auto partition_size = materialized.size() / partition_count;
        for (auto partition_id = size_t{0}; partition_id < partition_count; ++partition_id) {
          const auto partition_begin = partition_id * partition_size;
          const auto partition_end = partition_id == partition_count - 1 ? materialized.size() : (partition_id + 1) * partition_size;
          std::cout << "\t\tPartition " << partition_id << " from index " << partition_begin << " to " << partition_end << " (size " << (partition_end - partition_begin + 1) << ") - values " << materialized.at(partition_begin).first << " to " << materialized.at(partition_end - 1).first << std::endl;
          for (auto materialized_idx = partition_begin; materialized_idx < partition_end; ++materialized_idx) {
            const auto row_idx = materialized[materialized_idx].second;

            // Shift existing partitions
            partition_by_row_idx[row_idx] *= partition_count;

            // Set partition of this
            partition_by_row_idx[row_idx] += partition_id;
          }
        }
      });

      std::cout << "\t\tdone (" << timer.lap_formatted() << ")" << std::endl;
    }

    // Write segments
    auto segments_by_partition = std::vector<Segments>(total_num_partitions, Segments(table->column_count()));
    {
      std::cout << "\tWriting partitioned columns in parallel" << std::flush;
      Timer timer;

      auto threads = std::vector<std::thread>{};
      for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
        threads.emplace_back(std::thread([&, column_id] {
          resolve_data_type(table->column_data_type(column_id), [&](auto type) {
            using ColumnDataType = typename decltype(type)::type;

            auto original_dictionary_segments = std::vector<std::shared_ptr<const DictionarySegment<ColumnDataType>>>(table->chunk_count());
            for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
              const auto& original_chunk = table->get_chunk(chunk_id);
              const auto& original_segment = original_chunk->get_segment(column_id);
              const auto& original_dictionary_segment = std::static_pointer_cast<const DictionarySegment<ColumnDataType>>(original_segment);

              original_dictionary_segments[chunk_id] = original_dictionary_segment;
            }

            auto values_by_partition = std::vector<pmr_vector<ColumnDataType>>(total_num_partitions);
            for (auto& values : values_by_partition) {
              values.reserve(table->row_count() / (total_num_partitions - 1));
            }

            const auto row_count = table->row_count();
            for (auto row_idx = size_t{0}; row_idx < row_count; ++row_idx) {
              if (row_idx % 1000000 == 0) std::cout << "column " << column_id << " / row_idx " << row_idx << std::endl; 

              const auto [chunk_id, chunk_offset] = row_id_by_row_idx[row_idx];
              const auto partition_id = partition_by_row_idx[row_idx];

              const auto& original_dictionary_segment = original_dictionary_segments[chunk_id];

              values_by_partition[partition_id].emplace_back(*original_dictionary_segment->get_typed_value(chunk_offset));
            }

            for (auto partition_id = size_t{0}; partition_id < total_num_partitions; ++partition_id) {
              auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values_by_partition[partition_id]));
              segments_by_partition[partition_id][column_id] = value_segment;
            }
          });
        }));
      }
      for (auto& thread : threads) thread.join();

      std::cout << " - done (" << timer.lap_formatted() << ")" << std::endl;
    }


    // Write new table
    auto new_table = std::make_shared<Table>(table->column_definitions(), TableType::Data, std::nullopt, UseMvcc::Yes);
    for (auto partition_id = size_t{0}; partition_id < total_num_partitions; ++partition_id) {
      const auto& segments = segments_by_partition[partition_id];
      // Note that this makes all rows that have been deleted visible again
      auto mvcc_data = std::make_shared<MvccData>(segments[0]->size(), CommitID{0});
      new_table->append_chunk(segments, mvcc_data);
      new_table->last_chunk()->finalize();
    }

    {
      std::cout << "Applying dictionary encoding to new table" << std::flush;
      Timer timer;
      ChunkEncoder::encode_all_chunks(new_table, SegmentEncodingSpec{EncodingType::Dictionary});
      std::cout << " - done (" << timer.lap_formatted() << ")" << std::endl;
    }

    {
      std::cout << "Generating statistics" << std::flush;
      Timer timer;
      drop_table(table_name);
      add_table(table_name, new_table);
      std::cout << " - done (" << timer.lap_formatted() << ")" << std::endl;
    }
  }
}

}  // namespace opossum
