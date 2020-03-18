#include <future>
#include <thread>

#include "base_test.hpp"

#include "hyrise.hpp"
#include "scheduler/node_queue_scheduler.hpp"

namespace opossum {

class StressTest : public BaseTest {
 protected:
  void SetUp() override {
    Hyrise::reset();

    // Set scheduler so that we can execute multiple SQL statements on separate threads.
    Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  }
};

TEST_F(StressTest, TestTransactionConflicts) {
  // Update a table with two entries and a chunk size of 2. This will lead to a high number of transaction conflicts
  // and many chunks being created
  auto table_a = load_table("resources/test_data/tbl/int_float.tbl", 2);
  Hyrise::get().storage_manager.add_table("table_a", table_a);
  auto initial_sum = int64_t{};

  {
    auto pipeline = SQLPipelineBuilder{std::string{"SELECT SUM(a) FROM table_a"}}.create_pipeline();
    const auto [_, table] = pipeline.get_result_table();
    initial_sum = table->get_value<int64_t>(ColumnID{0}, 0);
  }

  std::atomic_int successful_increments{0};
  std::atomic_int conflicted_increments{0};
  const auto iterations_per_thread = 20;

  // Define the work package
  const auto run = [&]() {
    int my_successful_increments{0};
    int my_conflicted_increments{0};
    for (auto iteration = 0; iteration < iterations_per_thread; ++iteration) {
      const std::string sql = "UPDATE table_a SET a = a + 1 WHERE a = (SELECT MIN(a) FROM table_a);";
      auto pipeline = SQLPipelineBuilder{sql}.create_pipeline();
      const auto [status, _] = pipeline.get_result_table();
      if (status == SQLPipelineStatus::Success) {
        ++my_successful_increments;
      } else {
        ++my_conflicted_increments;
      }
    }
    successful_increments += my_successful_increments;
    conflicted_increments += my_conflicted_increments;
  };

  // Create the async objects and spawn them asynchronously (i.e., as their own threads)
  const auto num_threads = 100u;
  std::vector<std::future<void>> thread_futures;
  thread_futures.reserve(num_threads);

  for (auto thread_num = 0u; thread_num < num_threads; ++thread_num) {
    // We want a future to the thread running, so we can kill it after a future.wait(timeout) or the test would freeze
    thread_futures.emplace_back(std::async(std::launch::async, run));
  }

  // Wait for completion or timeout (should not occur)
  for (auto& thread_future : thread_futures) {
    // We give this a lot of time, not because we need that long for 100 threads to finish, but because sanitizers and
    // other tools like valgrind sometimes bring a high overhead that exceeds 10 seconds.
    if (thread_future.wait_for(std::chrono::seconds(150)) == std::future_status::timeout) {
      ASSERT_TRUE(false) << "At least one thread got stuck and did not commit.";
      // Retrieve the future so that exceptions stored in its state are thrown
      thread_future.get();
    }
  }

  // Verify results
  auto final_sum = int64_t{};
  {
    auto pipeline = SQLPipelineBuilder{std::string{"SELECT SUM(a) FROM table_a"}}.create_pipeline();
    const auto [_, table] = pipeline.get_result_table();
    final_sum = table->get_value<int64_t>(ColumnID{0}, 0);
  }

  // Really pessimistic, but at least 2 statements should have made it
  EXPECT_GT(successful_increments, 2);

  EXPECT_EQ(successful_increments + conflicted_increments, num_threads * iterations_per_thread);
  EXPECT_FLOAT_EQ(final_sum - initial_sum, successful_increments);
}

TEST_F(StressTest, TestTransactionInserts) {
  // An update-heavy load on a table with a ridiculously low target chunk size, creating many new chunks. This is
  // different from TestTransactionConflicts, in that each thread has its own logical row and no transaction
  // conflicts occur. In the other test, a failed "mark for deletion" (i.e., swap of the row's tid) would lead to
  // no row being appended.
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, false);
  column_definitions.emplace_back("b", DataType::Int, false);
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data, 3, UseMvcc::Yes);
  Hyrise::get().storage_manager.add_table("table_b", table);

  std::atomic_int successful_increments{0};
  std::atomic_int conflicted_increments{0};
  const auto iterations_per_thread = 20;

  // Define the work package - the job id is used so that each thread has its own logical row to work on
  std::atomic_int job_id{0};
  const auto run = [&]() {
    const auto my_job_id = job_id++;
    for (auto iteration = 0; iteration < iterations_per_thread; ++iteration) {
      auto pipeline =
          SQLPipelineBuilder{
              iteration == 0 ? std::string{"INSERT INTO table_b (a, b) VALUES ("} + std::to_string(my_job_id) + ", 1)"
                             : std::string{"UPDATE table_b SET b = b + 1 WHERE a = "} + std::to_string(my_job_id)}
              .create_pipeline();
      const auto [status, _] = pipeline.get_result_table();
      if (status == SQLPipelineStatus::Success) {
        ++successful_increments;
      } else {
        ++conflicted_increments;
      }
    }
  };

  // Create the async objects and spawn them asynchronously (i.e., as their own threads)
  const auto num_threads = 100u;
  std::vector<std::future<void>> thread_futures;
  thread_futures.reserve(num_threads);

  for (auto thread_num = 0u; thread_num < num_threads; ++thread_num) {
    // We want a future to the thread running, so we can kill it after a future.wait(timeout) or the test would freeze
    thread_futures.emplace_back(std::async(std::launch::async, run));
  }

  // Wait for completion or timeout (should not occur)
  for (auto& thread_future : thread_futures) {
    // We give this a lot of time, not because we need that long for 100 threads to finish, but because sanitizers and
    // other tools like valgrind sometimes bring a high overhead that exceeds 10 seconds.
    if (thread_future.wait_for(std::chrono::seconds(600)) == std::future_status::timeout) {
      ASSERT_TRUE(false) << "At least one thread got stuck and did not commit.";
      // Retrieve the future so that exceptions stored in its state are thrown
      thread_future.get();
    }
  }

  // Verify results
  {
    auto pipeline = SQLPipelineBuilder{std::string{"SELECT MIN(b) FROM table_b"}}.create_pipeline();
    const auto [_, table] = pipeline.get_result_table();
    EXPECT_FLOAT_EQ(table->get_value<int32_t>(ColumnID{0}, 0), iterations_per_thread);
  }

  // Really pessimistic, but at least 2 statements should have made it
  EXPECT_EQ(successful_increments, num_threads * iterations_per_thread);
  EXPECT_EQ(conflicted_increments, 0);
}

TEST_F(StressTest, Encoding) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, false);
  column_definitions.emplace_back("b", DataType::String, false);

  auto table_a = std::make_shared<Table>(column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);
  for (auto row_id = int32_t{0}; row_id < 2'000'017; ++row_id) {
    table_a->append({row_id, pmr_string{std::to_string(row_id)}});
  }
  const auto chunk_count = table_a->chunk_count();
  table_a->get_chunk(ChunkID{chunk_count - 1})->finalize();
  Hyrise::get().storage_manager.add_table("table_a", table_a);

  auto stop = false;

  auto query_runner = [&]() {
    while (!stop) {
      {
        auto pipeline =
            SQLPipelineBuilder{std::string{"SELECT SUM(a), MIN(b) FROM table_a WHERE a < 17"}}
                .create_pipeline();
        const auto [_, table] = pipeline.get_result_table();
        ASSERT_EQ(136, table->get_value<int64_t>(ColumnID{0}, 0));
        ASSERT_EQ(pmr_string{"0"}, table->get_value<pmr_string>(ColumnID{1}, 0));
      }
      {
        auto pipeline =
            SQLPipelineBuilder{std::string{"SELECT t1.a as t1a, t1.b as t1b FROM table_a AS t1 JOIN table_a AS t2 ON t1.a = t2.a WHERE t1a < 17"}}
                .create_pipeline();
        const auto [_, table] = pipeline.get_result_table();
        ASSERT_EQ(0, table->get_value<int32_t>(ColumnID{0}, 0));
        ASSERT_EQ(pmr_string{"0"}, table->get_value<pmr_string>(ColumnID{1}, 0));
      }
    }
  };

  const auto chunk_encoding_specs = {
      ChunkEncodingSpec{SegmentEncodingSpec{EncodingType::Unencoded},
                        SegmentEncodingSpec{EncodingType::FixedStringDictionary}},
      ChunkEncodingSpec{SegmentEncodingSpec{EncodingType::Dictionary}, SegmentEncodingSpec{EncodingType::Unencoded}},
      ChunkEncodingSpec{SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::SimdBp128},
                        SegmentEncodingSpec{EncodingType::LZ4}},
      ChunkEncodingSpec{SegmentEncodingSpec{EncodingType::Unencoded}, SegmentEncodingSpec{EncodingType::Dictionary}},
      ChunkEncodingSpec{SegmentEncodingSpec{EncodingType::RunLength}, SegmentEncodingSpec{EncodingType::LZ4}},
      ChunkEncodingSpec{SegmentEncodingSpec{EncodingType::FrameOfReference},
                        SegmentEncodingSpec{EncodingType::RunLength}},
      ChunkEncodingSpec{SegmentEncodingSpec{EncodingType::FrameOfReference, VectorCompressionType::SimdBp128},
                        SegmentEncodingSpec{EncodingType::LZ4}},
      ChunkEncodingSpec{SegmentEncodingSpec{EncodingType::FrameOfReference, VectorCompressionType::SimdBp128},
                        SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::SimdBp128}}};

  auto encoding_runner = [&]() {
    const auto column_count = table_a->column_count();
    while (!stop) {
      for (const auto& chunk_encoding_spec : chunk_encoding_specs) {
        for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
          const auto& chunk = table_a->get_chunk(chunk_id);
          for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
            const auto segment_encoding_spec = chunk_encoding_spec[column_id];
            const auto& inital_segment = chunk->get_segment(column_id);
            const auto data_type = column_definitions[column_id].data_type;
            const auto encoded_segment = ChunkEncoder::encode_segment(inital_segment, data_type, segment_encoding_spec);
            chunk->replace_segment(column_id, encoded_segment);
          }
          assert_chunk_encoding(chunk, chunk_encoding_spec);
        }
      }
    }
  };

  std::thread query_thread_1([&] { query_runner(); });
  std::thread query_thread_2([&] { query_runner(); });
  std::thread query_thread_3([&] { query_runner(); });
  std::thread query_thread_4([&] { query_runner(); });
  std::thread encoding_thread([&] { encoding_runner(); });

  std::this_thread::sleep_for(std::chrono::seconds(600));
  stop = true;
  query_thread_1.join();
  query_thread_2.join();
  query_thread_3.join();
  query_thread_4.join();
  encoding_thread.join();
}

}  // namespace opossum
