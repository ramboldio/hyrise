#include "run_length_segment.hpp"

#include <algorithm>

#include "resolve_type.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"
#include "utils/size_estimation_utils.hpp"

namespace opossum {

template <typename T>
RunLengthSegment<T>::RunLengthSegment(pmr_vector<T> values,
                                      std::optional<const pmr_vector<bool>> null_values,
                                      pmr_vector<ChunkOffset> end_positions)
    : BaseEncodedSegment(data_type_from_type<T>()),
      _values{std::move(values)},
      _null_values{std::move(null_values)},
      _end_positions{std::move(end_positions)} {}

template <typename T>
const pmr_vector<T>& RunLengthSegment<T>::values() const {
  return _values;
}

template <typename T>
const std::optional<const pmr_vector<bool>>& RunLengthSegment<T>::null_values() const {
  return _null_values;
}

template <typename T>
const pmr_vector<ChunkOffset>& RunLengthSegment<T>::end_positions() const {
  return _end_positions;
}

template <typename T>
AllTypeVariant RunLengthSegment<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  const auto typed_value = get_typed_value(chunk_offset);
  if (!typed_value) {
    return NULL_VALUE;
  }
  return *typed_value;
}

template <typename T>
ChunkOffset RunLengthSegment<T>::size() const {
  if (_end_positions.empty()) return 0u;
  return _end_positions.back() + 1u;
}

template <typename T>
std::shared_ptr<BaseSegment> RunLengthSegment<T>::copy_using_allocator(
    const PolymorphicAllocator<size_t>& allocator) const {
  auto new_values = pmr_vector<T>(_values, allocator);

  auto new_null_values =
      _null_values ? std::optional<pmr_vector<bool>>{pmr_vector<bool>{*_null_values, allocator}} : std::nullopt;
  auto new_end_positions = pmr_vector<ChunkOffset>(_end_positions, allocator);

  auto copy = std::make_shared<RunLengthSegment<T>>(std::move(new_values), std::move(new_null_values), std::move(new_end_positions));
  copy->access_counter = access_counter;

  return copy;
}

template <typename T>
size_t RunLengthSegment<T>::memory_usage([[maybe_unused]] const MemoryUsageCalculationMode mode) const {
  auto common_elements_size = sizeof(*this) + _end_positions.capacity() * sizeof(ChunkOffset) + sizeof(_null_values);

  if (_null_values) {
    common_elements_size += _null_values->capacity() / CHAR_BIT;
  }

  if constexpr (std::is_same_v<T, pmr_string>) {  // NOLINT
    return common_elements_size + string_vector_memory_usage(_values, mode);
  }
  return common_elements_size + _values.capacity() * sizeof(T);
}

template <typename T>
EncodingType RunLengthSegment<T>::encoding_type() const {
  return EncodingType::RunLength;
}

template <typename T>
std::optional<CompressedVectorType> RunLengthSegment<T>::compressed_vector_type() const {
  return std::nullopt;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(RunLengthSegment);

}  // namespace opossum
