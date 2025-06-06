// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "gtest/gtest.h"
#include "leveldb/filter_policy.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/testutil.h"

namespace leveldb {

static const int kVerbose = 1;

static std::string_view Key(int i, char* buffer) {
  EncodeFixed32(buffer, i);
  return std::string_view(buffer, sizeof(uint32_t));
}

class BloomTest : public testing::Test {
 public:
  BloomTest() : policy_(NewBloomFilterPolicy(10)) {}

  ~BloomTest() { delete policy_; }

  void Reset() {
    keys_.clear();
    filter_.clear();
  }

  void Add(const std::string_view& s) { keys_.push_back(std::string(s)); }

  void Build() {
    std::vector<std::string_view> key_slices;
    for (size_t i = 0; i < keys_.size(); i++) {
      key_slices.push_back(std::string_view(keys_[i]));
    }
    filter_.clear();
    policy_->CreateFilter(&key_slices[0], static_cast<int>(key_slices.size()),
                          &filter_);
    keys_.clear();
    if (kVerbose >= 2) DumpFilter();
  }

  size_t FilterSize() const { return filter_.size(); }

  void DumpFilter() {
    std::fprintf(stderr, "F(");
    for (size_t i = 0; i + 1 < filter_.size(); i++) {
      const unsigned int c = static_cast<unsigned int>(filter_[i]);
      for (int j = 0; j < 8; j++) {
        std::fprintf(stderr, "%c", (c & (1 << j)) ? '1' : '.');
      }
    }
    std::fprintf(stderr, ")\n");
  }

  bool Matches(const std::string_view& s) {
    if (!keys_.empty()) {
      Build();
    }
    return policy_->KeyMayMatch(s, filter_);
  }

  double FalsePositiveRate() {
    char buffer[sizeof(int)];
    int result = 0;
    for (int i = 0; i < 10000; i++) {
      if (Matches(Key(i + 1000000000, buffer))) {
        result++;
      }
    }
    return result / 10000.0;
  }

 private:
  const FilterPolicy* policy_;
  std::string filter_;
  std::vector<std::string> keys_;
};

TEST_F(BloomTest, EmptyFilter) {
  ASSERT_TRUE(!Matches("hello"));
  ASSERT_TRUE(!Matches("world"));
}

TEST_F(BloomTest, Small) {
  Add("hello");
  Add("world");
  ASSERT_TRUE(Matches("hello"));
  ASSERT_TRUE(Matches("world"));
  ASSERT_TRUE(!Matches("x"));
  ASSERT_TRUE(!Matches("foo"));
}

static int NextLength(int length) {
  if (length < 10) {
    length += 1;
  } else if (length < 100) {
    length += 10;
  } else if (length < 1000) {
    length += 100;
  } else {
    length += 1000;
  }
  return length;
}

TEST_F(BloomTest, VaryingLengths) {
  char buffer[sizeof(int)];

  // Count number of filters that significantly exceed the false positive rate
  int mediocre_filters = 0;
  int good_filters = 0;

  for (int length = 1; length <= 10000; length = NextLength(length)) {
    Reset();
    for (int i = 0; i < length; i++) {
      Add(Key(i, buffer));
    }
    Build();

    ASSERT_LE(FilterSize(), static_cast<size_t>((length * 10 / 8) + 40))
        << length;

    // All added keys must match
    for (int i = 0; i < length; i++) {
      ASSERT_TRUE(Matches(Key(i, buffer)))
          << "Length " << length << "; key " << i;
    }

    // Check false positive rate
    double rate = FalsePositiveRate();
    if (kVerbose >= 1) {
      std::fprintf(stderr,
                   "False positives: %5.2f%% @ length = %6d ; bytes = %6d\n",
                   rate * 100.0, length, static_cast<int>(FilterSize()));
    }
    ASSERT_LE(rate, 0.02);  // Must not be over 2%
    if (rate > 0.0125)
      mediocre_filters++;  // Allowed, but not too often
    else
      good_filters++;
  }
  if (kVerbose >= 1) {
    std::fprintf(stderr, "Filters: %d good, %d mediocre\n", good_filters,
                 mediocre_filters);
  }
  ASSERT_LE(mediocre_filters, good_filters / 5);
}

// Different bits-per-byte

}  // namespace leveldb
