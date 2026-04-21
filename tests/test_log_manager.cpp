#include <gtest/gtest.h>

#include <string>

#include "broker/log_manager.h"

using namespace kafka;

TEST(LogManagerTest, find_index_by_msg_offset_1_item) {
    std::vector<IndexEntry> indexes;
    indexes.push_back({0, 0});

    EXPECT_EQ(find_index_by_msg_offset(indexes, 127), 0);
}

TEST(LogManagerTest, find_index_by_msg_offset_2_item) {
    std::vector<IndexEntry> indexes;
    indexes.push_back({0, 0});
    indexes.push_back({5, 20});

    EXPECT_EQ(find_index_by_msg_offset(indexes, 0), 0);
    EXPECT_EQ(find_index_by_msg_offset(indexes, 1), 0);
    EXPECT_EQ(find_index_by_msg_offset(indexes, 4), 0);
    EXPECT_EQ(find_index_by_msg_offset(indexes, 5), 1);
    EXPECT_EQ(find_index_by_msg_offset(indexes, 6), 1);
    EXPECT_EQ(find_index_by_msg_offset(indexes, 20), 1);
}

TEST(LogManagerTest, find_index_by_msg_offset_3_item) {
    std::vector<IndexEntry> indexes;
    indexes.push_back({0, 0});
    indexes.push_back({5, 20});
    indexes.push_back({20, 50});

    EXPECT_EQ(find_index_by_msg_offset(indexes, 0), 0);
    EXPECT_EQ(find_index_by_msg_offset(indexes, 1), 0);
    EXPECT_EQ(find_index_by_msg_offset(indexes, 4), 0);
    EXPECT_EQ(find_index_by_msg_offset(indexes, 5), 1);
    EXPECT_EQ(find_index_by_msg_offset(indexes, 6), 1);
    EXPECT_EQ(find_index_by_msg_offset(indexes, 20), 2);
    EXPECT_EQ(find_index_by_msg_offset(indexes, 21), 2);
    EXPECT_EQ(find_index_by_msg_offset(indexes, 500), 2);
}

TEST(LogManagerTest, find_index_by_msg_offset_multiple_item) {
    std::vector<IndexEntry> indexes;
    indexes.push_back({0, 0});      // 0
    indexes.push_back({5, 20});     // 1
    indexes.push_back({20, 50});    // 2
    indexes.push_back({30, 100});   // 3
    indexes.push_back({60, 100});   // 4
    indexes.push_back({61, 100});   // 5
    indexes.push_back({100, 100});  // 6

    EXPECT_EQ(find_index_by_msg_offset(indexes, 60), 4);
    EXPECT_EQ(find_index_by_msg_offset(indexes, 61), 5);
    EXPECT_EQ(find_index_by_msg_offset(indexes, 70), 5);
    EXPECT_EQ(find_index_by_msg_offset(indexes, 700), 6);
}