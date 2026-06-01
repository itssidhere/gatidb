#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#define private public
#include "gatidb/btree.hpp"
#undef private
namespace {
int failures = 0;
void check(bool condition, const std::string& test_name, const std::string& message) {
    if (condition) {
        return;
    }
    ++failures;
    std::cerr << "FAIL " << test_name << ": " << message << '\n';
}
void collect_keys(const gatidb::Btree::Node* node, std::vector<int>& keys) {
    if (node == nullptr) {
        return;
    }
    keys.insert(keys.end(), node->keys.begin(), node->keys.end());
    for (const auto& child : node->children) {
        collect_keys(child.get(), keys);
    }
}
std::unique_ptr<gatidb::Btree::Node> make_leaf(std::vector<int> keys) {
    auto node = std::make_unique<gatidb::Btree::Node>();
    node->is_leaf = true;
    node->keys = std::move(keys);
    for (const int key : node->keys) {
        node->values.push_back(key * 10);
    }
    return node;
}
gatidb::Btree make_two_leaf_tree(std::vector<int> left_keys, int separator,
                                 std::vector<int> right_keys) {
    gatidb::Btree tree;
    tree.root_ = std::make_unique<gatidb::Btree::Node>();
    tree.root_->is_leaf = false;
    tree.root_->keys = {separator};
    tree.root_->values = {separator * 10};
    tree.root_->children.push_back(make_leaf(std::move(left_keys)));
    tree.root_->children.push_back(make_leaf(std::move(right_keys)));
    return tree;
}
gatidb::Btree make_three_leaf_root_tree() {
    gatidb::Btree tree;
    tree.root_ = std::make_unique<gatidb::Btree::Node>();
    tree.root_->is_leaf = false;
    tree.root_->keys = {3, 7};
    tree.root_->values = {30, 70};
    tree.root_->children.push_back(make_leaf({0, 1, 2}));
    tree.root_->children.push_back(make_leaf({4, 5, 6}));
    tree.root_->children.push_back(make_leaf({8, 9, 10}));
    return tree;
}
std::unique_ptr<gatidb::Btree::Node>
make_internal(std::vector<int> keys, std::vector<std::unique_ptr<gatidb::Btree::Node>> children) {
    auto node = std::make_unique<gatidb::Btree::Node>();
    node->is_leaf = false;
    node->keys = std::move(keys);
    for (const int key : node->keys) {
        node->values.push_back(key * 10);
    }
    node->children = std::move(children);
    return node;
}
gatidb::Btree make_three_level_tree() {
    std::vector<std::unique_ptr<gatidb::Btree::Node>> left_children;
    left_children.push_back(make_leaf({1, 2, 3}));
    left_children.push_back(make_leaf({21, 22, 23}));
    left_children.push_back(make_leaf({41, 42, 43}));
    left_children.push_back(make_leaf({61, 62, 63}));

    std::vector<std::unique_ptr<gatidb::Btree::Node>> right_children;
    right_children.push_back(make_leaf({101, 102, 103}));
    right_children.push_back(make_leaf({121, 122, 123}));
    right_children.push_back(make_leaf({141, 142, 143}));
    right_children.push_back(make_leaf({161, 162, 163}));

    gatidb::Btree tree;
    tree.root_ = std::make_unique<gatidb::Btree::Node>();
    tree.root_->is_leaf = false;
    tree.root_->keys = {100};
    tree.root_->values = {1000};
    tree.root_->children.push_back(make_internal({20, 40, 60}, std::move(left_children)));
    tree.root_->children.push_back(make_internal({120, 140, 160}, std::move(right_children)));
    return tree;
}
void test_first_insert_initializes_leaf_root() {
    const std::string test_name = "first insert initializes leaf root";
    gatidb::Btree tree;
    tree.insert(10, 100);
    check(tree.root_ != nullptr, test_name, "root should exist after first insert");
    if (!tree.root_) {
        return;
    }
    check(tree.root_->is_leaf, test_name, "root should start as a leaf");
    check(tree.root_->keys == std::vector<int>{10}, test_name, "root keys should be [10]");
    check(tree.root_->values == std::vector<int>{100}, test_name, "root values should be [100]");
}
void test_leaf_keys_stay_sorted() {
    const std::string test_name = "leaf keys stay sorted";
    gatidb::Btree tree;
    tree.insert(10, 100);
    tree.insert(5, 50);
    tree.insert(7, 70);
    check(tree.root_ != nullptr, test_name, "root should exist");
    if (!tree.root_) {
        return;
    }
    check(tree.root_->keys == std::vector<int>{5, 7, 10}, test_name,
          "after inserts 10, 5, 7, keys should be [5, 7, 10]");
    check(tree.root_->values == std::vector<int>{50, 70, 100}, test_name,
          "values should move with their matching keys");
}
void test_root_does_not_exceed_node_capacity() {
    const std::string test_name = "root does not exceed node capacity";
    gatidb::Btree tree;
    for (std::size_t i = 0; i <= gatidb::MAX_KEYS; ++i) {
        tree.insert(static_cast<int>(i), static_cast<int>(i * 10));
    }
    check(tree.root_ != nullptr, test_name, "root should exist");
    if (!tree.root_) {
        return;
    }
    check(tree.root_->keys.size() <= gatidb::MAX_KEYS, test_name,
          "root should split or otherwise avoid storing more than MAX_KEYS keys");
}
void test_insert_past_root_capacity_keeps_all_keys() {
    const std::string test_name = "insert past root capacity keeps all keys";
    gatidb::Btree tree;
    for (std::size_t i = 0; i <= gatidb::MAX_KEYS; ++i) {
        tree.insert(static_cast<int>(i), static_cast<int>(i * 10));
    }
    std::vector<int> keys;
    collect_keys(tree.root_.get(), keys);
    std::sort(keys.begin(), keys.end());
    std::vector<int> expected;
    for (std::size_t i = 0; i <= gatidb::MAX_KEYS; ++i) {
        expected.push_back(static_cast<int>(i));
    }
    check(keys == expected, test_name,
          "all inserted keys through MAX_KEYS should still exist after overflow insert");
}
void test_root_split_shape_after_overflow_insert() {
    const std::string test_name = "root split shape after overflow insert";
    gatidb::Btree tree;
    for (std::size_t i = 0; i <= gatidb::MAX_KEYS; ++i) {
        tree.insert(static_cast<int>(i), static_cast<int>(i * 10));
    }
    check(tree.root_ != nullptr, test_name, "root should exist");
    if (!tree.root_) {
        return;
    }
    check(!tree.root_->is_leaf, test_name, "root should become internal after split");
    check(tree.root_->keys.size() == 1, test_name, "new root should contain one promoted key");
    check(tree.root_->values.size() == 1, test_name, "new root should contain promoted value");
    check(tree.root_->children.size() == 2, test_name, "new root should have two children");
    if (tree.root_->children.size() != 2 || tree.root_->keys.empty()) {
        return;
    }
    const auto* left = tree.root_->children[0].get();
    const auto* right = tree.root_->children[1].get();
    const int separator = tree.root_->keys[0];
    check(left != nullptr, test_name, "left child should exist");
    check(right != nullptr, test_name, "right child should exist");
    if (left == nullptr || right == nullptr) {
        return;
    }
    check(left->is_leaf, test_name, "left child should be leaf when splitting a leaf root");
    check(right->is_leaf, test_name, "right child should be leaf when splitting a leaf root");
    check(std::all_of(left->keys.begin(), left->keys.end(),
                      [separator](int key) { return key < separator; }),
          test_name, "all left child keys should be less than root separator");
    check(std::all_of(right->keys.begin(), right->keys.end(),
                      [separator](int key) { return key > separator; }),
          test_name, "all right child keys should be greater than root separator");
}
void test_inserting_existing_separator_does_not_duplicate_key() {
    const std::string test_name = "inserting existing separator does not duplicate key";
    gatidb::Btree tree;
    for (std::size_t i = 0; i <= gatidb::MAX_KEYS; ++i) {
        tree.insert(static_cast<int>(i), static_cast<int>(i * 10));
    }
    check(tree.root_ != nullptr, test_name, "root should exist");
    if (!tree.root_ || tree.root_->keys.empty()) {
        return;
    }
    const int separator = tree.root_->keys[0];
    tree.insert(separator, 999);
    std::vector<int> keys;
    collect_keys(tree.root_.get(), keys);
    const auto count = std::count(keys.begin(), keys.end(), separator);
    check(count == 1, test_name, "separator key should not also be inserted into a leaf");
}
void test_duplicate_key_updates_value_in_leaf_root() {
    const std::string test_name = "duplicate key updates value in leaf root";
    gatidb::Btree tree;
    tree.insert(10, 100);
    tree.insert(10, 999);
    check(tree.root_ != nullptr, test_name, "root should exist");
    if (!tree.root_) {
        return;
    }
    check(tree.root_->keys == std::vector<int>{10}, test_name,
          "duplicate key should not be inserted twice in a leaf root");
    check(tree.root_->values == std::vector<int>{999}, test_name,
          "duplicate key should update the existing leaf value");
}
void test_duplicate_key_updates_value_in_promoted_root() {
    const std::string test_name = "duplicate key updates value in promoted root";
    gatidb::Btree tree;
    for (std::size_t i = 0; i <= gatidb::MAX_KEYS; ++i) {
        tree.insert(static_cast<int>(i), static_cast<int>(i * 10));
    }
    check(tree.root_ != nullptr, test_name, "root should exist");
    if (!tree.root_ || tree.root_->keys.empty()) {
        return;
    }
    const int separator = tree.root_->keys[0];
    tree.insert(separator, 999);
    check(tree.root_->values[0] == 999, test_name,
          "duplicate separator key should update the promoted root value");
}
void test_duplicate_key_updates_value_in_leaf_after_root_split() {
    const std::string test_name = "duplicate key updates value in leaf after root split";
    gatidb::Btree tree;
    for (std::size_t i = 0; i <= gatidb::MAX_KEYS; ++i) {
        tree.insert(static_cast<int>(i), static_cast<int>(i * 10));
    }
    tree.insert(1, 999);
    check(tree.root_ != nullptr, test_name, "root should exist");
    if (!tree.root_ || tree.root_->children.empty()) {
        return;
    }
    const auto* left = tree.root_->children[0].get();
    const auto value_it = std::find(left->values.begin(), left->values.end(), 999);
    const auto key_count = std::count(left->keys.begin(), left->keys.end(), 1);
    check(key_count == 1, test_name, "duplicate leaf key should not be inserted twice");
    check(value_it != left->values.end(), test_name, "duplicate leaf key should update value");
}
void test_find_returns_values_and_missing_sentinel() {
    const std::string test_name = "find returns values and nullopt for missing keys";
    gatidb::Btree empty_tree;
    check(!empty_tree.find(1).has_value(), test_name, "empty tree lookup should return nullopt");
    gatidb::Btree tree;
    for (int key = 0; key < 20; ++key) {
        tree.insert(key, key * 10);
    }
    tree.insert(1, 999);
    tree.insert(18, 1800);
    check(tree.find(0) == 0, test_name, "should find leftmost key");
    check(tree.find(1) == 999, test_name, "should find updated key in child leaf");
    check(tree.find(3) == 30, test_name, "should find promoted/internal key");
    check(tree.find(18) == 1800, test_name, "should find updated key in right child");
    check(tree.find(19) == 190, test_name, "should find rightmost key");
    check(!tree.find(-1).has_value(), test_name, "missing key below range should return nullopt");
    check(!tree.find(100).has_value(), test_name, "missing key above range should return nullopt");
}
void test_erase_removes_key_from_leaf_root() {
    const std::string test_name = "erase removes key from leaf root";
    gatidb::Btree tree;
    tree.insert(10, 100);
    tree.insert(20, 200);
    tree.insert(30, 300);
    tree.erase(20);
    check(!tree.find(20).has_value(), test_name, "erased key should not be found");
    check(tree.find(10) == 100, test_name, "other keys should remain findable");
    check(tree.find(30) == 300, test_name, "other keys should remain findable");
}
void test_erase_with_left_borrow_removes_target_and_preserves_order() {
    const std::string test_name = "erase with left borrow removes target and preserves order";
    gatidb::Btree tree;
    for (int key = 0; key < 9; ++key) {
        tree.insert(key, key * 10);
    }
    tree.insert(1, 111);
    tree.erase(6);
    check(!tree.find(6).has_value(), test_name, "erased key should not be found");
    check(tree.find(1) == 111, test_name, "borrow should keep sibling values aligned");
    std::vector<int> keys;
    collect_keys(tree.root_.get(), keys);
    std::sort(keys.begin(), keys.end());
    check(keys == std::vector<int>{0, 1, 2, 3, 4, 5, 7, 8}, test_name,
          "tree should contain all original keys except erased key");
}
void test_erase_with_left_borrow_preserves_search_ranges() {
    const std::string test_name = "erase with left borrow preserves search ranges";
    gatidb::Btree tree;
    for (int key = 0; key <= static_cast<int>(gatidb::MAX_KEYS); ++key) {
        tree.insert(key, key * 10);
    }
    tree.insert(-1, -10);
    tree.erase(4);
    tree.erase(5);
    check(!tree.find(4).has_value(), test_name, "first erased key should not be found");
    check(!tree.find(5).has_value(), test_name, "second erased key should not be found");
    check(tree.find(-1) == -10, test_name, "borrow should preserve leftmost lookup");
    check(tree.find(0) == 0, test_name, "borrow should preserve left sibling lookup");
    check(tree.find(1) == 10, test_name, "borrow should preserve left sibling lookup");
    check(tree.find(2) == 20, test_name, "borrowed key should remain findable");
    check(tree.find(3) == 30, test_name, "parent separator key should remain findable");
    check(tree.find(6) == 60, test_name, "right child lookup should remain findable");
    check(tree.find(7) == 70, test_name, "right child lookup should remain findable");
}
void test_erase_with_right_borrow_preserves_search_ranges() {
    const std::string test_name = "erase with right borrow preserves search ranges";
    gatidb::Btree tree;
    for (int key = 0; key <= static_cast<int>(gatidb::MAX_KEYS); ++key) {
        tree.insert(key, key * 10);
    }
    tree.insert(8, 80);
    tree.erase(0);
    tree.erase(1);
    check(!tree.find(0).has_value(), test_name, "first erased key should not be found");
    check(!tree.find(1).has_value(), test_name, "second erased key should not be found");
    check(tree.find(2) == 20, test_name, "left child lookup should remain findable");
    check(tree.find(3) == 30, test_name, "borrow should preserve separator lookup");
    check(tree.find(4) == 40, test_name, "borrowed key should remain findable");
    check(tree.find(5) == 50, test_name, "right child lookup should remain findable");
    check(tree.find(6) == 60, test_name, "right child lookup should remain findable");
    check(tree.find(7) == 70, test_name, "right child lookup should remain findable");
    check(tree.find(8) == 80, test_name, "rightmost lookup should remain findable");
}
void test_erase_with_left_merge_preserves_search_ranges() {
    const std::string test_name = "erase with left merge preserves search ranges";
    auto tree = make_two_leaf_tree({0, 1, 2}, 3, {4, 5, 6});
    tree.erase(4);
    check(!tree.find(4).has_value(), test_name, "erased key should not be found");
    check(tree.find(0) == 0, test_name, "left sibling key should remain findable");
    check(tree.find(1) == 10, test_name, "left sibling key should remain findable");
    check(tree.find(2) == 20, test_name, "left sibling key should remain findable");
    check(tree.find(3) == 30, test_name, "pulled-down separator should remain findable");
    check(tree.find(5) == 50, test_name, "current node key should remain findable");
    check(tree.find(6) == 60, test_name, "current node key should remain findable");
    check(!(tree.root_ && !tree.root_->is_leaf && tree.root_->keys.empty()), test_name,
          "root should not remain an empty internal node after merge");
    check(tree.root_ && tree.root_->is_leaf, test_name, "merged child should become leaf root");
    check(tree.root_ && tree.root_->keys == std::vector<int>{0, 1, 2, 3, 5, 6}, test_name,
          "merged root keys should stay sorted");
}
void test_erase_with_right_merge_preserves_search_ranges() {
    const std::string test_name = "erase with right merge preserves search ranges";
    auto tree = make_two_leaf_tree({0, 1, 2}, 3, {4, 5, 6});
    tree.erase(0);
    check(!tree.find(0).has_value(), test_name, "erased key should not be found");
    check(tree.find(1) == 10, test_name, "current node key should remain findable");
    check(tree.find(2) == 20, test_name, "current node key should remain findable");
    check(tree.find(3) == 30, test_name, "pulled-down separator should remain findable");
    check(tree.find(4) == 40, test_name, "right sibling key should remain findable");
    check(tree.find(5) == 50, test_name, "right sibling key should remain findable");
    check(tree.find(6) == 60, test_name, "right sibling key should remain findable");
    check(!(tree.root_ && !tree.root_->is_leaf && tree.root_->keys.empty()), test_name,
          "root should not remain an empty internal node after merge");
    check(tree.root_ && tree.root_->is_leaf, test_name, "merged child should become leaf root");
    check(tree.root_ && tree.root_->keys == std::vector<int>{1, 2, 3, 4, 5, 6}, test_name,
          "merged root keys should stay sorted");
}
void test_root_may_keep_one_key_after_child_merge() {
    const std::string test_name = "root may keep one key after child merge";
    auto tree = make_three_leaf_root_tree();
    tree.erase(4);

    check(!tree.find(4).has_value(), test_name, "erased key should not be found");
    check(tree.root_ && !tree.root_->is_leaf, test_name, "root should remain internal");
    check(tree.root_ && tree.root_->keys == std::vector<int>{7}, test_name,
          "root with one remaining separator is valid");
    check(tree.find(0) == 0, test_name, "left merged key should remain findable");
    check(tree.find(3) == 30, test_name, "pulled-down separator should remain findable");
    check(tree.find(5) == 50, test_name, "middle key should remain findable");
    check(tree.find(10) == 100, test_name, "right child key should remain findable");
}
void test_repair_underflow_treats_nonempty_root_as_valid() {
    const std::string test_name = "repair underflow treats nonempty root as valid";
    auto tree = make_two_leaf_tree({0, 1, 2}, 3, {4, 5, 6});
    tree.repair_underflow(*tree.root_, {});

    check(tree.root_ && tree.root_->keys == std::vector<int>{3}, test_name,
          "nonempty root should not be repaired through an empty path");
    check(tree.find(0) == 0, test_name, "left child key should remain findable");
    check(tree.find(6) == 60, test_name, "right child key should remain findable");
}
void test_leaf_merge_repairs_non_root_parent_underflow() {
    const std::string test_name = "leaf merge repairs non-root parent underflow";
    auto tree = make_three_level_tree();
    tree.erase(61);

    check(!tree.find(61).has_value(), test_name, "erased key should not be found");
    check(tree.find(60) == 600, test_name, "merged separator should remain findable");
    check(tree.find(62) == 620, test_name, "current leaf key should remain findable");
    check(tree.find(100) == 1000, test_name, "root separator should remain findable");

    const auto* left_internal = tree.root_->children[0].get();
    check(left_internal->keys.size() >= gatidb::MIN_KEYS, test_name,
          "non-root parent should be repaired after losing a separator");
}
void test_insert_greater_than_root_separator_after_split() {
    const std::string test_name = "insert greater than root separator after split";
    gatidb::Btree tree;
    for (std::size_t i = 0; i <= gatidb::MAX_KEYS; ++i) {
        tree.insert(static_cast<int>(i), static_cast<int>(i * 10));
    }
    tree.insert(100, 1000);
    std::vector<int> keys;
    collect_keys(tree.root_.get(), keys);
    std::sort(keys.begin(), keys.end());
    check(std::binary_search(keys.begin(), keys.end(), 100), test_name,
          "key greater than all separators should be inserted into rightmost child");
}
void test_many_ascending_inserts_split_child_and_keep_all_keys() {
    const std::string test_name = "many ascending inserts split child and keep all keys";
    gatidb::Btree tree;
    for (int key = 0; key < 20; ++key) {
        tree.insert(key, key * 10);
    }
    check(tree.root_ != nullptr, test_name, "root should exist");
    if (!tree.root_) {
        return;
    }
    std::vector<int> keys;
    collect_keys(tree.root_.get(), keys);
    std::sort(keys.begin(), keys.end());
    std::vector<int> expected;
    for (int key = 0; key < 20; ++key) {
        expected.push_back(key);
    }
    check(keys == expected, test_name, "all keys 0 through 19 should remain in the tree");
    for (const auto& child : tree.root_->children) {
        check(child->keys.size() <= gatidb::MAX_KEYS, test_name,
              "no root child should contain more than MAX_KEYS keys");
    }
}
} // namespace
int main() {
    test_first_insert_initializes_leaf_root();
    test_leaf_keys_stay_sorted();
    test_root_does_not_exceed_node_capacity();
    test_insert_past_root_capacity_keeps_all_keys();
    test_root_split_shape_after_overflow_insert();
    test_inserting_existing_separator_does_not_duplicate_key();
    test_duplicate_key_updates_value_in_leaf_root();
    test_duplicate_key_updates_value_in_promoted_root();
    test_duplicate_key_updates_value_in_leaf_after_root_split();
    test_find_returns_values_and_missing_sentinel();
    test_erase_removes_key_from_leaf_root();
    test_erase_with_left_borrow_removes_target_and_preserves_order();
    test_erase_with_left_borrow_preserves_search_ranges();
    test_erase_with_right_borrow_preserves_search_ranges();
    test_erase_with_left_merge_preserves_search_ranges();
    test_erase_with_right_merge_preserves_search_ranges();
    test_root_may_keep_one_key_after_child_merge();
    test_repair_underflow_treats_nonempty_root_as_valid();
    test_leaf_merge_repairs_non_root_parent_underflow();
    test_insert_greater_than_root_separator_after_split();
    test_many_ascending_inserts_split_child_and_keep_all_keys();
    if (failures != 0) {
        std::cerr << failures << " test assertion(s) failed\n";
        return 1;
    }
    std::cout << "all btree tests passed\n";
    return 0;
}
