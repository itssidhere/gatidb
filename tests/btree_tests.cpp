#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
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

    check(keys == std::vector<int>{0, 1, 2, 3, 4, 5, 6}, test_name,
          "all inserted keys 0 through 6 should still exist after overflow insert");
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

} // namespace

int main() {
    test_first_insert_initializes_leaf_root();
    test_leaf_keys_stay_sorted();
    test_root_does_not_exceed_node_capacity();
    test_insert_past_root_capacity_keeps_all_keys();
    test_root_split_shape_after_overflow_insert();
    test_inserting_existing_separator_does_not_duplicate_key();

    if (failures != 0) {
        std::cerr << failures << " test assertion(s) failed\n";
        return 1;
    }

    std::cout << "all btree tests passed\n";
    return 0;
}
