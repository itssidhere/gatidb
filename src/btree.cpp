#include "gatidb/btree.hpp"
#include <algorithm>
#include <iterator>
#include <memory>
#include <vector>

namespace gatidb {
void Btree::insert(int key, int value) {
    if (!root_) {
        root_ = std::make_unique<Node>();
        root_->is_leaf = true;
    }

    if (root_->keys.size() == MAX_KEYS) {
        split_root();
    }

    auto current = root_.get();

    while (current->is_leaf != true) {
        auto it = std::lower_bound(current->keys.begin(), current->keys.end(), key);
        const auto index = static_cast<std::size_t>(it - current->keys.begin());
        current = current->children[index].get();

        if (current->keys.size() == MAX_KEYS) {
            split_child(current);
        }
    }

    auto it = std::lower_bound(current->keys.begin(), current->keys.end(), key);
    auto index = it - current->keys.begin();

    current->keys.insert(current->keys.begin() + index, key);
    current->values.insert(current->values.begin() + index, value);
}
void Btree::split_root() {
    auto old_root = std::move(root_);
    root_ = std::make_unique<Node>();
    const auto median = old_root->keys.size() / 2;
    root_->keys.push_back(old_root->keys[median]);
    root_->values.push_back(old_root->values[median]);
    root_->is_leaf = false;

    auto left = std::make_unique<Node>();
    auto right = std::make_unique<Node>();

    auto left_keys = std::vector<int>(old_root->keys.begin(), old_root->keys.begin() + median);
    auto right_keys = std::vector<int>(old_root->keys.begin() + median + 1, old_root->keys.end());

    auto left_values =
        std::vector<int>(old_root->values.begin(), old_root->values.begin() + median);
    auto right_values =
        std::vector<int>(old_root->values.begin() + median + 1, old_root->values.end());

    left->keys = std::move(left_keys);
    left->values = std::move(left_values);

    right->keys = std::move(right_keys);
    right->values = std::move(right_values);

    if (old_root->is_leaf == false) {
        std::move(old_root->children.begin(), old_root->children.begin() + median + 1,
                  std::back_inserter(left->children));

        std::move(old_root->children.begin() + median + 1, old_root->children.end(),
                  std::back_inserter(right->children));

        left->is_leaf = false;
        right->is_leaf = false;
    } else {
        left->is_leaf = true;
        right->is_leaf = true;
    }

    root_->children.push_back(std::move(left));
    root_->children.push_back(std::move(right));
}
void Btree::split_child(Node* node) {}
} // namespace gatidb