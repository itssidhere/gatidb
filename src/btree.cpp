#include "gatidb/btree.hpp"
#include <algorithm>
#include <cstddef>
#include <iterator>
#include <memory>
#include <vector>

namespace {
template <typename Iter> Iter advance_by(Iter it, std::size_t offset) {
    return std::next(it, static_cast<std::ptrdiff_t>(offset));
}
} // namespace
namespace gatidb {
void Btree::insert(int key, int value) {
    if (!root_) {
        root_ = std::make_unique<Node>();
        root_->is_leaf = true;
    }

    if (root_->keys.size() == MAX_KEYS) {
        // we follow the convention of splitting before descending to next node because if a split
        // is needed in the child node we will have to propogate the changes back until the parent
        // is not overflowing. this can be a waste of memory and time
        split_root();
    }

    auto current = root_.get();

    while (current->is_leaf != true) {
        auto it = std::lower_bound(current->keys.begin(), current->keys.end(), key);
        const auto index = static_cast<std::size_t>(it - current->keys.begin());
        if (it != current->keys.end() && current->keys[index] == key) {
            // duplicate key. for now we will not do anything later we will update the value
            return;
        }
        auto nxt = current->children[index].get();

        if (nxt->keys.size() == MAX_KEYS) {
            split_child(current, index);
        } else {
            current = nxt;
        }
    }

    auto it = std::lower_bound(current->keys.begin(), current->keys.end(), key);
    const auto index = static_cast<std::size_t>(it - current->keys.begin());

    current->keys.insert(advance_by(current->keys.begin(), index), key);
    current->values.insert(advance_by(current->values.begin(), index), value);
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

    const auto key_mid = advance_by(old_root->keys.begin(), median);
    const auto key_after_mid = advance_by(old_root->keys.begin(), median + 1);
    const auto value_mid = advance_by(old_root->values.begin(), median);
    const auto value_after_mid = advance_by(old_root->values.begin(), median + 1);

    auto left_keys = std::vector<int>(old_root->keys.begin(), key_mid);
    auto right_keys = std::vector<int>(key_after_mid, old_root->keys.end());

    auto left_values = std::vector<int>(old_root->values.begin(), value_mid);
    auto right_values = std::vector<int>(value_after_mid, old_root->values.end());

    left->keys = std::move(left_keys);
    left->values = std::move(left_values);

    right->keys = std::move(right_keys);
    right->values = std::move(right_values);

    if (old_root->is_leaf == false) {
        const auto child_mid = advance_by(old_root->children.begin(), median + 1);

        std::move(old_root->children.begin(), child_mid, std::back_inserter(left->children));

        std::move(child_mid, old_root->children.end(), std::back_inserter(right->children));

        left->is_leaf = false;
        right->is_leaf = false;
    } else {
        left->is_leaf = true;
        right->is_leaf = true;
    }

    root_->children.push_back(std::move(left));
    root_->children.push_back(std::move(right));
}
void Btree::split_child(Node* parent, std::size_t child_index) {
    auto child = std::move(parent->children[child_index]);
    auto median = child->keys.size() / 2;

    auto median_key = child->keys[median];
    auto median_value = child->values[median];

    parent->keys.insert(advance_by(parent->keys.begin(), child_index), median_key);
    parent->values.insert(advance_by(parent->values.begin(), child_index), median_value);

    auto left = std::make_unique<Node>();
    auto right = std::make_unique<Node>();

    const auto key_mid = advance_by(child->keys.begin(), median);
    const auto key_after_mid = advance_by(child->keys.begin(), median + 1);
    const auto value_mid = advance_by(child->values.begin(), median);
    const auto value_after_mid = advance_by(child->values.begin(), median + 1);

    auto left_keys = std::vector<int>(child->keys.begin(), key_mid);
    auto left_values = std::vector<int>(child->values.begin(), value_mid);

    auto right_keys = std::vector<int>(key_after_mid, child->keys.end());
    auto right_values = std::vector<int>(value_after_mid, child->values.end());

    left->keys = std::move(left_keys);
    left->values = std::move(left_values);

    right->keys = std::move(right_keys);
    right->values = std::move(right_values);

    if (child->is_leaf == false) {
        const auto child_mid = advance_by(child->children.begin(), median + 1);

        std::move(child->children.begin(), child_mid, std::back_inserter(left->children));

        std::move(child_mid, child->children.end(), std::back_inserter(right->children));

        left->is_leaf = false;
        right->is_leaf = false;

    } else {
        left->is_leaf = true;
        right->is_leaf = true;
    }

    // remove the old children ref now
    parent->children.erase(advance_by(parent->children.begin(), child_index));

    // insert left
    parent->children.insert(advance_by(parent->children.begin(), child_index), std::move(left));

    // insert right
    parent->children.insert(advance_by(parent->children.begin(), child_index + 1),
                            std::move(right));
}
} // namespace gatidb
