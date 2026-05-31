#include "gatidb/btree.hpp"
#include <algorithm>
#include <cstddef>
#include <iterator>
#include <memory>
#include <optional>
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
            update_value_at_node(current, index, value);
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
    if (it != current->keys.end() && current->keys[index] == key) {
        update_value_at_node(current, index, value);
    } else {
        current->keys.insert(advance_by(current->keys.begin(), index), key);
        current->values.insert(advance_by(current->values.begin(), index), value);
    }
}
std::optional<int> Btree::find(int key) const {
    auto cursor = seek(key);
    if (!cursor.found) {
        return std::nullopt;
    }
    return cursor.node->values[cursor.index];
}
void Btree::erase(int key) {
    auto cursor = seek(key);
    if (!cursor.found || !cursor.node->is_leaf) {
        return;
    }
    // will deletion satisfy the invariant?
    cursor.node->keys.erase(advance_by(cursor.node->keys.begin(), cursor.index));
    cursor.node->values.erase(advance_by(cursor.node->values.begin(), cursor.index));
    auto n = cursor.node->keys.size() - 1;
    if (n <= MAX_KEYS && n >= MIN_KEYS) {
        return;
    } else {
        // can i use my siblings?
        // left borrow
        if (cursor.child_index > 0 &&
            cursor.parent->children[cursor.child_index - 1]->keys.size() > MIN_KEYS) {
            // i can borrow one node from left child
            auto last_index_of_left_sibling =
                cursor.parent->children[cursor.child_index - 1]->keys.size() - 1;
            auto sibling_key =
                cursor.parent->children[cursor.child_index - 1]->keys[last_index_of_left_sibling];
            auto sibling_value =
                cursor.parent->children[cursor.child_index - 1]->values[last_index_of_left_sibling];
            cursor.parent->children[cursor.child_index - 1]->keys.erase(
                advance_by(cursor.parent->children[cursor.child_index - 1]->keys.begin(),
                           last_index_of_left_sibling));
            cursor.parent->children[cursor.child_index - 1]->values.erase(
                advance_by(cursor.parent->children[cursor.child_index - 1]->values.begin(),
                           last_index_of_left_sibling));
            // get root node key and value at the index
            auto root_key = cursor.parent->keys[cursor.child_index - 1];
            auto root_value = cursor.parent->values[cursor.child_index - 1];
            // put the sibling key and value in the root
            cursor.parent->keys[cursor.child_index - 1] = sibling_key;
            cursor.parent->values[cursor.child_index - 1] = sibling_value;
            cursor.node->keys.insert(cursor.node->keys.begin(), root_key);
            cursor.node->values.insert(cursor.node->values.begin(), root_value);
        }
    }
}
Btree::ConstCursor Btree::seek(int key) const {
    return seek_impl<const Node, ConstCursor>(root_.get(), key);
}
Btree::Cursor Btree::seek(int key) {
    return seek_impl<Node, Cursor>(root_.get(), key);
}
template <typename NodeType, typename CursorType>
CursorType Btree::seek_impl(NodeType* root, int key) const {
    NodeType* current = root;
    NodeType* parent = nullptr;
    std::size_t child_index = 0;
    while (current) {
        auto it = std::lower_bound(current->keys.begin(), current->keys.end(), key);
        auto index = static_cast<std::size_t>(it - current->keys.begin());
        if (it != current->keys.end() && current->keys[index] == key) {
            return CursorType{current, parent, index, child_index, true};
        }
        if (current->is_leaf) {
            break;
        }
        parent = current;
        child_index = index;
        current = current->children[index].get();
    }
    return CursorType{};
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
void Btree::update_value_at_node(Node* node, std::size_t index, int value) {
    node->values[index] = value;
}
} // namespace gatidb
