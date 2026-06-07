#include "gatidb/btree.hpp"
#include <algorithm>
#include <cstddef>
#include <iterator>
#include <memory>
#include <optional>
#include <utility>
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
    cursor.node->keys.erase(advance_by(cursor.node->keys.begin(), cursor.index));
    cursor.node->values.erase(advance_by(cursor.node->values.begin(), cursor.index));

    auto n = cursor.node->keys.size();
    if ((n <= MAX_KEYS && n >= MIN_KEYS) || cursor.path.empty()) {
        return;
    } else {
        repair_underflow(*cursor.node, cursor.path);
    }
}

void Btree::repair_underflow(Node& node, std::vector<PathEntry<Node>> path) {
    if (path.empty()) {
        return;
    }
    auto parent = path.back().node;
    auto child_index = path.back().child_index;
    auto can_borrow_from_left =
        child_index > 0 && parent->children[child_index - 1]->keys.size() > MIN_KEYS;

    auto can_borrow_from_right = child_index < parent->children.size() - 1 &&
                                 parent->children[child_index + 1]->keys.size() > MIN_KEYS;

    if (can_borrow_from_left) {
        borrow_leaf_from_left(node, path);
    } else if (can_borrow_from_right) {
        borrow_leaf_from_right(node, path);
    } else {
        auto is_left_merge_possible = child_index > 0;
        if (is_left_merge_possible) {
            auto separator_key = parent->keys[child_index - 1];
            auto separator_value = parent->values[child_index - 1];
            auto left = parent->children[child_index - 1].get();
            left->keys.push_back(separator_key);
            left->values.push_back(separator_value);
            left->keys.insert(left->keys.end(), node.keys.begin(), node.keys.end());
            left->values.insert(left->values.end(), node.values.begin(), node.values.end());

            if (!left->is_leaf) {
                std::move(node.children.begin(), node.children.end(),
                          std::back_inserter(left->children));

                node.children.clear();
            }

            parent->keys.erase(advance_by(parent->keys.begin(), child_index - 1));
            parent->values.erase(advance_by(parent->values.begin(), child_index - 1));

            parent->children.erase(advance_by(parent->children.begin(), child_index));

        } else {
            auto separator_key = parent->keys[child_index];
            auto separator_value = parent->values[child_index];
            auto right = parent->children[child_index + 1].get();
            right->keys.insert(right->keys.begin(), separator_key);
            right->values.insert(right->values.begin(), separator_value);
            right->keys.insert(right->keys.begin(), node.keys.begin(), node.keys.end());
            right->values.insert(right->values.begin(), node.values.begin(), node.values.end());

            if (!right->is_leaf) {
                std::move(node.children.begin(), node.children.end(),
                          std::inserter(right->children, right->children.begin()));

                node.children.clear();
            }

            parent->keys.erase(advance_by(parent->keys.begin(), child_index));
            parent->values.erase(advance_by(parent->values.begin(), child_index));
            parent->children.erase(advance_by(parent->children.begin(), child_index));
        }

        if (parent == root_.get() && root_->keys.empty() && root_->children.size() == 1) {
            root_ = std::move(root_->children[0]);
        } else if (parent->keys.size() < MIN_KEYS) {
            path.pop_back();
            repair_underflow(*parent, path);
        }
    }
}

void Btree::borrow_leaf_from_left(Node& node, const std::vector<PathEntry<Node>>& path) {
    auto parent = path.back().node;
    auto child_index = path.back().child_index;
    auto separator_index = child_index - 1;
    auto last_index_of_left_sibling = parent->children[child_index - 1]->keys.size() - 1;
    auto sibling_key = parent->children[child_index - 1]->keys[last_index_of_left_sibling];
    auto sibling_value = parent->children[child_index - 1]->values[last_index_of_left_sibling];
    parent->children[child_index - 1]->keys.erase(
        advance_by(parent->children[child_index - 1]->keys.begin(), last_index_of_left_sibling));
    parent->children[child_index - 1]->values.erase(
        advance_by(parent->children[child_index - 1]->values.begin(), last_index_of_left_sibling));
    // get root node key and value at the index
    auto root_key = parent->keys[separator_index];
    auto root_value = parent->values[separator_index];
    // put the sibling key and value in the root
    parent->keys[child_index - 1] = sibling_key;
    parent->values[child_index - 1] = sibling_value;
    node.keys.insert(node.keys.begin(), root_key);
    node.values.insert(node.values.begin(), root_value);

    if (!node.is_leaf) {
        auto& left_children = parent->children[child_index - 1]->children;
        node.children.insert(node.children.begin(), std::move(left_children.back()));
        left_children.pop_back();
    }
}

void Btree::borrow_leaf_from_right(Node& node, const std::vector<PathEntry<Node>>& path) {
    auto parent = path.back().node;
    auto child_index = path.back().child_index;
    auto separator_index = child_index;
    std::size_t last_index_of_right_sibling = 0;
    auto sibling_key = parent->children[child_index + 1]->keys[last_index_of_right_sibling];
    auto sibling_value = parent->children[child_index + 1]->values[last_index_of_right_sibling];

    parent->children[child_index + 1]->keys.erase(parent->children[child_index + 1]->keys.begin());
    parent->children[child_index + 1]->values.erase(
        parent->children[child_index + 1]->values.begin());
    // get root node key and value at the index
    auto root_key = parent->keys[separator_index];
    auto root_value = parent->values[separator_index];
    // put the sibling key and value in the root
    parent->keys[child_index] = sibling_key;
    parent->values[child_index] = sibling_value;
    node.keys.insert(node.keys.end(), root_key);
    node.values.insert(node.values.end(), root_value);

    if (!node.is_leaf) {
        auto& right_children = parent->children[child_index + 1]->children;
        node.children.push_back(std::move(right_children.front()));
        right_children.erase(right_children.begin());
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
    std::vector<PathEntry<NodeType>> path;
    while (current) {
        auto it = std::lower_bound(current->keys.begin(), current->keys.end(), key);
        auto index = static_cast<std::size_t>(it - current->keys.begin());
        if (it != current->keys.end() && current->keys[index] == key) {
            return CursorType{current, index, true, path};
        }
        if (current->is_leaf) {
            break;
        }

        path.push_back({current, index});
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
bool Btree::is_valid() const {
    if (root_ == nullptr) {
        return true;
    }

    if (root_->keys.size() != root_->values.size()) {
        return false;
    }

    if (root_->keys.size() > MAX_KEYS) {
        return false;
    }

    if (root_->is_leaf) {
        return root_->children.empty();
    }

    if (root_->keys.empty()) {
        return false;
    }

    if (root_->children.size() != root_->keys.size() + 1) {
        return false;
    }

    if (!std::is_sorted(root_->keys.begin(), root_->keys.end())) {
        return false;
    }

    if (std::adjacent_find(root_->keys.begin(), root_->keys.end()) != root_->keys.end()) {
        return false;
    }

    std::optional<std::size_t> leaf_depth;
    for (std::size_t i = 0; i < root_->children.size(); i++) {
        std::optional<int> min_allowed = std::nullopt;
        std::optional<int> max_allowed = std::nullopt;
        if (i > 0) {
            min_allowed = root_->keys[i - 1];
        }
        if (i < root_->keys.size()) {
            max_allowed = root_->keys[i];
        }
        if (root_->children[i] == nullptr ||
            !is_valid_node(root_->children[i].get(), min_allowed, max_allowed, 1, leaf_depth)) {
            return false;
        }
    }
    return true;
}
bool Btree::is_valid_node(const Node* node, const std::optional<int> min_allowed,
                          const std::optional<int> max_allowed, std::size_t depth,
                          std::optional<std::size_t>& leaf_depth) const {
    if (node->keys.size() != node->values.size()) {
        return false;
    }

    if (node->keys.size() < MIN_KEYS) {
        return false;
    }

    if (node->keys.size() > MAX_KEYS) {
        return false;
    }

    if (!std::is_sorted(node->keys.begin(), node->keys.end())) {
        return false;
    }

    if (std::adjacent_find(node->keys.begin(), node->keys.end()) != node->keys.end()) {
        return false;
    }

    for (const auto key : node->keys) {
        if (min_allowed.has_value() && key <= min_allowed.value()) {
            return false;
        }
        if (max_allowed.has_value() && key >= max_allowed.value()) {
            return false;
        }
    }

    if (node->is_leaf) {
        if (!node->children.empty()) {
            return false;
        }
        if (!leaf_depth.has_value()) {
            leaf_depth = depth;
            return true;
        }
        return depth == leaf_depth.value();
    }

    if (node->keys.size() + 1 != node->children.size()) {
        return false;
    }

    for (std::size_t i = 0; i < node->children.size(); i++) {
        auto child_min_allowed = min_allowed;
        auto child_max_allowed = max_allowed;
        if (i > 0) {
            child_min_allowed = node->keys[i - 1];
        }
        if (i < node->keys.size()) {
            child_max_allowed = node->keys[i];
        }
        if (node->children[i] == nullptr ||
            !is_valid_node(node->children[i].get(), child_min_allowed, child_max_allowed, depth + 1,
                           leaf_depth)) {
            return false;
        }
    }

    return true;
}
} // namespace gatidb
