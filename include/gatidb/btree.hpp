#pragma once
#include <memory>
#include <optional>
#include <vector>
namespace gatidb {
constexpr std::size_t MIN_DEGREE = 4;
constexpr std::size_t MAX_KEYS = 2 * MIN_DEGREE - 1;
constexpr std::size_t MIN_KEYS = MIN_DEGREE - 1;
class Btree {
  public:
    void insert(int key, int value);
    std::optional<int> find(int key) const;
    void erase(int key);
    bool is_valid() const;

  private:
    struct Node {
        bool is_leaf;
        std::vector<int> keys;
        std::vector<int> values;
        std::vector<std::unique_ptr<Node>> children;
    };
    template <typename NodeType> struct PathEntry {
        NodeType* node;
        std::size_t child_index;
    };
    template <typename NodeType> struct CursorBase {
        NodeType* node = nullptr;
        std::size_t index = 0;
        bool found = false;
        std::vector<PathEntry<NodeType>> path;
    };

    using Cursor = CursorBase<Node>;
    using ConstCursor = CursorBase<const Node>;
    std::unique_ptr<Node> root_;
    Cursor seek(int key);
    ConstCursor seek(int key) const;
    template <typename NodeType, typename CursorType>
    CursorType seek_impl(NodeType* root, int key) const;
    void split_root();
    void split_child(Node* parent, std::size_t child_index);
    void update_value_at_node(Node* parent, std::size_t index, int value);
    void borrow_leaf_from_left(Node& node, const std::vector<PathEntry<Node>>& path);
    void borrow_leaf_from_right(Node& node, const std::vector<PathEntry<Node>>& path);
    void repair_underflow(Node& node, std::vector<PathEntry<Node>> path);
    bool is_valid_node(const Node* node, const std::optional<int> min_allowed,
                       const std::optional<int> max_allowed, std::size_t depth,
                       std::optional<std::size_t>& leaf_depth) const;
};
} // namespace gatidb
