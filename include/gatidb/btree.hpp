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

  private:
    struct Node {
        bool is_leaf;
        std::vector<int> keys;
        std::vector<int> values;
        std::vector<std::unique_ptr<Node>> children;
    };
    template <typename NodeType> struct CursorBase {
        NodeType* node = nullptr;
        NodeType* parent = nullptr;
        std::size_t index = 0;
        std::size_t child_index = 0;
        bool found = false;
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
    void borrow_leaf_from_left(const Cursor& cursor);
    void borrow_leaf_from_right(const Cursor& cursor);
};
} // namespace gatidb