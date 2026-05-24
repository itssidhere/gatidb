#pragma once

#include <memory>
#include <vector>

namespace gatidb {
constexpr std::size_t MAX_KEYS = 6;
class Btree {
  public:
    void insert(int key, int value);

  private:
    struct Node {
        bool is_leaf;
        std::vector<int> keys;
        std::vector<int> values;
        std::vector<std::unique_ptr<Node>> children;
    };
    std::unique_ptr<Node> root_;
    void split_root();
    void split_child(Node* parent, std::size_t child_index);
    void update_value_at_node(Node* parent, std::size_t index, int value);
};
} // namespace gatidb