#include "primer/trie.h"
#include <cstddef>
#include <memory>
#include <string_view>
#include <utility>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  auto node = root_;
  size_t index = 0;

  while (index < key.size()) {
    if (node == nullptr) {
      return nullptr;
    }
    auto c = key.at(index);
    auto search = node->children_.find(c);
    if (search == node->children_.end()) {
      return nullptr;
    }
    node = search->second;
    index += 1;
  }

  // node现在的类型是shared_ptr<const TrieNode>
  // 需要使用类型转换才能获取value
  auto result = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
  if (result == nullptr) {
    return nullptr;
  }
  if (result->is_value_node_) {
    return result->value_.get();
  }
  return nullptr;
}

template <class T>
auto PutHelper(std::unique_ptr<TrieNode> node, std::string_view key, size_t index, std::shared_ptr<T> value)
    -> std::unique_ptr<TrieNode> {
  // 已经到达字符串尾，插入数据即可
  if (index == key.size()) {
    return std::make_unique<TrieNodeWithValue<T>>(node->children_, std::move(value));
  }

  std::unique_ptr<TrieNode> child;  // 准备插入的子节点
  auto c = key.at(index);           // 当前使用的char

  // 将value插入子trie中
  if (auto search = node->children_.find(c); search != node->children_.end()) {
    child = search->second->Clone();
  } else {
    child = std::make_unique<TrieNode>();
  }
  child = PutHelper(std::move(child), key, index + 1, value);

  // 用插入了value的子trie替换现在的子trie
  if (auto search = node->children_.find(c); search != node->children_.end()) {
    search->second = std::shared_ptr<TrieNode>(std::move(child));
  } else {
    node->children_[c] = std::move(child);
  }

  return node;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  // 用于插入value的根节点
  std::unique_ptr<TrieNode> root;
  if (this->root_ == nullptr) {
    root = std::make_unique<TrieNode>();
  } else {
    root = root_->Clone();
  }

  auto value_shared = std::make_shared<T>(std::move(value));
  root = PutHelper(std::move(root), key, 0, value_shared);
  return Trie(std::move(root));
}

// recursively delete
auto RemoveHelper(std::unique_ptr<TrieNode> node, std::string_view key, size_t index) -> std::unique_ptr<TrieNode> {
  if (index == key.size()) {
    // no more child, just delete
    if (node->children_.empty()) {
      return nullptr;
    }
    return std::make_unique<TrieNode>(node->children_);
  }

  std::unique_ptr<TrieNode> child;
  auto c = key.at(index);

  auto search = node->children_.find(c);
  if (search != node->children_.end()) {
    child = search->second->Clone();
    child = RemoveHelper(std::move(child), key, index + 1);
  } else {
    // 没有对应子节点不需要执行删除操作
    return node;
  }

  if (child == nullptr) {
    node->children_.erase(search);
    // 是否还有别的子节点？
    if (node->children_.empty() && !(node->is_value_node_)) {
      return nullptr;
    }
    return node;
  }

  node->children_[c] = std::move(child);
  return node;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  if (root_ == nullptr) {
    return Trie(nullptr);
  }

  auto root = root_->Clone();
  root = RemoveHelper(std::move(root), key, 0);
  return Trie(std::move(root));
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked
// up by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
