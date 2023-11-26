#include <iostream>
#include "letree_config.h"
#include "clevel.h"

namespace letree
{

  int CLevel::MemControl::file_id_ = 0;

  void CLevel::Node::PutChild(MemControl *mem, uint64_t key, const Node *child)
  {
    assert(type == Type::INDEX);
    assert(index_buf.entries < index_buf.max_entries);

    bool exist;
    int pos = index_buf.Find(key, exist);
    index_buf.Put(pos, key, (uint64_t)child - mem->BaseAddr());
  }

  CLevel::Node *CLevel::Node::Put(MemControl *mem, uint64_t key, uint64_t value, Node *parent)
  {
    if (type == Type::LEAF)
    {

      bool exist;
      int pos = leaf_buf.Find(key, exist);
      if (exist)
      {
        *(uint64_t *)leaf_buf.sort_pvalue(pos) = value;
        clflush(leaf_buf.sort_pvalue(pos));
        fence();
        return this;
      }

      leaf_buf.Put(pos, key, value);
      if (leaf_buf.entries == leaf_buf.max_entries)
      {
        // split
        Node *new_node = mem->NewNode(Type::LEAF, leaf_buf.suffix_bytes);
        leaf_buf.CopyData(&new_node->leaf_buf, leaf_buf.entries / 2);
        // set next pointer
        memcpy(new_node->next, next, sizeof(next));
        // persist new node
        clflush(new_node);
        clflush((uint8_t *)new_node + 64);
        fence();

        if (parent == nullptr)
        {
          Node *new_root = mem->NewNode(Type::INDEX, leaf_buf.suffix_bytes);
          uint64_t tmp = (uint64_t)this - mem->BaseAddr();
          // set first_child before Put, beacause Put will do clflush,
          // which contains first_child
          memcpy(new_root->first_child, &tmp, sizeof(new_root->first_child));
          new_root->PutChild(mem, new_node->leaf_buf.sort_key(0, key), new_node);
          // clflush root
          clflush(new_root);
          clflush((uint8_t *)new_root + 64);
          fence();

          SetNext(mem->BaseAddr(), new_node);
          leaf_buf.DeleteData(leaf_buf.entries / 2);
          return new_root;
        }
        else
        {
          parent->PutChild(mem, new_node->leaf_buf.sort_key(0, key), new_node);
          SetNext(mem->BaseAddr(), new_node);
          leaf_buf.DeleteData(leaf_buf.entries / 2);
          return new_node;
        }
      }
      else
      {
        return this;
      }
    }
    else if (type == Type::INDEX)
    {

      bool exist;
      int pos = index_buf.FindLE(key, exist);
      Node *child = GetChild(pos + 1, mem->BaseAddr());
      Node *new_child = child->Put(mem, key, value, this);
      if (new_child != child)
      {
        if (index_buf.entries == index_buf.max_entries)
        {
          // full, split
          Node *new_node = mem->NewNode(Type::INDEX, index_buf.suffix_bytes);
          // copy data to new_node
          index_buf.CopyData(&new_node->index_buf, (index_buf.entries + 1) / 2);
          // set new_node.first_child
          memcpy(new_node->first_child, index_buf.sort_pvalue((index_buf.entries + 1) / 2 - 1), sizeof(new_node->first_child));
          // persist new_node
          clflush(new_node);
          clflush((uint8_t *)new_node + 64);
          fence();

          if (parent == nullptr)
          {
            Node *new_root = mem->NewNode(Type::INDEX, index_buf.suffix_bytes);
            uint64_t tmp = (uint64_t)this - mem->BaseAddr();
            memcpy(new_root->first_child, &tmp, sizeof(new_root->first_child));
            new_root->PutChild(mem, index_buf.sort_key((index_buf.entries + 1) / 2 - 1, key), new_node);
            // clflush root
            clflush(new_root);
            clflush((uint8_t *)new_root + 64);
            fence();

            index_buf.DeleteData((index_buf.entries + 1) / 2 - 1);
            return new_root;
          }
          else
          {
            parent->PutChild(mem, index_buf.sort_key((index_buf.entries + 1) / 2 - 1, key), new_node);
            index_buf.DeleteData((index_buf.entries + 1) / 2 - 1);
            return new_node;
          }
        }
        else
        {
          return this;
        }
      }
      else
      {
        return this;
      }
    }
    else
    {
      assert(0);
      return this;
    }
  }

  bool CLevel::Node::Update(MemControl *mem, uint64_t key, uint64_t value)
  {
    Node *leaf = (Node *)FindLeaf(mem, key);
    bool exist;
    int pos = leaf->leaf_buf.Find(key, exist);
    if (exist)
      return leaf->leaf_buf.Update(pos, value);
    else
    {
      assert(0);
      return false;
    }
  }

  bool CLevel::Node::Get(MemControl *mem, uint64_t key, uint64_t &value) const
  {
    const Node *leaf = FindLeaf(mem, key);
    bool exist;
    int pos = leaf->leaf_buf.Find(key, exist);
    if (exist)
    {
      value = leaf->leaf_buf.sort_value(pos);
      return true;
    }
    else
    {
      return false;
    }
  }

  bool CLevel::Node::Delete(MemControl *mem, uint64_t key, uint64_t *value)
  {
    Node *leaf = (Node *)FindLeaf(mem, key);
    bool exist;
    int pos = leaf->leaf_buf.Find(key, exist);
    if (exist && value)
      *value = leaf->leaf_buf.sort_value(pos);
    return exist ? leaf->leaf_buf.Delete(pos) : true;
  }

  /******************** CLevel ********************/
  CLevel::CLevel()
  {
    // set root to NULL: set LSB to 1
    root_[0] = 1;
  }

  void CLevel::Setup(MemControl *mem, int suffix_len)
  {
    uint64_t new_root = (uint64_t)mem->NewNode(Node::Type::LEAF, suffix_len);
    // set next to NULL: set LSB to 1
    ((Node *)new_root)->next[0] = 1;
    new_root -= mem->BaseAddr();
    memcpy(root_, &new_root, sizeof(root_));
    clflush(&root_);
    fence();
  }

  void CLevel::Setup(MemControl *mem, KVBuffer<112, 8> &blevel_buf)
  {
    Node *new_root = mem->NewNode(Node::Type::LEAF, blevel_buf.suffix_bytes);
    assert(sizeof(new_root->leaf_buf.buf) == sizeof(blevel_buf.buf));
    new_root->leaf_buf.FromKVBuffer(blevel_buf);
    clflush(new_root);
    clflush((uint8_t *)new_root + 64);

    // set next to NULL: set LSB to 1
    new_root->next[0] = 1;
    new_root = (Node *)((uint64_t)new_root - mem->BaseAddr());
    memcpy(root_, &new_root, sizeof(root_));
    clflush(&root_);
    fence();
  }

  bool CLevel::Put(MemControl *mem, uint64_t key, uint64_t value)
  {
    Node *old_root = root(mem->BaseAddr());
    Node *new_root = old_root->Put(mem, key, value, nullptr);
    if (old_root != new_root)
    {
      new_root = (Node *)((uint64_t)new_root - mem->BaseAddr());
      memcpy(root_, &new_root, sizeof(root_));
      clflush(&root_);
      fence();
    }
    return true;
  }

} // namespace letree