#ifndef _BPTREE_TREE_H_
#define _BPTREE_TREE_H_

#include "bptree/page_cache.h"
#include "bptree/tree_node.h"

#include <cassert>
#include <iostream>

namespace bptree {

template <unsigned int N, typename K, typename V,
          typename KeySerializer = CopySerializer<K>,
          typename KeyComparator = std::less<K>,
          typename KeyEq = std::equal_to<K>,
          typename ValueSerializer = CopySerializer<V>>
class BTree {
public:
    BTree(AbstractPageCache* page_cache) : page_cache(page_cache)
    {
        bool create = !read_metadata();

        if (create) {
            {
                boost::upgrade_lock<Page> lock;
                auto page = page_cache->new_page(lock);
                assert(page->get_id() == META_PAGE_ID);
            }

            root = create_node<LeafNode<N, K, V, KeySerializer, KeyComparator,
                                        KeyEq, ValueSerializer>>(nullptr);
            num_pairs.store(0);
            write_metadata();
        }
    }

    ~BTree() { write_metadata(); }

    size_t size() const { return num_pairs.load(); }

    template <
        typename T,
        typename std::enable_if<std::is_base_of<
            BaseNode<K, V, KeyComparator, KeyEq>, T>::value>::type* = nullptr>
    std::unique_ptr<T> create_node(BaseNode<K, V, KeyComparator, KeyEq>* parent)
    {
        boost::upgrade_lock<Page> lock;
        auto page = page_cache->new_page(lock);
        auto node = std::make_unique<T>(this, parent, page->get_id());
        page_cache->unpin_page(page, false, lock);

        return node;
    }

    // Helper method to prefetch nodes along a search path
    void prefetch_search_path(const K& key) {
        auto* node = root.get();
        std::vector<PageID> pages_to_prefetch;
        
        while (node && !node->is_leaf()) {
            auto* inner_node = static_cast<InnerNode<N, K, V, KeySerializer, KeyComparator, KeyEq, ValueSerializer>*>(node);
            
            // Find the child index for this key
            int child_idx = std::distance(
                inner_node->keys.begin(),
                std::upper_bound(inner_node->keys.begin(), 
                                inner_node->keys.begin() + inner_node->get_size(),
                                key, 
                                inner_node->kcmp)
            );
            
            // Prefetch this child
            if (inner_node->child_pages[child_idx] != Page::INVALID_PAGE_ID) {
                pages_to_prefetch.push_back(inner_node->child_pages[child_idx]);
            }
            
            // Also prefetch siblings if available - this helps with range scans
            if (child_idx > 0 && inner_node->child_pages[child_idx-1] != Page::INVALID_PAGE_ID) {
                pages_to_prefetch.push_back(inner_node->child_pages[child_idx-1]);
            }
            
            if (child_idx < inner_node->get_size() && 
                inner_node->child_pages[child_idx+1] != Page::INVALID_PAGE_ID) {
                pages_to_prefetch.push_back(inner_node->child_pages[child_idx+1]);
            }
            
            // Move to the next node in the path
            if (inner_node->child_cache[child_idx]) {
                node = inner_node->child_cache[child_idx].get();
            } else {
                break; // We can't continue the path prediction
            }
        }
        
        // Request prefetch for all identified pages
        if (!pages_to_prefetch.empty()) {
            page_cache->prefetch_pages(pages_to_prefetch);
        }
    }

    void get_value(const K& key, std::vector<V>& value_list)
    {
        prefetch_search_path(key);

        while (true) {
            try {
                value_list.clear();
                auto* root_node = root.get();
                root_node->get_values(key, false, nullptr, nullptr, value_list,
                                      0);
                if (root_node != root.get()) continue;
                break;
            } catch (OLCRestart&) {
                continue;
            }
        }
    }

    void collect_values(const K& key, std::optional<K>* next_key,
                        std::vector<K>& key_list, std::vector<V>& value_list)
    {
        while (true) {
            try {
                key_list.clear();
                value_list.clear();
                auto* root_node = root.get();
                root_node->get_values(key, true, next_key, &key_list,
                                      value_list, 0);
                if (root_node != root.get()) continue;
                break;
            } catch (OLCRestart&) {
                continue;
            }
        }
    }

    void insert(const K& key, const V& value)
    {
        while (true) {
            try {
                K split_key;
                auto old_root = root.get();
                if (!old_root)
                    continue; /* old_root may be nullptr when another thread is
                                 updating the root node pointer */

                auto root_sibling = old_root->insert(key, value, split_key, 0);

                if (root_sibling) {
                    auto new_root =
                        create_node<InnerNode<N, K, V, KeySerializer,
                                              KeyComparator, KeyEq>>(nullptr);

                    root->set_parent(new_root.get());
                    root_sibling->set_parent(new_root.get());

                    new_root->set_size(1);
                    new_root->keys[0] = split_key;
                    new_root->child_pages[0] = root->get_pid();
                    new_root->child_pages[1] = root_sibling->get_pid();
                    new_root->child_cache[0] = std::move(root);
                    new_root->child_cache[1] = std::move(root_sibling);

                    root = std::move(new_root);
                    write_node(root.get());
                    write_metadata();

                    /* release the lock on the old root */
                    old_root->write_unlock();
                    continue;
                }

                num_pairs++;
                write_metadata();
                break;
            } catch (OLCRestart&) {
                continue;
            }
        }
    }

    void print(std::ostream& os) const
    {
        while (true) {
            try {
                root->print(os, "");
                break;
            } catch (OLCRestart&) {
                continue;
            }
        }
    } /* for debug purpose */
    friend std::ostream& operator<<(std::ostream& os, BTree& tree)
    {
        tree.print(os);
        return os;
    }

    std::unique_ptr<BaseNode<K, V, KeyComparator, KeyEq>>
    read_node(BaseNode<K, V, KeyComparator, KeyEq>* parent, PageID pid)
    {
        boost::upgrade_lock<Page> lock;
        auto page = page_cache->fetch_page(pid, lock);

        if (!page) {
            return nullptr;
        }
        const auto* buf = page->get_buffer(lock);

        uint32_t tag = *reinterpret_cast<const uint32_t*>(buf);
        std::unique_ptr<BaseNode<K, V, KeyComparator, KeyEq>> node;

        if (tag == INNER_TAG) {
            node = std::make_unique<InnerNode<
                N, K, V, KeySerializer, KeyComparator, KeyEq, ValueSerializer>>(
                this, parent, pid);
        } else if (tag == LEAF_TAG) {
            node =
                std::make_unique<LeafNode<N, K, V, KeySerializer, KeyComparator,
                                          KeyEq, ValueSerializer>>(this, parent,
                                                                   pid);
        }

        node->deserialize(&buf[sizeof(uint32_t)],
                          page->get_size() - sizeof(uint32_t));

        page_cache->unpin_page(page, false, lock);
        return node;
    }

    void write_node(const BaseNode<K, V, KeyComparator, KeyEq>* node)
    {
        boost::upgrade_lock<Page> lock;
        auto page = page_cache->fetch_page(node->get_pid(), lock);

        {
            boost::upgrade_to_unique_lock<Page> ulock(lock);
            if (!page) return;
            auto* buf = page->get_buffer(ulock);
            uint32_t tag = node->is_leaf() ? LEAF_TAG : INNER_TAG;

            *reinterpret_cast<uint32_t*>(buf) = tag;
            node->serialize(&buf[sizeof(uint32_t)],
                            page->get_size() - sizeof(uint32_t));
        }

        page_cache->unpin_page(page, true, lock);
    }


    /* iterator interface */
    class iterator {
        friend class BTree<N, K, V, KeySerializer, KeyComparator, KeyEq,
                           ValueSerializer>;

    public:
        using self_type = iterator;
        using value_type = std::pair<K, V>;
        using reference = value_type&;
        using pointer = value_type*;
        using iterator_category = std::forward_iterator_tag;
        using difference_type = int;

        self_type operator++()
        {
            self_type i = *this;
            inc();
            return i;
        }
        self_type operator++(int _unused)
        {
            inc();
            return *this;
        }
        reference operator*() { return kvp; }
        pointer operator->() { return &kvp; }
        bool operator==(const self_type& rhs) { return false; }
        bool operator!=(const self_type& rhs) { return true; }
        bool is_end() const { return ended; }

    private:
        std::vector<K> key_buf;
        std::vector<V> value_buf;
        size_t idx;
        value_type kvp;
        std::optional<K> next_key;
        bool ended;
        KeyComparator kcmp;

        using container_type = BTree<N, K, V, KeySerializer, KeyComparator,
                                     KeyEq, ValueSerializer>;
        container_type* tree;

        iterator(container_type* tree, KeyComparator kcmp = KeyComparator{})
            : tree(tree), kcmp(kcmp), next_key(std::nullopt)
        {
            ended = false;
            auto first_node = tree->read_node(
                nullptr, BTree<N, K, V, KeySerializer, KeyComparator, KeyEq,
                               ValueSerializer>::FIRST_NODE_PAGE_ID);

            if (!first_node) {
                ended = true;
                return;
            }

            auto leaf =
                static_cast<LeafNode<N, K, V, KeySerializer, KeyComparator,
                                     KeyEq, ValueSerializer>*>(
                    first_node.get());
            key_buf.clear();
            value_buf.clear();
            std::copy(leaf->keys.begin(), leaf->keys.begin() + leaf->get_size(),
                      std::back_inserter(key_buf));
            std::copy(leaf->values.begin(),
                      leaf->values.begin() + leaf->get_size(),
                      std::back_inserter(value_buf));

            idx = 0;
            if (key_buf.empty()) {
                ended = true;
            } else {
                kvp = std::make_pair(key_buf[idx], value_buf[idx]);
            }
        }

        iterator(container_type* tree, const K& key,
                 KeyComparator kcmp = KeyComparator{})
            : tree(tree), kcmp(kcmp)
        {
            ended = false;
            tree->collect_values(key, &next_key, key_buf, value_buf);
            idx = std::lower_bound(key_buf.begin(), key_buf.end(), key, kcmp) -
                  key_buf.begin();
            if (idx == key_buf.size()) {
                ended = true;
            } else {
                kvp = std::make_pair(key_buf[idx], value_buf[idx]);
            }
        }

        void inc()
        {
            if (ended) return;
            idx++;
            if (idx == key_buf.size()) {
                get_next_batch();
            }
            if (ended) return;
            kvp = std::make_pair(key_buf[idx], value_buf[idx]);
        }

        void prefetch_next_batch() {
            if (!next_key) return;

            K key = *next_key;
            
            // Prefetch nodes that might contain this key and subsequent keys
            tree->prefetch_search_path(key);
            
            // If we know this key is in a leaf node, also try to prefetch subsequent leaf nodes
            // This is a simplified approach - in a real implementation, you'd track leaf node pointers
            if (!key_buf.empty() && !tree->root->is_leaf()) {
                // Estimate a few keys ahead
                K ahead_key = key;
                // This is a simple heuristic - you might need to adjust for your key type
                if constexpr (std::is_integral_v<K>) {
                    ahead_key += 100; // Look ~100 keys ahead
                }
                tree->prefetch_search_path(ahead_key);
            }
        }

        void get_next_batch()
        {
            if (!next_key) {
                ended = true;
                return;
            }

            K key = *next_key;
            next_key = std::nullopt;
            
            // After getting data, prefetch the next batch
            tree->collect_values(key, &next_key, key_buf, value_buf);
            idx = std::lower_bound(key_buf.begin(), key_buf.end(), key, kcmp) -
                key_buf.begin();
            
            if (idx == key_buf.size()) {
                ended = true;
            } else if (next_key) {
                // Prefetch the next batch in advance
                prefetch_next_batch();
            }
        }
    };

private:
    struct Sentinel {
        friend bool operator==(iterator const& it, Sentinel)
        {
            return it.is_end();
        }

        template <class Rhs,
                  std::enable_if_t<!std::is_same<Rhs, Sentinel>{}, int> = 0>
        friend bool operator!=(Rhs const& ptr, Sentinel)
        {
            return !(ptr == Sentinel{});
        }
        friend bool operator==(Sentinel, iterator const& it)
        {
            return it.is_end();
        }
        template <class Lhs,
                  std::enable_if_t<!std::is_same<Lhs, Sentinel>{}, int> = 0>
        friend bool operator!=(Sentinel, Lhs const& ptr)
        {
            return !(Sentinel{} == ptr);
        }
        friend bool operator==(Sentinel, Sentinel) { return true; }
        friend bool operator!=(Sentinel, Sentinel) { return false; }
    };

public:
    iterator begin() { return iterator(this); }
    iterator begin(const K& key) { return iterator(this, key); }
    Sentinel end() const { return Sentinel{}; }

private:
    static const PageID META_PAGE_ID = 1;
    static const PageID FIRST_NODE_PAGE_ID = META_PAGE_ID + 1;
    static const uint32_t META_PAGE_MAGIC = 0x00C0FFEE;
    static const uint32_t INNER_TAG = 1;
    static const uint32_t LEAF_TAG = 2;

    AbstractPageCache* page_cache;
    std::unique_ptr<BaseNode<K, V, KeyComparator, KeyEq>> root;
    std::atomic<size_t> num_pairs;

    /* metadata: | magic(4 bytes) | root page id(4 bytes) | */
    bool read_metadata()
    {
        boost::upgrade_lock<Page> lock;
        auto page = page_cache->fetch_page(META_PAGE_ID, lock);
        if (!page) return false;

        const auto* buf = page->get_buffer(lock);
        buf += sizeof(uint32_t);
        PageID root_pid = (PageID) * reinterpret_cast<const uint32_t*>(buf);
        buf += sizeof(uint32_t);
        size_t pair_count = *reinterpret_cast<const uint32_t*>(buf);
        root = read_node(nullptr, root_pid);
        num_pairs.store(pair_count);

        page_cache->unpin_page(page, false, lock);

        return true;
    }

    void write_metadata()
    {
        boost::upgrade_lock<Page> lock;
        auto page = page_cache->fetch_page(META_PAGE_ID, lock);

        {
            boost::upgrade_to_unique_lock<Page> ulock(lock);
            auto* buf = page->get_buffer(ulock);

            *reinterpret_cast<uint32_t*>(buf) = META_PAGE_MAGIC;
            buf += sizeof(uint32_t);
            *reinterpret_cast<uint32_t*>(buf) = (uint32_t)root->get_pid();
            buf += sizeof(uint32_t);
            *reinterpret_cast<uint32_t*>(buf) = (uint32_t)num_pairs.load();
        }

        page_cache->unpin_page(page, true, lock);
    }
};

} // namespace bptree

#endif
