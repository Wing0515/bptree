#include "bptree/heap_page_cache.h"

#include <algorithm>
#include <cassert>
#include <iostream>

namespace bptree {

    HeapPageCache::HeapPageCache(std::string_view filename, bool create,
                                size_t max_pages, size_t page_size)
        : heap_file(std::make_unique<HeapFile>(filename, create, page_size)),
        max_pages(max_pages)
    {
        this->page_size = page_size;
    }

    Page* HeapPageCache::alloc_page(PageID id, boost::upgrade_lock<Page>& lock)
    {
        if (size() < max_pages) {
            auto page = new Page(id, page_size);
            lock = boost::upgrade_lock(*page);
            pages.emplace_back(page);
            page_map[id] = page;

            return page;
        }

        PageID victim_id;
        if (!lru_victim(victim_id)) {
            return nullptr;
        }

        auto it = page_map.find(victim_id);
        assert(it != page_map.end());

        auto* page = it->second;
        lock = boost::upgrade_lock(*page);

        if (page->is_dirty()) {
            flush_page(page, lock);
        }

        boost::upgrade_to_unique_lock<Page> ulock(lock);
        page_map.erase(it);
        page->set_id(id);
        page_map[id] = page;

        return page;
    }

    Page* HeapPageCache::new_page(boost::upgrade_lock<Page>& lock)
    {
        std::lock_guard<std::mutex> guard(mutex);

        PageID new_id = heap_file->new_page();
        auto page = alloc_page(new_id, lock);

        pin_page(page, lock);

        return page;
    }

    Page* HeapPageCache::fetch_page(PageID id, boost::upgrade_lock<Page>& lock)
    {
        bptree::Page* page = nullptr;
        {
            std::lock_guard<std::mutex> guard(mutex);

            auto it = page_map.find(id);

            if (it == page_map.end()) {
                auto page = alloc_page(id, lock);

                try {
                    boost::upgrade_to_unique_lock<Page> ulock(lock);
                    heap_file->read_page(page, ulock);
                    pin_page(page, lock);

                    return page;
                } catch (IOException e) {
                    // std::cerr << "Failed to read page: " << e.what() << std::endl;
                    return nullptr;
                }
            }

            page = it->second;
            pin_page(page, lock);
        }

        lock = boost::upgrade_lock<Page>(*page);
        return page;
    }

    void HeapPageCache::pin_page(Page* page, boost::upgrade_lock<Page>& lock)
    {
        if (page->pin() == 0) {
            lru_erase(page->get_id());
        }
    }

    void HeapPageCache::unpin_page(Page* page, bool dirty, boost::upgrade_lock<Page>& lock)
    {
        page->set_dirty(dirty);

        int pin_count = page->unpin();
        if (pin_count == 1) {
            lru_insert(page->get_id());
        }

        flush_page(page, lock);
    }

    void HeapPageCache::flush_page(Page* page, boost::upgrade_lock<Page>& lock)
    {
        if (page->is_dirty()) {
            heap_file->write_page(page, lock);

            page->set_dirty(false);
        }
    }

    void HeapPageCache::flush_all_pages()
    {
        for (auto&& p : pages) {
            auto lock = boost::upgrade_lock<Page>(*p);
            flush_page(p.get(), lock);
        }
    }

    void HeapPageCache::lru_insert(PageID id)
    {
        std::lock_guard<std::mutex> lock(lru_mutex);

        if (lru_map.find(id) == lru_map.end()) {
            lru_list.push_front(id);
            lru_map.emplace(id, lru_list.begin());
        }
    }

    void HeapPageCache::lru_erase(PageID id)
    {
        std::lock_guard<std::mutex> lock(lru_mutex);

        auto it = lru_map.find(id);

        if (it != lru_map.end()) {
            lru_list.erase(it->second);
            lru_map.erase(it);
        }
    }

    bool HeapPageCache::lru_victim(PageID& id)
    {
        std::lock_guard<std::mutex> lock(lru_mutex);

        if (lru_list.empty()) {
            return false;
        }

        id = lru_list.back();
        lru_map.erase(id);
        lru_list.pop_back();

        return true;
    }

    void HeapPageCache::prefetch_page(PageID id) {
        {
            std::lock_guard<std::mutex> guard(mutex);
            if (page_map.find(id) != page_map.end()) {
                return; // Already in cache
            }
        }
        
        boost::upgrade_lock<Page> lock;
        Page* page = nullptr;
        
        {
            std::lock_guard<std::mutex> guard(mutex);
            page = alloc_page(id, lock);
            if (!page) return; // Couldn't allocate a page
        }
        
        try {
            boost::upgrade_to_unique_lock<Page> ulock(lock);
            heap_file->read_page(page, ulock);
        } catch (IOException& e) {
            std::cerr << "Failed to prefetch page: " << e.what() << std::endl;
        }
    }

    void HeapPageCache::prefetch_pages(const std::vector<PageID>& ids) {
        for (PageID id : ids) {
            prefetch_page(id);
        }
    }

} // namespace bptree
