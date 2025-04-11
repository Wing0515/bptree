#ifndef _BPTREE_FULLY_ASSOCIATIVE_CACHE_H_
#define _BPTREE_FULLY_ASSOCIATIVE_CACHE_H_

#include "bptree/page_cache.h"
#include <list>
#include <unordered_map>
#include <mutex>

namespace bptree {

class FullyAssociativeCache : public AbstractPageCache {
public:
    FullyAssociativeCache(size_t max_pages, size_t page_size, size_t line_size_bytes)
        : max_pages(max_pages), page_size(page_size), line_size_bytes(line_size_bytes)
    {
        next_id.store(1);
    }

    virtual Page* new_page(boost::upgrade_lock<Page>& lock) override {
        std::unique_lock<std::mutex> guard(mutex);
        auto id = next_id.fetch_add(1);
        auto* page = new Page(id, page_size);
        lock = boost::upgrade_lock<Page>(*page);
        
        // Add to the cache
        cache_entries[id] = page;
        lru_list.push_front(id);
        lru_map[id] = lru_list.begin();
        
        // If we've exceeded max pages, evict the LRU page
        while (cache_entries.size() > max_pages) {
            PageID lru_id = lru_list.back();
            lru_list.pop_back();
            lru_map.erase(lru_id);
            
            // Free the evicted page
            delete cache_entries[lru_id];
            cache_entries.erase(lru_id);
        }
        
        return page;
    }

    virtual Page* fetch_page(PageID id, boost::upgrade_lock<Page>& lock) override {
        if (id == Page::INVALID_PAGE_ID) {
            std::cerr << "ERROR: Attempting to fetch invalid page ID 0" << std::endl;
            return nullptr;
        }
        
        std::unique_lock<std::mutex> guard(mutex);
        
        // Check if page is in cache
        auto it = cache_entries.find(id);
        if (it != cache_entries.end()) {
            // Cache hit - update LRU position
            auto lru_it = lru_map[id];
            lru_list.erase(lru_it);
            lru_list.push_front(id);
            lru_map[id] = lru_list.begin();
            
            lock = boost::upgrade_lock<Page>(*it->second);
            return it->second;
        }
        
        // Cache miss - load the page (simulated for now)
        auto* page = new Page(id, page_size);
        lock = boost::upgrade_lock<Page>(*page);
        
        // Add to cache
        cache_entries[id] = page;
        lru_list.push_front(id);
        lru_map[id] = lru_list.begin();
        
        // If we've exceeded max pages, evict the LRU page
        while (cache_entries.size() > max_pages) {
            PageID lru_id = lru_list.back();
            lru_list.pop_back();
            lru_map.erase(lru_id);
            
            // Free the evicted page
            delete cache_entries[lru_id];
            cache_entries.erase(lru_id);
        }
        
        return page;
    }

    virtual void pin_page(Page* page, boost::upgrade_lock<Page>&) override {}
    virtual void unpin_page(Page* page, bool dirty, boost::upgrade_lock<Page>&) override {}
    virtual void flush_page(Page* page, boost::upgrade_lock<Page>&) override {}
    virtual void flush_all_pages() override {}

    virtual size_t size() const override { return cache_entries.size(); }
    virtual size_t get_page_size() const override { return page_size; }

    virtual void prefetch_page(PageID id) override {
        boost::upgrade_lock<Page> lock;
        fetch_page(id, lock);
    }

    virtual void prefetch_pages(const std::vector<PageID>& ids) override {
        for (auto id : ids) {
            prefetch_page(id);
        }
    }

private:
    std::unordered_map<PageID, Page*> cache_entries;
    std::list<PageID> lru_list;
    std::unordered_map<PageID, std::list<PageID>::iterator> lru_map;
    std::mutex mutex;
    std::atomic<PageID> next_id;
    size_t max_pages;
    size_t page_size;
    size_t line_size_bytes;
};

} // namespace bptree

#endif // _BPTREE_FULLY_ASSOCIATIVE_CACHE_H_