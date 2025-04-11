#ifndef _BPTREE_DIRECT_MAPPED_CACHE_H_
#define _BPTREE_DIRECT_MAPPED_CACHE_H_

#include "bptree/page_cache.h"
#include <unordered_map>
#include <vector>
#include <mutex>

namespace bptree {

class DirectMappedCache : public AbstractPageCache {
public:
    DirectMappedCache(size_t num_lines, size_t page_size, size_t line_size_bytes)
        : page_size(page_size), line_size_bytes(line_size_bytes) 
    {
        // Initialize the cache lines
        cache_lines.resize(num_lines);
        for (auto& line : cache_lines) {
            line.page_id = Page::INVALID_PAGE_ID;
            line.page = nullptr;
            line.valid = false;
        }
        next_id.store(1);
    }

    virtual Page* new_page(boost::upgrade_lock<Page>& lock) override {
        std::unique_lock<std::mutex> guard(mutex);
        auto id = next_id.fetch_add(1);
        auto* page = new Page(id, page_size);
        lock = boost::upgrade_lock<Page>(*page);
        
        // Store in the appropriate cache line
        size_t index = id % cache_lines.size();
        if (cache_lines[index].valid && cache_lines[index].page) {
            // Evict the current page at this index
            delete cache_lines[index].page;
        }
        
        cache_lines[index].page_id = id;
        cache_lines[index].page = page;
        cache_lines[index].valid = true;
        
        return page;
    }

    virtual Page* fetch_page(PageID id, boost::upgrade_lock<Page>& lock) override {
        if (id == Page::INVALID_PAGE_ID) {
            std::cerr << "ERROR: Attempting to fetch invalid page ID 0" << std::endl;
            return nullptr;
        }
        
        std::unique_lock<std::mutex> guard(mutex);
        
        // Direct mapped - check only the specific index
        size_t index = id % cache_lines.size();
        
        if (cache_lines[index].valid && cache_lines[index].page_id == id) {
            // Cache hit
            lock = boost::upgrade_lock<Page>(*cache_lines[index].page);
            return cache_lines[index].page;
        }
        
        // Cache miss - for demonstration purposes, we'll create a new page
        // In a real implementation, this would load from far memory
        auto* page = new Page(id, page_size);
        lock = boost::upgrade_lock<Page>(*page);
        
        // Evict current occupant if necessary
        if (cache_lines[index].valid && cache_lines[index].page) {
            delete cache_lines[index].page;
        }
        
        // Store in cache
        cache_lines[index].page_id = id;
        cache_lines[index].page = page;
        cache_lines[index].valid = true;
        
        return page;
    }

    virtual void pin_page(Page* page, boost::upgrade_lock<Page>&) override {}
    virtual void unpin_page(Page* page, bool dirty, boost::upgrade_lock<Page>&) override {}
    virtual void flush_page(Page* page, boost::upgrade_lock<Page>&) override {}
    virtual void flush_all_pages() override {}

    virtual size_t size() const override { 
        size_t count = 0;
        for (const auto& line : cache_lines) {
            if (line.valid) count++;
        }
        return count;
    }
    
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
    struct CacheLine {
        PageID page_id;
        Page* page;
        bool valid;
    };

    std::vector<CacheLine> cache_lines;
    std::mutex mutex;
    std::atomic<PageID> next_id;
    size_t page_size;
    size_t line_size_bytes;
};

} // namespace bptree

#endif // _BPTREE_DIRECT_MAPPED_CACHE_H_