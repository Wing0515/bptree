#ifndef _BPTREE_MEM_PAGE_CACHE_H_
#define _BPTREE_MEM_PAGE_CACHE_H_

#include "bptree/page_cache.h"

#include <shared_mutex>
#include <unordered_map>

namespace bptree {

class MemPageCache : public AbstractPageCache {
public:
    MemPageCache(size_t page_size) : page_size(page_size) { next_id.store(1); }

    virtual Page* new_page(boost::upgrade_lock<Page>& lock) override
    {
        auto id = get_next_id();
        std::unique_lock<std::shared_mutex> guard(mutex);
        page_map[id] = std::make_unique<Page>(id, page_size);
        Page* page = page_map[id].get();
        lock = boost::upgrade_lock<Page>(*page);
        return page;
    }

    virtual Page* fetch_page(PageID id, boost::upgrade_lock<Page>& lock) override
    {
        std::shared_lock<std::shared_mutex> guard(mutex);
        auto it = page_map.find(id);
        if (it == page_map.end()) return nullptr;
        lock = boost::upgrade_lock<Page>(*it->second);
        return it->second.get();
    }

    virtual void pin_page(Page* page, boost::upgrade_lock<Page>&) override {}
    virtual void unpin_page(Page* page, bool dirty, boost::upgrade_lock<Page>&) override {}

    virtual void flush_page(Page* page, boost::upgrade_lock<Page>&) override {}
    virtual void flush_all_pages() override {}

    virtual size_t size() const override { return page_map.size(); }
    virtual size_t get_page_size() const override { return page_size; }

    virtual void prefetch_page(PageID id) override {
        std::shared_lock<std::shared_mutex> guard(mutex);
        if (page_map.find(id) == page_map.end())
        {
            std::unique_lock<std::shared_mutex> write_guard(mutex);
            if (page_map.find(id) == page_map.end())
            {
                page_map[id] = std::make_unique<Page>(id, page_size);
            }
        }
    }

    virtual void prefetch_pages(const std::vector<PageID> &ids) override {
        for (PageID id : ids)
        {
            prefetch_page(id);
        }
    }

private:
    size_t page_size;
    std::atomic<PageID> next_id;
    std::shared_mutex mutex;
    std::unordered_map<PageID, std::unique_ptr<Page>> page_map;

    PageID get_next_id() { return next_id++; }
};

} // namespace bptree

#endif
