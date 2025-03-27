#ifndef _BPTREE_HEAP_PAGE_CACHE_H_
#define _BPTREE_HEAP_PAGE_CACHE_H_

#include "bptree/heap_file.h"
#include "bptree/page_cache.h"

#include <atomic>
#include <cstdint>
#include <deque>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace bptree {

class HeapPageCache : public AbstractPageCache {
public:
    HeapPageCache(std::string_view filename, bool create,
                  size_t max_pages = 4096, size_t page_size = 4096);

    virtual Page *new_page(boost::upgrade_lock<Page> &lock) override;
    virtual Page *fetch_page(PageID id, boost::upgrade_lock<Page> &lock) override;

    virtual void pin_page(Page *page, boost::upgrade_lock<Page> &lock) override;
    virtual void unpin_page(Page *page, bool dirty, boost::upgrade_lock<Page> &lock) override;

    virtual void flush_page(Page *page, boost::upgrade_lock<Page> &lock) override;
    virtual void flush_all_pages() override;

    virtual size_t size() const override { return pages.size(); }
    virtual size_t get_page_size() const override { return page_size; }

    virtual void prefetch_page(PageID id) override;
    virtual void prefetch_pages(const std::vector<PageID>& ids) override;

private:
    std::unique_ptr<HeapFile> heap_file;
    size_t page_size;
    size_t max_pages;
    std::mutex mutex;
    std::mutex lru_mutex;

    std::list<std::unique_ptr<Page>> pages;
    std::unordered_map<PageID, Page*> page_map;
    std::list<PageID> lru_list;
    std::unordered_map<PageID, std::list<PageID>::iterator> lru_map;

    Page* alloc_page(PageID new_id, boost::upgrade_lock<Page>& lock);

    void lru_insert(PageID id);
    void lru_erase(PageID id);
    bool lru_victim(PageID& id);
};

} // namespace bptree

#endif
