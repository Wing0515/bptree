#include "bptree/heap_page_cache.h"

#include <algorithm>
#include <cassert>

namespace bptree {

HeapPageCache::HeapPageCache(std::string_view filename, bool create,
                             size_t max_pages, size_t page_size)
    : heap_file(std::make_unique<HeapFile>(filename, create, page_size)),
      max_pages(max_pages)
{
    this->page_size = page_size;
}

Page* HeapPageCache::alloc_page(PageID id)
{
    if (size() < max_pages) {
        auto page = new Page(id, page_size);
        page->lock();
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
    page->lock();

    page_map.erase(it);

    if (page->is_dirty()) {
        flush_page(page);
    }
    page->set_id(id);
    page_map[id] = page;

    return page;
}

Page* HeapPageCache::new_page()
{
    std::lock_guard<std::mutex> lock(mutex);

    PageID new_id = heap_file->new_page();
    auto page = alloc_page(new_id);

    pin_page(page);

    return page;
}

Page* HeapPageCache::fetch_page(PageID id)
{
    bptree::Page* page = nullptr;
    {
        std::lock_guard<std::mutex> lock(mutex);

        auto it = page_map.find(id);

        if (it == page_map.end()) {
            auto page = alloc_page(id);

            try {
                heap_file->read_page(page->get_id(), page->get_buffer_locked());
                pin_page(page);

                return page;
            } catch (const IOException&) {
                return nullptr;
            }
        }

        page = it->second;
        pin_page(page);
    }

    page->lock();
    return page;
}

void HeapPageCache::pin_page(Page* page)
{
    if (page->pin() == 0) {
        lru_erase(page->get_id());
    }
}

void HeapPageCache::unpin_page(Page* page, bool dirty)
{
    page->set_dirty(dirty);

    int pin_count = page->unpin();
    if (pin_count == 1) {
        lru_insert(page->get_id());
    }

    flush_page(page);
}

void HeapPageCache::flush_page(Page* page)
{
    if (page->is_dirty()) {
        heap_file->write_page(page->get_id(), page->get_buffer_locked());

        page->set_dirty(false);
    }
}

void HeapPageCache::flush_all_pages()
{
    for (auto&& p : pages) {
        // flush_page(p.get());
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

} // namespace bptree
