#ifndef _BPTREE_PAGE_H_
#define _BPTREE_PAGE_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>

namespace bptree {

typedef uint32_t PageID;

class Page {
public:
    static const PageID INVALID_PAGE_ID = 0;

    explicit Page(PageID id, size_t size) : id(id), size(size), dirty(false), pin_count(0)
    {
        buffer = std::make_unique<uint8_t[]>(size);
    }

    uint8_t* lock()
    {
        mutex.lock();
        return buffer.get();
    }
    void unlock() { mutex.unlock(); }

    uint8_t* get_buffer_locked() {
        return buffer.get();
    }

    int32_t pin() { return pin_count.fetch_add(1); }
    int32_t unpin() { return pin_count.fetch_add(-1); }

    void set_id(PageID pid) { id = pid; }
    PageID get_id() const { return id; }
    size_t get_size() const { return size; }

    bool is_dirty() const { return dirty; }
    void set_dirty(bool d) { dirty = d; }

private:
    PageID id;
    std::unique_ptr<uint8_t[]> buffer;
    size_t size;
    bool dirty;
    std::atomic<int32_t> pin_count;
    std::mutex mutex;
};

} // namespace bptree

#endif
