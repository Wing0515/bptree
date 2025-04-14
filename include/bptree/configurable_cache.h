#ifndef _BPTREE_CONFIGURABLE_CACHE_H_
#define _BPTREE_CONFIGURABLE_CACHE_H_

#include "bptree/page_cache.h"
#include "bptree/latency_simulator.h"

#include <atomic>
#include <chrono>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>
#include <string>
#include <algorithm>
#include <numeric>

namespace bptree {

/**
 * Statistics for a cache section
 */
struct CacheSectionStats {
    size_t accesses{0};
    size_t hits{0};
    size_t misses{0};
    double avg_hit_time_ms{0};
    double avg_miss_time_ms{0};
    
    double miss_rate() const {
        return accesses > 0 ? static_cast<double>(misses) / accesses : 0.0;
    }
};

/**
 * Represents a single section of the configurable cache with its own
 * structure, line size, and replacement policy
 */
class CacheSection {
public:
    enum class Structure {
        DirectMapped,
        SetAssociative,
        FullyAssociative
    };

    CacheSection(size_t section_id, size_t size, size_t line_size, Structure structure, size_t associativity)
        : section_id(section_id),
        size(size),
        line_size(line_size),
        structure(structure),
        associativity(associativity),
        num_sets(size / (line_size * associativity)),
        pages_capacity(size / line_size)
    {
        if (num_sets == 0) num_sets = 1;

        // Initialize the cache structure based on type
        if (structure == Structure::DirectMapped || structure == Structure::SetAssociative) {
            sets.resize(num_sets);
            for (auto& set : sets) {
            set.resize(associativity);
            for (auto& entry : set) {
                    entry.valid = false;
                    entry.referenced = false;
                    entry.id = Page::INVALID_PAGE_ID;
                }
            }
        }
    }

    ~CacheSection() = default;

    // Retrieve page from this cache section
    Page* fetch_page(PageID id, boost::upgrade_lock<Page>& lock);
    
    // Add a newly created page to this section
    Page* new_page(PageID id, boost::upgrade_lock<Page>& lock);
    
    // Pin a page in this section
    void pin_page(Page* page, boost::upgrade_lock<Page>& lock);
    
    // Unpin a page in this section
    void unpin_page(Page* page, bool dirty, boost::upgrade_lock<Page>& lock);
    
    // Write a dirty page back to underlying storage
    void flush_page(Page* page, boost::upgrade_lock<Page>& lock);
    
    // Flush all pages in this section
    void flush_all_pages();
    
    // Prefetch a page into this section
    void prefetch_page(PageID id);
    
    // Prefetch multiple pages into this section
    void prefetch_pages(const std::vector<PageID>& ids);
    
    // Get statistics for this section
    CacheSectionStats get_stats() const;
    
    // Reset statistics
    void reset_stats();
    
    // Configuration getters
    size_t get_section_id() const { return section_id; }
    size_t get_size() const { return size; }
    size_t get_line_size() const { return line_size; }
    Structure get_structure() const { return structure; }
    size_t get_associativity() const { return associativity; }
    
    // Resize this section
    void resize(size_t new_size);
    
    // Get number of pages currently in this section
    size_t page_count() const;

private:
    struct CacheEntry {
        std::unique_ptr<Page> page;
        PageID id;
        bool valid;
        bool referenced;  // For clock replacement algorithm
        
        CacheEntry() : id(Page::INVALID_PAGE_ID), valid(false), referenced(false) {}
    };

    size_t section_id;
    size_t size;                 // Size in bytes
    size_t line_size;            // Line size in bytes
    Structure structure;         // Cache structure type
    size_t associativity;        // Number of ways for set-associative cache
    size_t num_sets;             // Number of sets in the cache
    size_t pages_capacity;       // Maximum number of pages this section can hold
    
    mutable std::shared_mutex mutex;
    
    // Cache storage based on structure
    std::vector<std::vector<CacheEntry>> sets;  // For direct-mapped and set-associative
    std::list<CacheEntry> pages;                // For fully-associative
    std::unordered_map<PageID, decltype(pages)::iterator> page_map;  // For fully-associative lookup
    
    // Statistics
    CacheSectionStats stats;
    
    // Internal helper methods
    size_t get_set_index(PageID id) const;
    size_t find_victim_in_set(size_t set_index);
    decltype(pages)::iterator find_victim_in_fully_associative();
    
    // Helper to safely create a page
    std::unique_ptr<Page> create_page(PageID id) {
        return std::make_unique<Page>(id, line_size);
    }
    
    // Create a new cache page
    Page* allocate_page(PageID id, boost::upgrade_lock<Page>& lock);
    
    // Timer for measuring operation times
    using high_resolution_clock = std::chrono::high_resolution_clock;
    using time_point = std::chrono::time_point<high_resolution_clock>;
    
    time_point start_timer() const {
        return high_resolution_clock::now();
    }
    
    double end_timer(time_point start) const {
        auto end = high_resolution_clock::now();
        return std::chrono::duration<double, std::milli>(end - start).count();
    }
};

/**
 * A page cache that supports multiple configurable sections with
 * different structures, line sizes, and replacement policies.
 */
class ConfigurableCache : public AbstractPageCache {
public:
    ConfigurableCache(size_t total_size, size_t page_size, size_t default_line_size = 4096);
    virtual ~ConfigurableCache() = default;
    
    // AbstractPageCache interface implementation
    virtual Page* new_page(boost::upgrade_lock<Page>& lock) override;
    virtual Page* fetch_page(PageID id, boost::upgrade_lock<Page>& lock) override;
    virtual void pin_page(Page* page, boost::upgrade_lock<Page>& lock) override;
    virtual void unpin_page(Page* page, bool dirty, boost::upgrade_lock<Page>& lock) override;
    virtual void flush_page(Page* page, boost::upgrade_lock<Page>& lock) override;
    virtual void flush_all_pages() override;
    virtual size_t size() const override;
    virtual size_t get_page_size() const override;
    virtual void prefetch_page(PageID id) override;
    virtual void prefetch_pages(const std::vector<PageID>& ids) override;
    
    // Section management
    size_t create_section(size_t size, size_t line_size, 
                        CacheSection::Structure structure, 
                        size_t associativity = 8);
    void remove_section(size_t section_id);
    void resize_section(size_t section_id, size_t new_size);
    
    // Page to section mapping
    void map_page_to_section(PageID id, size_t section_id);
    void map_page_range_to_section(PageID start, PageID end, size_t section_id);
    size_t get_section_for_page(PageID id) const;
    
    // Statistics
    std::vector<CacheSectionStats> get_all_section_stats() const;
    void reset_all_stats();
    
    // Size optimization
    void optimize_section_sizes();

    private:
    size_t total_size;
    size_t available_size;
    size_t page_size;
    size_t next_section_id;
    size_t next_page_id;  // Page ID counter
    
    mutable std::shared_mutex mutex;
    
    // Cache sections
    std::unordered_map<size_t, std::unique_ptr<CacheSection>> sections;
    
    // Default section for pages not explicitly mapped
    size_t default_section_id;
    
    // Page to section mapping
    std::unordered_map<PageID, size_t> page_to_section_map;
    std::vector<std::pair<PageID, PageID>> page_ranges;
    std::vector<size_t> range_section_ids;
    
    // Find the appropriate section for a page
    CacheSection* find_section_for_page(PageID id) const;
    
    // Perform simple cache size optimization based on miss rates
    void simple_size_optimization();
};

} // namespace bptree

#endif // _BPTREE_CONFIGURABLE_CACHE_H_