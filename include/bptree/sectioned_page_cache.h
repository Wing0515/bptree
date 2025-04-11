#ifndef _BPTREE_SECTIONED_PAGE_CACHE_H_
#define _BPTREE_SECTIONED_PAGE_CACHE_H_

#include "bptree/page_cache.h"
#include "bptree/cache_profiler.h"
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace bptree {

enum class CacheStructure {
    DIRECT_MAPPED,
    SET_ASSOCIATIVE,
    FULLY_ASSOCIATIVE
};

struct CacheSectionConfig {
    std::string name;
    size_t size_pages;
    size_t line_size_bytes;
    CacheStructure structure;
    size_t associativity; // For set-associative cache
    
    CacheSectionConfig(const std::string& name, size_t size_pages, size_t line_size_bytes, 
                       CacheStructure structure, size_t associativity = 1)
        : name(name), size_pages(size_pages), line_size_bytes(line_size_bytes),
          structure(structure), associativity(associativity) {}
};

class SectionedPageCache : public AbstractPageCache {
public:
    SectionedPageCache(size_t total_cache_size_pages, size_t page_size);
    
    // Add a new cache section with the specified configuration
    void add_section(const CacheSectionConfig& config);
    
    // Assign a page ID range to a specific cache section
    void assign_page_range(PageID start_id, PageID end_id, const std::string& section_name);
    
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
    
    // Get statistics for all sections
    void print_stats() const;
    
    // Reset all statistics
    void reset_stats();
    
private:
    struct CacheSection {
        CacheSectionConfig config;
        std::unique_ptr<AbstractPageCache> impl;
        std::unordered_map<PageID, Page*> page_map;
        
        CacheSection(const CacheSectionConfig& config, std::unique_ptr<AbstractPageCache> impl)
            : config(config), impl(std::move(impl)) {}
    };
    
    size_t total_cache_size_pages;
    size_t page_size;
    
    std::vector<std::unique_ptr<CacheSection>> sections;
    std::unordered_map<std::string, CacheSection*> section_by_name;
    std::unordered_map<PageID, CacheSection*> page_to_section;
    
    // Default section for pages not explicitly assigned
    CacheSection* default_section;
    
    // Find the appropriate section for a given page ID
    CacheSection* find_section_for_page(PageID id);
    
    // Create appropriate cache implementation based on configuration
    std::unique_ptr<AbstractPageCache> create_cache_impl(const CacheSectionConfig& config);
};

} // namespace bptree

#endif // _BPTREE_SECTIONED_PAGE_CACHE_H_