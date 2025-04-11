#include "bptree/sectioned_page_cache.h"
#include "bptree/mem_page_cache.h"
#include "bptree/direct_mapped_cache.h"
#include "bptree/fully_associative_cache.h"
#include <algorithm>
#include <cassert>
#include <iostream>

#define DEBUG_LOG(msg) std::cout << "[DEBUG] " << __FUNCTION__ << ": " << msg << std::endl

namespace bptree {

SectionedPageCache::SectionedPageCache(size_t total_cache_size_pages, size_t page_size)
    : total_cache_size_pages(total_cache_size_pages), page_size(page_size), default_section(nullptr)
{
    std::cout << "[DEBUG] Creating SectionedPageCache with " << total_cache_size_pages << " pages of size " << page_size << " bytes" << std::endl;
              
    // Create a default section with fully associative structure
    CacheSectionConfig default_config("default", total_cache_size_pages, page_size, CacheStructure::FULLY_ASSOCIATIVE);
    add_section(default_config);
    
    auto it = section_by_name.find("default");
    if (it != section_by_name.end()) {
        default_section = it->second;
        std::cout << "[DEBUG] Default section created successfully" << std::endl;
    } else {
        std::cerr << "[ERROR] Failed to create default section!" << std::endl;
    }
}

void SectionedPageCache::add_section(const CacheSectionConfig& config)
{
    DEBUG_LOG("Adding section: " << config.name);
    
    // Ensure section name doesn't exist
    if (section_by_name.find(config.name) != section_by_name.end()) {
        std::cerr << "Section already exists: " << config.name << std::endl;
        return;
    }
    
    // Create appropriate cache implementation
    auto cache_impl = create_cache_impl(config);
    if (!cache_impl) {
        DEBUG_LOG("  Failed to create cache implementation!");
        return;
    }
    
    // Create the section
    auto section = std::make_unique<CacheSection>(config, std::move(cache_impl));
    auto section_ptr = section.get();
    
    DEBUG_LOG("  Section created successfully");
    
    // Add to maps
    sections.push_back(std::move(section));
    section_by_name[config.name] = section_ptr;
    
    // If this is the first section, make it the default
    if (sections.size() == 1) {
        DEBUG_LOG("  Setting as default section");
        default_section = section_ptr;
    }
}

void SectionedPageCache::assign_page_range(PageID start_id, PageID end_id, const std::string& section_name)
{
    DEBUG_LOG("Assigning page range [" << start_id << "-" << end_id << "] to section " << section_name);
    
    auto it = section_by_name.find(section_name);
    if (it == section_by_name.end()) {
        DEBUG_LOG("  Section not found: " << section_name);
        return;
    }
    
    auto section = it->second;
    
    // Simply assign the range to the section - we'll handle creation/transfer in new_page
    for (PageID id = start_id; id <= end_id; ++id) {
        DEBUG_LOG("  Assigning page " << id << " to section " << section_name);
        page_to_section[id] = section;
    }
}

Page* SectionedPageCache::new_page(boost::upgrade_lock<Page>& lock) 
{
    DEBUG_LOG("Creating new page");
    
    if (!default_section) {
        DEBUG_LOG("  ERROR: No default section available!");
        return nullptr;
    }
    
    if (!default_section->impl) {
        DEBUG_LOG("  ERROR: Default section has no implementation!");
        return nullptr;
    }
    
    // Use default section for new pages
    Page* page = default_section->impl->new_page(lock);
    if (page) {
        PageID id = page->get_id();
        DEBUG_LOG("  Created page with ID: " << id);
        
        // Check if this page has been pre-assigned to a section
        auto section = find_section_for_page(id);
        if (section != default_section) {
            DEBUG_LOG("  This page ID is assigned to section: " << section->config.name);
            DEBUG_LOG("  Transferring page to assigned section");
            
            // Transfer the page to the assigned section
            default_section->page_map.erase(id);
            
            // For now, create a new page in the target section
            // In a real implementation, we would transfer the page data
            boost::upgrade_lock<Page> new_lock;
            Page* new_page = section->impl->new_page(new_lock);
            
            if (new_page) {
                DEBUG_LOG("  Created new page in target section with ID: " << new_page->get_id());
                
                // Since IDs might not match, we need to update our page_to_section map
                page_to_section[new_page->get_id()] = section;
                section->page_map[new_page->get_id()] = new_page;
                
                // Update the lock and return the new page
                lock = std::move(new_lock);
                return new_page;
            } else {
                DEBUG_LOG("  Failed to create page in target section!");
            }
        }
        
        // If no transfer needed or transfer failed, use the default section page
        default_section->page_map[id] = page;
    } else {
        DEBUG_LOG("  Failed to create page!");
    }
    
    return page;
}

Page* SectionedPageCache::fetch_page(PageID id, boost::upgrade_lock<Page>& lock)
{
    DEBUG_LOG("Fetching page " << id);
    
    auto section = find_section_for_page(id);
    if (!section) {
        DEBUG_LOG("  ERROR: No section found for page " << id);
        return nullptr;
    }
    
    if (!section->impl) {
        DEBUG_LOG("  ERROR: Section " << section->config.name << " has no implementation");
        return nullptr;
    }
    
    DEBUG_LOG("  Using section: " << section->config.name);
    
    // Record the access for profiling
    auto start = std::chrono::high_resolution_clock::now();
    
    // Check if page already exists in this section's map
    auto it = section->page_map.find(id);
    Page* page = nullptr;
    
    if (it != section->page_map.end() && it->second) {
        DEBUG_LOG("  Page found in section's page map");
        page = it->second;
        try {
            lock = boost::upgrade_lock<Page>(*page);
        } catch (const std::exception& e) {
            DEBUG_LOG("  ERROR: Exception while acquiring lock: " << e.what());
            return nullptr;
        }
    } else {
        DEBUG_LOG("  Fetching page from section implementation");
        try {
            page = section->impl->fetch_page(id, lock);
        } catch (const std::exception& e) {
            DEBUG_LOG("  ERROR: Exception in fetch_page: " << e.what());
            return nullptr;
        }
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    
    bool is_hit = (page != nullptr);
    try {
        CacheProfiler::instance().record_access(section->config.name, id, is_hit, false);
    } catch (const std::exception& e) {
        DEBUG_LOG("  ERROR: Exception in record_access: " << e.what());
    }
    
    if (page) {
        DEBUG_LOG("  Page fetched successfully, ID: " << page->get_id());
        section->page_map[id] = page;
    } else {
        DEBUG_LOG("  Failed to fetch page");
    }
    
    return page;
}

void SectionedPageCache::pin_page(Page* page, boost::upgrade_lock<Page>& lock)
{
    if (!page) {
        DEBUG_LOG("ERROR: Trying to pin a null page");
        return;
    }
    
    PageID id = page->get_id();
    DEBUG_LOG("Pinning page " << id);
    
    auto section = find_section_for_page(id);
    if (!section) {
        DEBUG_LOG("  ERROR: No section found for page " << id);
        return;
    }
    
    if (!section->impl) {
        DEBUG_LOG("  ERROR: Section has no implementation");
        return;
    }
    
    try {
        section->impl->pin_page(page, lock);
        DEBUG_LOG("  Page pinned successfully");
    } catch (const std::exception& e) {
        DEBUG_LOG("  ERROR: Exception in pin_page: " << e.what());
    }
}

void SectionedPageCache::unpin_page(Page* page, bool dirty, boost::upgrade_lock<Page>& lock)
{
    if (!page) {
        DEBUG_LOG("ERROR: Trying to unpin a null page");
        return;
    }
    
    PageID id = page->get_id();
    DEBUG_LOG("Unpinning page " << id << " (dirty: " << dirty << ")");
    
    auto section = find_section_for_page(id);
    if (!section) {
        DEBUG_LOG("  ERROR: No section found for page " << id);
        return;
    }
    
    if (!section->impl) {
        DEBUG_LOG("  ERROR: Section has no implementation");
        return;
    }
    
    // Record the access for profiling (write operation)
    if (dirty) {
        try {
            CacheProfiler::instance().record_access(section->config.name, id, true, true);
        } catch (const std::exception& e) {
            DEBUG_LOG("  ERROR: Exception in record_access: " << e.what());
        }
    }
    
    try {
        section->impl->unpin_page(page, dirty, lock);
        DEBUG_LOG("  Page unpinned successfully");
    } catch (const std::exception& e) {
        DEBUG_LOG("  ERROR: Exception in unpin_page: " << e.what());
    }
}

void SectionedPageCache::flush_page(Page* page, boost::upgrade_lock<Page>& lock)
{
    PageID id = page->get_id();
    auto section = find_section_for_page(id);
    section->impl->flush_page(page, lock);
}

void SectionedPageCache::flush_all_pages()
{
    for (auto& section : sections) {
        section->impl->flush_all_pages();
    }
}

size_t SectionedPageCache::size() const
{
    size_t total = 0;
    for (const auto& section : sections) {
        total += section->impl->size();
    }
    return total;
}

size_t SectionedPageCache::get_page_size() const
{
    return page_size;
}

void SectionedPageCache::prefetch_page(PageID id)
{
    auto section = find_section_for_page(id);
    section->impl->prefetch_page(id);
}

void SectionedPageCache::prefetch_pages(const std::vector<PageID>& ids)
{
    // Group pages by section for more efficient prefetching
    std::unordered_map<CacheSection*, std::vector<PageID>> section_pages;
    
    for (PageID id : ids) {
        auto section = find_section_for_page(id);
        section_pages[section].push_back(id);
    }
    
    // Prefetch pages for each section
    for (auto& [section, page_ids] : section_pages) {
        section->impl->prefetch_pages(page_ids);
    }
}

void SectionedPageCache::print_stats() const
{
    CacheProfiler::instance().print_stats();
}

void SectionedPageCache::reset_stats()
{
    CacheProfiler::instance().reset();
}

SectionedPageCache::CacheSection* SectionedPageCache::find_section_for_page(PageID id)
{
    DEBUG_LOG("Finding section for page " << id);
    
    auto it = page_to_section.find(id);
    if (it != page_to_section.end()) {
        DEBUG_LOG("  Found section: " << it->second->config.name);
        return it->second;
    }
    
    DEBUG_LOG("  Using default section: " << (default_section ? default_section->config.name : "NULL"));
    return default_section;
}

std::unique_ptr<AbstractPageCache> SectionedPageCache::create_cache_impl(const CacheSectionConfig& config)
{
    DEBUG_LOG("Creating cache implementation for section " << config.name);
    DEBUG_LOG("  Size: " << config.size_pages << " pages");
    DEBUG_LOG("  Line size: " << config.line_size_bytes << " bytes");
    DEBUG_LOG("  Structure: " << static_cast<int>(config.structure));
    
    switch (config.structure) {
        case CacheStructure::DIRECT_MAPPED:
            return std::make_unique<DirectMappedCache>(
                config.size_pages, page_size, config.line_size_bytes);
            
        case CacheStructure::SET_ASSOCIATIVE:
            // For now, we'll use fully associative as a placeholder
            return std::make_unique<FullyAssociativeCache>(
                config.size_pages, page_size, config.line_size_bytes);
            
        case CacheStructure::FULLY_ASSOCIATIVE:
            return std::make_unique<FullyAssociativeCache>(
                config.size_pages, page_size, config.line_size_bytes);
            
        default:
            DEBUG_LOG("  Unknown cache structure type!");
            return std::make_unique<MemPageCache>(page_size);
    }
}

} // namespace bptree