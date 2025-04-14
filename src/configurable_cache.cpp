#include "bptree/configurable_cache.h"
#include <iostream>

namespace bptree {

//
// CacheSection implementation
//

Page* CacheSection::fetch_page(PageID id, boost::upgrade_lock<Page>& lock) {
    auto start = start_timer();
    
    switch (structure) {
        case Structure::DirectMapped:
        case Structure::SetAssociative: {
            std::shared_lock<std::shared_mutex> guard(mutex);
            
            // Check if sets are initialized
            if (sets.empty()) {
                return nullptr; // Not found, need to allocate
            }
            
            size_t set_idx = get_set_index(id);
            
            // Check if page is in cache
            for (auto& entry : sets[set_idx]) {
                if (entry.valid && entry.id == id && entry.page) {
                    // Cache hit
                    Page* page = entry.page.get();
                    guard.unlock(); // Release the mutex before acquiring page lock
                    
                    try {
                        lock = boost::upgrade_lock<Page>(*page);
                        
                        // Update statistics
                        std::unique_lock<std::shared_mutex> stats_guard(mutex);
                        stats.accesses++;
                        stats.hits++;
                        stats.avg_hit_time_ms = (stats.avg_hit_time_ms * (stats.hits - 1) + end_timer(start)) / stats.hits;
                        entry.referenced = true;
                        stats_guard.unlock();
                        
                        return page;
                    } catch (std::exception& e) {
                        // Failed to lock the page
                        return nullptr;
                    }
                }
            }
            
            // Cache miss - update statistics
            std::unique_lock<std::shared_mutex> stats_guard(mutex);
            stats.accesses++;
            stats.misses++;
            stats_guard.unlock();
            
            // Need to allocate a new entry
            return nullptr;
        }
        
        case Structure::FullyAssociative: {
            std::shared_lock<std::shared_mutex> guard(mutex);
            
            auto it = page_map.find(id);
            if (it != page_map.end()) {
                auto page_it = it->second;
                if (page_it->valid && page_it->page) {
                    // Cache hit
                    Page* page = page_it->page.get();
                    guard.unlock(); // Release the mutex before acquiring page lock
                    
                    try {
                        lock = boost::upgrade_lock<Page>(*page);
                        
                        // Update LRU list and statistics
                        std::unique_lock<std::shared_mutex> update_guard(mutex);
                        pages.splice(pages.begin(), pages, page_it);
                        stats.accesses++;
                        stats.hits++;
                        stats.avg_hit_time_ms = (stats.avg_hit_time_ms * (stats.hits - 1) + end_timer(start)) / stats.hits;
                        update_guard.unlock();
                        
                        return page;
                    } catch (std::exception& e) {
                        // Failed to lock the page
                        return nullptr;
                    }
                }
            }
            
            // Cache miss - update statistics
            std::unique_lock<std::shared_mutex> stats_guard(mutex);
            stats.accesses++;
            stats.misses++;
            stats_guard.unlock();
            
            return nullptr;
        }
    }
    
    return nullptr;
}

Page* CacheSection::new_page(PageID id, boost::upgrade_lock<Page>& lock) {
    auto start = start_timer();
    
    // We're creating a new page, so it's always a cache miss
    {
        std::unique_lock<std::shared_mutex> stats_guard(mutex);
        stats.accesses++;
        stats.misses++;
    }
    
    return allocate_page(id, lock);
}

void CacheSection::pin_page(Page* page, boost::upgrade_lock<Page>& lock) {
    page->pin();
}

void CacheSection::unpin_page(Page* page, bool dirty, boost::upgrade_lock<Page>& lock) {
    if (dirty) {
        page->set_dirty(true);
    }
    page->unpin();
}

void CacheSection::flush_page(Page* page, boost::upgrade_lock<Page>& lock) {
    // In a real implementation, this would write the page back to disk
    // For this implementation, we just mark it as not dirty
    page->set_dirty(false);
}

void CacheSection::flush_all_pages() {
    switch (structure) {
        case Structure::DirectMapped:
        case Structure::SetAssociative: {
            std::unique_lock<std::shared_mutex> guard(mutex);
            for (auto& set : sets) {
                for (auto& entry : set) {
                    if (entry.valid && entry.page && entry.page->is_dirty()) {
                        boost::upgrade_lock<Page> lock(*entry.page);
                        flush_page(entry.page.get(), lock);
                    }
                }
            }
            break;
        }
        
        case Structure::FullyAssociative: {
            std::unique_lock<std::shared_mutex> guard(mutex);
            for (auto& entry : pages) {
                if (entry.valid && entry.page && entry.page->is_dirty()) {
                    boost::upgrade_lock<Page> lock(*entry.page);
                    flush_page(entry.page.get(), lock);
                }
            }
            break;
        }
    }
}

void CacheSection::prefetch_page(PageID id) {
    // We'd actually fetch the page data here
    // For this implementation, just allocate space in the cache
    
    switch (structure) {
        case Structure::DirectMapped:
        case Structure::SetAssociative: {
            std::unique_lock<std::shared_mutex> guard(mutex);
            size_t set_idx = get_set_index(id);
            
            // Check if already in cache
            for (auto& entry : sets[set_idx]) {
                if (entry.valid && entry.id == id) {
                    return; // Already in cache
                }
            }
            
            // Find a free slot or victim
            for (auto& entry : sets[set_idx]) {
                if (!entry.valid) {
                    // Found free slot
                    entry.page = std::make_unique<Page>(id, line_size);
                    entry.id = id;
                    entry.valid = true;
                    return;
                }
            }
            
            // No free slot, need to evict
            size_t victim_idx = find_victim_in_set(set_idx);
            auto& victim = sets[set_idx][victim_idx];
            
            if (victim.page->is_dirty()) {
                // Would flush the dirty page here
                victim.page->set_dirty(false);
            }
            
            victim.page = std::make_unique<Page>(id, line_size);
            victim.id = id;
            victim.valid = true;
            victim.referenced = false;
            
            break;
        }
        
        case Structure::FullyAssociative: {
            std::unique_lock<std::shared_mutex> guard(mutex);
            
            // Check if already in cache
            auto it = page_map.find(id);
            if (it != page_map.end()) {
                return; // Already in cache
            }
            
            // Check if we have capacity
            if (pages.size() < pages_capacity) {
                // We have room, add to front of LRU list
                pages.emplace_front();
                auto& entry = pages.front();
                entry.page = std::make_unique<Page>(id, line_size);
                entry.id = id;
                entry.valid = true;
                
                // Update page map
                page_map[id] = pages.begin();
            } else {
                // Need to evict
                auto victim_it = find_victim_in_fully_associative();
                
                if (victim_it->page && victim_it->page->is_dirty()) {
                    // Would flush the dirty page here
                    victim_it->page->set_dirty(false);
                }
                
                // Remove old entry from page map
                page_map.erase(victim_it->id);
                
                // Update with new page
                victim_it->page = std::make_unique<Page>(id, line_size);
                victim_it->id = id;
                victim_it->valid = true;
                
                // Move to front of LRU list and update page map
                pages.splice(pages.begin(), pages, victim_it);
                page_map[id] = pages.begin();
            }
            
            break;
        }
    }
}

void CacheSection::prefetch_pages(const std::vector<PageID>& ids) {
    for (const auto& id : ids) {
        prefetch_page(id);
    }
}

CacheSectionStats CacheSection::get_stats() const {
    std::shared_lock<std::shared_mutex> guard(mutex);
    return stats;
}

void CacheSection::reset_stats() {
    std::unique_lock<std::shared_mutex> guard(mutex);
    stats = CacheSectionStats{};
}

void CacheSection::resize(size_t new_size) {
    std::unique_lock<std::shared_mutex> guard(mutex);
    
    if (new_size == size) {
        return; // No change needed
    }
    
    // This is a simple implementation - a more sophisticated one would
    // try to preserve as many pages as possible during resize
    
    size = new_size;
    pages_capacity = size / line_size;
    
    if (structure == Structure::DirectMapped || structure == Structure::SetAssociative) {
        num_sets = size / (line_size * associativity);
        if (num_sets == 0) num_sets = 1;
        
        // Reset all sets
        sets.clear();
        sets.resize(num_sets);
        for (auto& set : sets) {
            set.resize(associativity);
        }
    } else {
        // Fully associative - clear all entries
        pages.clear();
        page_map.clear();
    }
}

size_t CacheSection::page_count() const {
    std::shared_lock<std::shared_mutex> guard(mutex);
    
    size_t count = 0;
    
    if (structure == Structure::DirectMapped || structure == Structure::SetAssociative) {
        for (const auto& set : sets) {
            for (const auto& entry : set) {
                if (entry.valid) {
                    count++;
                }
            }
        }
    } else {
        count = pages.size();
    }
    
    return count;
}

size_t CacheSection::get_set_index(PageID id) const {
    return id % num_sets;
}

size_t CacheSection::find_victim_in_set(size_t set_index) {
    // Simple clock algorithm for replacement
    static size_t clock_hand = 0;
    
    size_t start_pos = clock_hand % associativity;
    size_t pos = start_pos;
    
    do {
        if (!sets[set_index][pos].referenced) {
            clock_hand = (pos + 1) % associativity;
            return pos;
        }
        
        // Clear reference bit and move to next position
        sets[set_index][pos].referenced = false;
        pos = (pos + 1) % associativity;
    } while (pos != start_pos);
    
    // If all are referenced, just return the current clock hand
    clock_hand = (pos + 1) % associativity;
    return pos;
}

std::list<CacheSection::CacheEntry>::iterator CacheSection::find_victim_in_fully_associative() {
    // In fully associative cache, we use LRU
    // The victim is at the back of the list
    return std::prev(pages.end());
}

Page* CacheSection::allocate_page(PageID id, boost::upgrade_lock<Page>& lock)
{
    switch (structure) {
        case Structure::DirectMapped:
        case Structure::SetAssociative: {
            std::unique_lock<std::shared_mutex> guard(mutex);
            
            // Initialize sets if not already done
            if (sets.empty()) {
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
            
            size_t set_idx = get_set_index(id);
            
            // Check for free slot
            for (auto& entry : sets[set_idx]) {
                if (!entry.valid) {
                    entry.page = create_page(id);
                    entry.id = id;
                    entry.valid = true;
                    entry.referenced = true;
                    
                    Page* page = entry.page.get();
                    guard.unlock(); // Unlock the mutex before acquiring page lock
                    
                    try {
                        lock = boost::upgrade_lock<Page>(*page);
                    } catch (std::exception& e) {
                        // Re-lock and handle failure
                        guard.lock();
                        entry.valid = false;
                        entry.page.reset();
                        return nullptr;
                    }
                    
                    return page;
                }
            }
            
            // No free slot, evict a page
            size_t victim_idx = find_victim_in_set(set_idx);
            auto& victim = sets[set_idx][victim_idx];
            
            if (victim.valid && victim.page && victim.page->is_dirty()) {
                // Would normally flush to disk here
                victim.page->set_dirty(false);
            }
            
            // Create a new page
            victim.page = create_page(id);
            victim.id = id;
            victim.valid = true;
            victim.referenced = true;
            
            Page* page = victim.page.get();
            guard.unlock(); // Unlock the mutex before acquiring page lock
            
            try {
                lock = boost::upgrade_lock<Page>(*page);
            } catch (std::exception& e) {
                // Re-lock and handle failure
                guard.lock();
                victim.valid = false;
                victim.page.reset();
                return nullptr;
            }
            
            return page;
        }
        
        case Structure::FullyAssociative: {
            std::unique_lock<std::shared_mutex> guard(mutex);
            
            // Check if we have capacity
            if (pages.size() < pages_capacity) {
                // We have room, add to front of LRU list
                pages.emplace_front();
                auto& entry = pages.front();
                entry.page = create_page(id);
                entry.id = id;
                entry.valid = true;
                
                // Update page map
                page_map[id] = pages.begin();
                
                Page* page = entry.page.get();
                guard.unlock(); // Unlock the mutex before acquiring page lock
                
                try {
                    lock = boost::upgrade_lock<Page>(*page);
                } catch (std::exception& e) {
                    // Re-lock and handle failure
                    guard.lock();
                    entry.valid = false;
                    entry.page.reset();
                    page_map.erase(id);
                    pages.pop_front();
                    return nullptr;
                }
                
                return page;
            } else if (!pages.empty()) {
                // Need to evict
                auto victim_it = find_victim_in_fully_associative();
                
                if (victim_it->valid && victim_it->page && victim_it->page->is_dirty()) {
                    // Would flush to disk here
                    victim_it->page->set_dirty(false);
                }
                
                // Remove old entry from page map
                if (victim_it->valid) {
                    page_map.erase(victim_it->id);
                }
                
                // Update with new page
                victim_it->page = create_page(id);
                victim_it->id = id;
                victim_it->valid = true;
                
                // Move to front of LRU list and update page map
                pages.splice(pages.begin(), pages, victim_it);
                page_map[id] = pages.begin();
                
                Page* page = victim_it->page.get();
                guard.unlock(); // Unlock the mutex before acquiring page lock
                
                try {
                    lock = boost::upgrade_lock<Page>(*page);
                } catch (std::exception& e) {
                    // Re-lock and handle failure
                    guard.lock();
                    victim_it->valid = false;
                    victim_it->page.reset();
                    page_map.erase(id);
                    return nullptr;
                }
                
                return page;
            } else {
                // No pages yet, create first one
                pages.emplace_front();
                auto& entry = pages.front();
                entry.page = create_page(id);
                entry.id = id;
                entry.valid = true;
                
                // Update page map
                page_map[id] = pages.begin();
                
                Page* page = entry.page.get();
                guard.unlock(); // Unlock the mutex before acquiring page lock
                
                try {
                    lock = boost::upgrade_lock<Page>(*page);
                } catch (std::exception& e) {
                    // Re-lock and handle failure
                    guard.lock();
                    entry.valid = false;
                    entry.page.reset();
                    page_map.erase(id);
                    pages.pop_front();
                    return nullptr;
                }
                
                return page;
            }
        }
    }
    
    return nullptr; // Should never reach here
}


//
// ConfigurableCache implementation
//

ConfigurableCache::ConfigurableCache(size_t total_size, size_t page_size, size_t default_line_size)
    : total_size(total_size), available_size(total_size), page_size(page_size), next_section_id(0), next_page_id(0)
{
    // Create default section (fully associative)
    default_section_id = create_section(
        total_size,
        default_line_size,
        CacheSection::Structure::FullyAssociative
    );
}

Page* ConfigurableCache::new_page(boost::upgrade_lock<Page>& lock)
{
    std::lock_guard<std::shared_mutex> guard(mutex);

    // Special handling for the first page (metadata page)
    if (next_page_id == 0) {
        next_page_id = 1; // Reserve 0 for special cases
    }
    
    PageID new_id = next_page_id++;
    
    // Find the appropriate section
    CacheSection* section = find_section_for_page(new_id);
    if (!section) {
        // If no section is found (shouldn't happen), use default section
        section = sections[default_section_id].get();
    }
    
    // Allocate the page in the selected section
    Page* page = section->new_page(new_id, lock);
    if (page) {
        section->pin_page(page, lock);
    }
    
    return page;
}

Page* ConfigurableCache::fetch_page(PageID id, boost::upgrade_lock<Page>& lock) {
    CacheSection* section = find_section_for_page(id);
    
    // Try to fetch from the appropriate section
    Page* page = section->fetch_page(id, lock);
    
    if (!page) {
        // Cache miss - allocate in the appropriate section
        page = section->new_page(id, lock);
    }
    
    return page;
}

void ConfigurableCache::pin_page(Page* page, boost::upgrade_lock<Page>& lock) {
    if (!page) return;
    
    CacheSection* section = find_section_for_page(page->get_id());
    section->pin_page(page, lock);
}

void ConfigurableCache::unpin_page(Page* page, bool dirty, boost::upgrade_lock<Page>& lock) {
    if (!page) return;
    
    CacheSection* section = find_section_for_page(page->get_id());
    section->unpin_page(page, dirty, lock);
}

void ConfigurableCache::flush_page(Page* page, boost::upgrade_lock<Page>& lock) {
    if (!page) return;
    
    CacheSection* section = find_section_for_page(page->get_id());
    section->flush_page(page, lock);
}

void ConfigurableCache::flush_all_pages() {
    std::shared_lock<std::shared_mutex> guard(mutex);
    
    for (auto& section_pair : sections) {
        section_pair.second->flush_all_pages();
    }
}

size_t ConfigurableCache::size() const {
    std::shared_lock<std::shared_mutex> guard(mutex);
    
    size_t total = 0;
    for (const auto& section_pair : sections) {
        total += section_pair.second->page_count();
    }
    
    return total;
}

size_t ConfigurableCache::get_page_size() const {
    return page_size;
}

void ConfigurableCache::prefetch_page(PageID id) {
    CacheSection* section = find_section_for_page(id);
    section->prefetch_page(id);
}

void ConfigurableCache::prefetch_pages(const std::vector<PageID>& ids) {
    // Group pages by section for efficiency
    std::unordered_map<size_t, std::vector<PageID>> section_pages;
    
    for (const auto& id : ids) {
        size_t section_id = get_section_for_page(id);
        section_pages[section_id].push_back(id);
    }
    
    // Prefetch each group
    for (const auto& entry : section_pages) {
        if (sections.count(entry.first) > 0) {
            sections[entry.first]->prefetch_pages(entry.second);
        }
    }
}

size_t ConfigurableCache::create_section(size_t size, size_t line_size, 
                                      CacheSection::Structure structure, 
                                      size_t associativity) {
    std::unique_lock<std::shared_mutex> guard(mutex);
    
    // Make sure we have enough space
    if (size > available_size) {
        // Not enough space, allocate what we can
        size = available_size;
    }
    
    // Create the section
    size_t section_id = next_section_id++;
    sections[section_id] = std::make_unique<CacheSection>(
        section_id, size, line_size, structure, associativity
    );
    
    // Update available size
    available_size -= size;
    
    return section_id;
}

void ConfigurableCache::remove_section(size_t section_id) {
    std::unique_lock<std::shared_mutex> guard(mutex);
    
    // Cannot remove default section
    if (section_id == default_section_id) {
        return;
    }
    
    auto it = sections.find(section_id);
    if (it != sections.end()) {
        // Return space to available pool
        available_size += it->second->get_size();
        
        // Remove section
        sections.erase(it);
        
        // Update page mappings
        for (auto it = page_to_section_map.begin(); it != page_to_section_map.end();) {
            if (it->second == section_id) {
                it = page_to_section_map.erase(it);
            } else {
                ++it;
            }
        }
        
        // Update range mappings
        for (size_t i = 0; i < page_ranges.size(); ++i) {
            if (range_section_ids[i] == section_id) {
                page_ranges.erase(page_ranges.begin() + i);
                range_section_ids.erase(range_section_ids.begin() + i);
                --i;
            }
        }
    }
}

void ConfigurableCache::resize_section(size_t section_id, size_t new_size) {
    std::unique_lock<std::shared_mutex> guard(mutex);
    
    auto it = sections.find(section_id);
    if (it == sections.end()) {
        return; // Section not found
    }
    
    size_t old_size = it->second->get_size();
    
    // Check if we have enough available space
    if (new_size > old_size) {
        size_t additional_size = new_size - old_size;
        if (additional_size > available_size) {
            // Not enough space, adjust new size
            new_size = old_size + available_size;
        }
        
        // Update available size
        available_size -= (new_size - old_size);
    } else {
        // Returning space to available pool
        available_size += (old_size - new_size);
    }
    
    // Resize the section
    it->second->resize(new_size);
}

void ConfigurableCache::map_page_to_section(PageID id, size_t section_id) {
    std::unique_lock<std::shared_mutex> guard(mutex);
    
    if (sections.find(section_id) == sections.end()) {
        return; // Invalid section
    }
    
    page_to_section_map[id] = section_id;
}

void ConfigurableCache::map_page_range_to_section(PageID start, PageID end, size_t section_id) {
    std::unique_lock<std::shared_mutex> guard(mutex);
    
    if (sections.find(section_id) == sections.end()) {
        return; // Invalid section
    }
    
    // Check for overlapping ranges and remove them
    for (size_t i = 0; i < page_ranges.size(); ++i) {
        auto& range = page_ranges[i];
        
        // Check if ranges overlap
        if (!(range.second < start || range.first > end)) {
            // Ranges overlap, remove the old range
            page_ranges.erase(page_ranges.begin() + i);
            range_section_ids.erase(range_section_ids.begin() + i);
            --i;
        }
    }
    
    // Add the new range
    page_ranges.emplace_back(start, end);
    range_section_ids.push_back(section_id);
}

size_t ConfigurableCache::get_section_for_page(PageID id) const {
    std::shared_lock<std::shared_mutex> guard(mutex);
    
    // Check specific page mapping
    auto it = page_to_section_map.find(id);
    if (it != page_to_section_map.end()) {
        return it->second;
    }
    
    // Check range mappings
    for (size_t i = 0; i < page_ranges.size(); ++i) {
        if (id >= page_ranges[i].first && id <= page_ranges[i].second) {
            return range_section_ids[i];
        }
    }
    
    // Default section
    return default_section_id;
}

std::vector<CacheSectionStats> ConfigurableCache::get_all_section_stats() const {
    std::shared_lock<std::shared_mutex> guard(mutex);
    
    std::vector<CacheSectionStats> stats;
    
    for (const auto& section_pair : sections) {
        stats.push_back(section_pair.second->get_stats());
    }
    
    return stats;
}

void ConfigurableCache::reset_all_stats() {
    std::shared_lock<std::shared_mutex> guard(mutex);
    
    for (auto& section_pair : sections) {
        section_pair.second->reset_stats();
    }
}

void ConfigurableCache::optimize_section_sizes() {
    // For simplicity, we'll use a simple heuristic-based approach
    simple_size_optimization();
}

CacheSection* ConfigurableCache::find_section_for_page(PageID id) const {
    std::shared_lock<std::shared_mutex> guard(mutex);
    
    // Check specific page mapping
    auto it = page_to_section_map.find(id);
    if (it != page_to_section_map.end()) {
        auto section_it = sections.find(it->second);
        if (section_it != sections.end()) {
            return section_it->second.get();
        }
    }
    
    // Check range mappings
    for (size_t i = 0; i < page_ranges.size(); ++i) {
        if (id >= page_ranges[i].first && id <= page_ranges[i].second) {
            auto section_it = sections.find(range_section_ids[i]);
            if (section_it != sections.end()) {
                return section_it->second.get();
            }
        }
    }
    
    // Default section
    auto section_it = sections.find(default_section_id);
    if (section_it != sections.end()) {
        return section_it->second.get();
    }
    
    // This should not happen if the cache is properly initialized
    if (!sections.empty()) {
        return sections.begin()->second.get();
    }
    
    return nullptr;
}

void ConfigurableCache::simple_size_optimization() {
    std::unique_lock<std::shared_mutex> guard(mutex);
    
    // Collect section statistics
    std::vector<std::pair<size_t, CacheSectionStats>> section_stats;
    size_t total_allocated = 0;
    
    for (const auto& section_pair : sections) {
        section_stats.emplace_back(section_pair.first, section_pair.second->get_stats());
        total_allocated += section_pair.second->get_size();
    }
    
    // If no statistics available or only one section, nothing to optimize
    if (section_stats.size() <= 1) {
        return;
    }
    
    // Sort sections by miss rate (highest to lowest)
    std::sort(section_stats.begin(), section_stats.end(), 
              [](const auto& a, const auto& b) {
                  return a.second.miss_rate() > b.second.miss_rate();
              });
    
    // Calculate optimal cache distribution based on miss rates
    // (Simple heuristic: allocate proportional to miss rate)
    std::vector<double> miss_rates;
    double total_miss_rate = 0.0;
    
    for (const auto& stat : section_stats) {
        double rate = stat.second.miss_rate();
        miss_rates.push_back(rate);
        total_miss_rate += rate;
    }
    
    // If total miss rate is 0, no optimization needed
    if (total_miss_rate <= 0.0) {
        return;
    }
    
    // Calculate new sizes
    std::vector<size_t> new_sizes;
    size_t total_size_to_allocate = total_allocated + available_size;
    
    for (size_t i = 0; i < section_stats.size(); ++i) {
        double proportion = miss_rates[i] / total_miss_rate;
        size_t new_size = static_cast<size_t>(proportion * total_size_to_allocate);
        
        // Ensure minimum size
        if (new_size < sections[section_stats[i].first]->get_line_size() * 2) {
            new_size = sections[section_stats[i].first]->get_line_size() * 2;
        }
        
        new_sizes.push_back(new_size);
    }
    
    // Adjust sizes to match total
    size_t total_new_size = std::accumulate(new_sizes.begin(), new_sizes.end(), 0ULL);
    
    if (total_new_size > total_size_to_allocate) {
        // Need to reduce some sizes
        size_t excess = total_new_size - total_size_to_allocate;
        
        // Reduce from sections with lowest miss rates
        for (size_t i = section_stats.size() - 1; i > 0 && excess > 0; --i) {
            size_t max_reduction = new_sizes[i] - sections[section_stats[i].first]->get_line_size() * 2;
            size_t reduction = std::min(excess, max_reduction);
            
            new_sizes[i] -= reduction;
            excess -= reduction;
        }
    } else if (total_new_size < total_size_to_allocate) {
        // Extra space to allocate
        size_t extra = total_size_to_allocate - total_new_size;
        
        // Allocate to sections with highest miss rates
        for (size_t i = 0; i < section_stats.size() && extra > 0; ++i) {
            size_t addition = std::min(extra, extra / (section_stats.size() - i));
            new_sizes[i] += addition;
            extra -= addition;
        }
    }
    
    // Apply new sizes
    guard.unlock(); // Release lock while resizing
    
    for (size_t i = 0; i < section_stats.size(); ++i) {
        resize_section(section_stats[i].first, new_sizes[i]);
    }
}

} // namespace bptree