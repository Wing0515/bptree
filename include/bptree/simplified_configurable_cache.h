// include/bptree/simplified_configurable_cache.h
#ifndef _BPTREE_SIMPLIFIED_CONFIGURABLE_CACHE_H_
#define _BPTREE_SIMPLIFIED_CONFIGURABLE_CACHE_H_

#include "bptree/page_cache.h"
#include "bptree/latency_simulator.h"

#include <atomic>
#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>
#include <string>
#include <algorithm>
#include <memory>
#include <iostream>
#include <cassert>

namespace bptree {

/**
 * Statistics for cache performance monitoring
 */
struct CacheStats {
    size_t accesses{0};
    size_t hits{0};
    size_t misses{0};
    double avg_hit_time_ms{0};
    double avg_miss_time_ms{0};
    
    double miss_rate() const {
        return accesses > 0 ? static_cast<double>(misses) / accesses : 0.0;
    }
    
    void print() const {
        std::cout << "Cache statistics:\n"
                  << "  Accesses: " << accesses << "\n"
                  << "  Hits: " << hits << "\n"
                  << "  Misses: " << misses << "\n"
                  << "  Miss rate: " << (miss_rate() * 100.0) << "%\n"
                  << "  Avg hit time: " << avg_hit_time_ms << " ms\n"
                  << "  Avg miss time: " << avg_miss_time_ms << " ms\n";
    }
};

/**
 * A simplified configurable cache based on MemPageCache with additional
 * configurable properties for optimizing B+Tree performance. This implementation
 * focuses on reliability over complexity.
 */
class SimplifiedConfigurableCache : public AbstractPageCache {
public:
    // Cache structure types (inspired by Mira paper)
    enum class Structure {
        DirectMapped,   // Simple direct mapping (like basic CPU cache)
        SetAssociative, // Set associative caching (like most CPU caches)
        FullyAssociative // Fully associative (most flexible, higher overhead)
    };

    /**
     * Create a new configurable cache
     * @param total_size Total size of the cache in bytes
     * @param page_size Size of a single page in bytes
     * @param line_size Size of a cache line in bytes (should be >= page_size)
     * @param structure Cache structure type
     * @param associativity Number of ways for set-associative cache
     * @param debug Enable debug output
     */
    SimplifiedConfigurableCache(
        size_t total_size, 
        size_t page_size = 4096,
        size_t line_size = 4096, 
        Structure structure = Structure::FullyAssociative,
        size_t associativity = 8,
        bool debug = false) 
        : page_size(page_size),
          line_size(line_size),
          structure(structure),
          associativity(associativity),
          next_id(1), // Start from 1 to match B+Tree expectations
          debug_mode(debug)
    {
        // Calculate capacity
        capacity = total_size / page_size;
        if (capacity == 0) capacity = 1;
        
        if (debug_mode) {
            std::cout << "Created cache with:\n"
                      << "  Total size: " << total_size << " bytes\n"
                      << "  Page size: " << page_size << " bytes\n"
                      << "  Line size: " << line_size << " bytes\n"
                      << "  Structure: " << structure_name(structure) << "\n"
                      << "  Associativity: " << associativity << "\n"
                      << "  Capacity: " << capacity << " pages\n";
        }
    }

    // AbstractPageCache interface implementation
    virtual Page* new_page(boost::upgrade_lock<Page>& lock) override {
        std::unique_lock<std::shared_mutex> guard(mutex);
        
        PageID id = next_id.fetch_add(1);
        auto page = std::make_unique<Page>(id, page_size);
        auto* page_ptr = page.get();
        
        // Check if we need to evict a page before adding this one
        if (page_map.size() >= capacity) {
            if (debug_mode) {
                std::cout << "Cache full (" << page_map.size() << "/" << capacity << " pages), trying to evict a page before creating new page " << id << std::endl;
            }
            evictPage();
        }
        
        // Add to cache
        page_map[id] = std::move(page);
        
        // Update LRU
        updateLRU(id);
        
        // Set lock before returning
        lock = boost::upgrade_lock<Page>(*page_ptr);
        
        // Pin the page so it won't be evicted while in use
        page_ptr->pin();
        
        if (debug_mode) {
            std::cout << "Created new page with ID " << id << ", cache size now: " << page_map.size() << "/" << capacity << std::endl;
        }
        
        return page_ptr;
    }

    virtual Page* fetch_page(PageID id, boost::upgrade_lock<Page>& lock) override {
        auto start = std::chrono::high_resolution_clock::now();
        
        // First check if page exists in cache (with shared lock)
        {
            std::shared_lock<std::shared_mutex> guard(mutex);
            
            auto it = page_map.find(id);
            if (it != page_map.end()) {
                // Cache hit
                auto* page = it->second.get();
                
                // Try to acquire lock outside of our mutex to avoid deadlock
                guard.unlock();
                
                try {
                    lock = boost::upgrade_lock<Page>(*page);
                    
                    // Pin the page so it won't be evicted while in use
                    page->pin();
                    
                    // Update stats and LRU (needs exclusive lock)
                    std::unique_lock<std::shared_mutex> stats_guard(mutex);
                    stats.hits++;
                    stats.accesses++;
                    auto duration = std::chrono::high_resolution_clock::now() - start;
                    double ms = std::chrono::duration<double, std::milli>(duration).count();
                    stats.avg_hit_time_ms = (stats.avg_hit_time_ms * (stats.hits - 1) + ms) / stats.hits;
                    
                    // Update LRU 
                    updateLRU(id);
                    
                    if (debug_mode) {
                        std::cout << "Cache HIT for page " << id << std::endl;
                    }
                    
                    return page;
                }
                catch (const std::exception& e) {
                    // Lock failed, continue to cache miss path
                    if (debug_mode) {
                        std::cout << "Lock failed for page " << id << ": " << e.what() << std::endl;
                    }
                }
            }
        }
        
        // Cache miss path - we need exclusive lock
        std::unique_lock<std::shared_mutex> guard(mutex);
        
        // Double-check if page appeared since we last checked
        auto it = page_map.find(id);
        if (it != page_map.end()) {
            auto* page = it->second.get();
            
            // Try to acquire lock
            guard.unlock();
            try {
                lock = boost::upgrade_lock<Page>(*page);
                
                // Pin the page
                page->pin();
                
                // Update hit stats
                std::unique_lock<std::shared_mutex> stats_guard(mutex);
                stats.hits++;
                stats.accesses++;
                auto duration = std::chrono::high_resolution_clock::now() - start;
                double ms = std::chrono::duration<double, std::milli>(duration).count();
                stats.avg_hit_time_ms = (stats.avg_hit_time_ms * (stats.hits - 1) + ms) / stats.hits;
                
                // Update LRU
                updateLRU(id);
                
                if (debug_mode) {
                    std::cout << "Cache HIT on double-check for page " << id << std::endl;
                }
                
                return page;
            }
            catch (const std::exception& e) {
                // Lock failed, proceed to create new page
                if (debug_mode) {
                    std::cout << "Lock failed on double-check for page " << id << ": " << e.what() << std::endl;
                }
            }
            guard.lock(); // Re-lock for the rest of the function
        }
        
        // THIS IS A CACHE MISS - Record it
        stats.misses++;
        stats.accesses++;
        
        if (debug_mode) {
            std::cout << "Cache MISS for page " << id << std::endl;
        }
        
        // Use simulated far memory latency
        LatencySimulator::simulate_network_latency();
        
        // Check if we need to evict a page before adding this one
        if (page_map.size() >= capacity) {
            if (debug_mode) {
                std::cout << "Cache full (" << page_map.size() << "/" << capacity << " pages), trying to evict a page before fetching page " << id << std::endl;
            }
            evictPage();
        }
        
        // Create new page
        auto page = std::make_unique<Page>(id, page_size);
        auto* page_ptr = page.get();
        
        // Add to cache
        page_map[id] = std::move(page);
        
        // Update LRU
        updateLRU(id);
        
        // Set lock before returning
        guard.unlock();
        lock = boost::upgrade_lock<Page>(*page_ptr);
        
        // Pin the page so it won't be evicted while in use
        page_ptr->pin();
        
        // Record miss time
        auto duration = std::chrono::high_resolution_clock::now() - start;
        double ms = std::chrono::duration<double, std::milli>(duration).count();
        
        {
            std::unique_lock<std::shared_mutex> stats_guard(mutex);
            stats.avg_miss_time_ms = (stats.avg_miss_time_ms * (stats.misses - 1) + ms) / stats.misses;
            
            if (debug_mode) {
                std::cout << "Created/fetched page " << id << ", cache size now: " << page_map.size() << "/" << capacity << std::endl;
            }
        }
        
        return page_ptr;
    }

    virtual void pin_page(Page* page, boost::upgrade_lock<Page>& lock) override {
        if (!page) return;
        page->pin();
        
        if (debug_mode) {
            std::cout << "Pinned page " << page->get_id() << std::endl;
        }
    }

    virtual void unpin_page(Page* page, bool dirty, boost::upgrade_lock<Page>& lock) override {
        if (!page) return;
        
        if (dirty) {
            page->set_dirty(true);
        }
        
        int pin_count = page->unpin();
        
        if (debug_mode) {
            std::cout << "Unpinned page " << page->get_id() 
                     << (dirty ? " (dirty)" : "") 
                     << ", pin count now: " << pin_count << std::endl;
        }
    }

    virtual void flush_page(Page* page, boost::upgrade_lock<Page>& lock) override {
        if (!page) return;
        page->set_dirty(false);
        
        if (debug_mode) {
            std::cout << "Flushed page " << page->get_id() << std::endl;
        }
    }

    virtual void flush_all_pages() override {
        std::shared_lock<std::shared_mutex> guard(mutex);
        
        for (auto& entry : page_map) {
            if (entry.second->is_dirty()) {
                entry.second->set_dirty(false);
                
                if (debug_mode) {
                    std::cout << "Flushed dirty page " << entry.first << std::endl;
                }
            }
        }
    }

    virtual size_t size() const override { 
        std::shared_lock<std::shared_mutex> guard(mutex);
        return page_map.size();
    }
    
    virtual size_t get_page_size() const override { 
        return page_size; 
    }
    
    virtual void prefetch_page(PageID id) override {
        if (debug_mode) {
            std::cout << "Prefetching page " << id << std::endl;
        }
        
        boost::upgrade_lock<Page> lock;
        fetch_page(id, lock);
        
        // Immediately unpin the page since it was just for prefetching
        if (lock.owns_lock()) {
            Page* page = get_page_from_lock(lock);
            if (page) {
                unpin_page(page, false, lock);
            }
        }
    }
    
    virtual void prefetch_pages(const std::vector<PageID>& ids) override {
        for (PageID id : ids) {
            prefetch_page(id);
        }
    }
    
    /**
     * Configure the cache structure
     * @param structure The cache structure type
     * @param line_size The cache line size in bytes
     * @param associativity The associativity for set-associative cache
     */
    void configure(Structure structure, size_t line_size, size_t associativity = 8) {
        std::unique_lock<std::shared_mutex> guard(mutex);
        this->structure = structure;
        this->line_size = line_size;
        this->associativity = associativity;
        
        if (debug_mode) {
            std::cout << "Reconfigured cache with:\n"
                      << "  Structure: " << structure_name(structure) << "\n"
                      << "  Line size: " << line_size << " bytes\n"
                      << "  Associativity: " << associativity << "\n";
        }
    }
    
    /**
     * Get current cache statistics
     */
    CacheStats get_stats() const {
        std::shared_lock<std::shared_mutex> guard(mutex);
        return stats;
    }
    
    /**
     * Reset cache statistics
     */
    void reset_stats() {
        std::unique_lock<std::shared_mutex> guard(mutex);
        if (debug_mode) {
            std::cout << "Reset cache statistics" << std::endl;
        }
        stats = CacheStats{};
    }
    
    /**
     * Enable or disable debug mode
     */
    void set_debug(bool enable) {
        debug_mode = enable;
    }
    
    /**
     * Return the cache capacity in pages
     */
    size_t get_capacity() const {
        return capacity;
    }
    
    /**
     * Return the current structure name as string
     */
    std::string structure_name(Structure s) const {
        switch (s) {
            case Structure::DirectMapped: return "Direct-Mapped";
            case Structure::SetAssociative: return "Set-Associative";
            case Structure::FullyAssociative: return "Fully-Associative";
            default: return "Unknown";
        }
    }
    
    /**
     * Dump cache status (for debugging)
     */
    void dump_status() {
        std::shared_lock<std::shared_mutex> guard(mutex);
        
        std::cout << "=== CACHE STATUS ===\n";
        std::cout << "Structure: " << structure_name(structure) << "\n";
        std::cout << "Line size: " << line_size << " bytes\n";
        std::cout << "Associativity: " << associativity << "\n";
        std::cout << "Capacity: " << capacity << " pages\n";
        std::cout << "Current size: " << page_map.size() << " pages\n";
        
        stats.print();
        
        std::cout << "LRU list (most recent first):\n";
        for (size_t i = 0; i < std::min<size_t>(lru_list.size(), 10); i++) {
            std::cout << "  " << lru_list[i] << "\n";
        }
        if (lru_list.size() > 10) {
            std::cout << "  ... (" << (lru_list.size() - 10) << " more)\n";
        }
        
        std::cout << "===================\n";
    }
    
    /**
     * Check if a page is in the cache
     */
    bool is_page_in_cache(PageID id) {
        std::shared_lock<std::shared_mutex> guard(mutex);
        return page_map.find(id) != page_map.end();
    }

private:
    // Cache configuration
    size_t page_size;
    size_t line_size;
    Structure structure;
    size_t associativity;
    size_t capacity;
    
    // Page storage
    std::unordered_map<PageID, std::unique_ptr<Page>> page_map;
    
    // LRU tracking
    std::vector<PageID> lru_list;
    
    // Statistics
    CacheStats stats;
    
    // Debug flag
    bool debug_mode;
    
    // Synchronization
    mutable std::shared_mutex mutex;
    std::atomic<PageID> next_id;
    
    // Helper to get page from lock
    Page* get_page_from_lock(boost::upgrade_lock<Page>& lock) {
        if (!lock.owns_lock()) return nullptr;
        
        // This is a hack to get the page pointer from the lock
        // Find the page in our map with matching ID
        std::shared_lock<std::shared_mutex> guard(mutex);
        
        for (const auto& entry : page_map) {
            if (&(*(entry.second)) == lock.mutex()) {
                return entry.second.get();
            }
        }
        
        return nullptr;
    }
    
    // Update LRU list for a page (must be called with mutex held)
    void updateLRU(PageID id) {
        // Remove if exists
        auto it = std::find(lru_list.begin(), lru_list.end(), id);
        if (it != lru_list.end()) {
            lru_list.erase(it);
        }
        
        // Add to front (most recently used)
        lru_list.insert(lru_list.begin(), id);
    }
    
    // Evict least recently used page (must be called with mutex held)
    void evictPage() {
        if (debug_mode) {
            std::cout << "Trying to evict a page. Cache size: " << page_map.size() 
                      << "/" << capacity << ", LRU list size: " << lru_list.size() << std::endl;
        }
        
        // We'll iterate through the LRU list from least recently used to most recently used
        // Start from back (least recently used)
        for (auto it = lru_list.rbegin(); it != lru_list.rend(); ++it) {
            PageID id = *it;
            
            if (debug_mode) {
                std::cout << "  Checking page " << id << " for eviction" << std::endl;
            }
            
            auto page_it = page_map.find(id);
            if (page_it == page_map.end()) {
                // Page not in map, remove from LRU list
                if (debug_mode) {
                    std::cout << "  Page " << id << " in LRU list but not in page map, removing from LRU" << std::endl;
                }
                lru_list.erase(std::next(it).base());
                continue;
            }
            
            auto& page = page_it->second;
            
            // Check pin count safely without modifying it
            int pin_count = 0;
            {
                try {
                    boost::upgrade_lock<Page> lock(*page);
                    // Now we're locked, can safely check pin count
                    pin_count = page->pin(); // Need to pin to check
                    page->unpin(); // Undo our temporary pin
                    pin_count--; // Adjust for our temporary pin
                }
                catch (std::exception& e) {
                    if (debug_mode) {
                        std::cout << "  Failed to lock page " << id << " for pin count check: " << e.what() << std::endl;
                    }
                    continue; // Try next page
                }
            }
            
            if (debug_mode) {
                std::cout << "  Page " << id << " pin count: " << pin_count << std::endl;
            }
            
            if (pin_count <= 0) {
                // Page is not pinned, can evict
                if (page->is_dirty()) {
                    if (debug_mode) {
                        std::cout << "  Page " << id << " is dirty, would write back to disk here" << std::endl;
                    }
                    // Would actually write back to disk here
                    page->set_dirty(false);
                }
                
                if (debug_mode) {
                    std::cout << "  Evicting page " << id << " from cache" << std::endl;
                }
                
                // Remove from map and LRU list
                page_map.erase(page_it);
                lru_list.erase(std::next(it).base());
                
                // Check that cache size decreased
                if (debug_mode) {
                    std::cout << "  Cache size after eviction: " << page_map.size() << "/" << capacity << std::endl;
                }
                
                return; // Successfully evicted
            }
            else if (debug_mode) {
                std::cout << "  Cannot evict page " << id << " because it's pinned (count: " << pin_count << ")" << std::endl;
            }
        }
        
        if (debug_mode) {
            std::cout << "WARNING: Couldn't find any page to evict!" << std::endl;
            
            // Print all pages and their pin status
            std::cout << "Current pages in cache:" << std::endl;
            for (const auto& entry : page_map) {
                int pin_count = 0;
                try {
                    boost::upgrade_lock<Page> lock(*entry.second);
                    pin_count = entry.second->pin();
                    entry.second->unpin();
                    pin_count--; // Adjust for our temporary pin
                }
                catch (...) {
                    pin_count = -1; // Couldn't determine
                }
                
                std::cout << "  Page " << entry.first 
                          << ", pin count: " << pin_count 
                          << (entry.second->is_dirty() ? " (dirty)" : "")
                          << std::endl;
            }
        }
        
        // If we got here, we couldn't evict any page
        // Rather than failing, we'll just expand the cache
        if (debug_mode) {
            std::cout << "WARNING: All pages are pinned, expanding cache capacity from " 
                      << capacity << " to " << (capacity + 1) << std::endl;
        }
        
        capacity++; // Allow one more page
    }
};

} // namespace bptree

#endif // _BPTREE_SIMPLIFIED_CONFIGURABLE_CACHE_H_