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
     */
    SimplifiedConfigurableCache(
        size_t total_size, 
        size_t page_size = 4096,
        size_t line_size = 4096, 
        Structure structure = Structure::FullyAssociative,
        size_t associativity = 8) 
        : page_size(page_size),
          line_size(line_size),
          structure(structure),
          associativity(associativity),
          next_id(1) // Start from 1 to match B+Tree expectations
    {
        // Calculate capacity
        capacity = total_size / page_size;
        if (capacity == 0) capacity = 1;
        
        // Initialize with empty pages map
    }

    // AbstractPageCache interface implementation
    virtual Page* new_page(boost::upgrade_lock<Page>& lock) override {
        std::unique_lock<std::shared_mutex> guard(mutex);
        
        PageID id = next_id.fetch_add(1);
        auto page = std::make_unique<Page>(id, page_size);
        auto* page_ptr = page.get();
        
        // Add to cache
        page_map[id] = std::move(page);
        
        // Set lock before returning
        lock = boost::upgrade_lock<Page>(*page_ptr);
        
        // Update LRU
        updateLRU(id);
        
        return page_ptr;
    }

    virtual Page* fetch_page(PageID id, boost::upgrade_lock<Page>& lock) override {
        auto start = std::chrono::high_resolution_clock::now();
        {
            // First try with shared lock
            std::shared_lock<std::shared_mutex> guard(mutex);
            
            auto it = page_map.find(id);
            if (it != page_map.end()) {
                // Cache hit
                auto* page = it->second.get();
                
                // Try to acquire lock outside of our mutex to avoid deadlock
                guard.unlock();
                
                try {
                    lock = boost::upgrade_lock<Page>(*page);
                    
                    // Update stats and LRU (needs exclusive lock)
                    std::unique_lock<std::shared_mutex> stats_guard(mutex);
                    stats.hits++;
                    stats.accesses++;
                    auto duration = std::chrono::high_resolution_clock::now() - start;
                    double ms = std::chrono::duration<double, std::milli>(duration).count();
                    stats.avg_hit_time_ms = (stats.avg_hit_time_ms * (stats.hits - 1) + ms) / stats.hits;
                    
                    // Update LRU 
                    updateLRU(id);
                    
                    return page;
                }
                catch (const std::exception& e) {
                    // Lock failed, continue to cache miss path
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
                
                // Update hit stats
                std::unique_lock<std::shared_mutex> stats_guard(mutex);
                stats.hits++;
                stats.accesses++;
                auto duration = std::chrono::high_resolution_clock::now() - start;
                double ms = std::chrono::duration<double, std::milli>(duration).count();
                stats.avg_hit_time_ms = (stats.avg_hit_time_ms * (stats.hits - 1) + ms) / stats.hits;
                
                // Update LRU
                updateLRU(id);
                
                return page;
            }
            catch (const std::exception& e) {
                // Lock failed, proceed to create new page
            }
        }
        
        // Miss stats
        stats.misses++;
        stats.accesses++;
        
        // Use simulated far memory latency
        LatencySimulator::simulate_network_latency();
        
        // Create new page
        auto page = std::make_unique<Page>(id, page_size);
        auto* page_ptr = page.get();
        
        // If we're at capacity, evict a page
        if (page_map.size() >= capacity) {
            evictPage();
        }
        
        // Add to cache
        page_map[id] = std::move(page);
        
        // Update LRU
        updateLRU(id);
        
        // Set lock before returning
        guard.unlock();
        lock = boost::upgrade_lock<Page>(*page_ptr);
        
        return page_ptr;
    }

    virtual void pin_page(Page* page, boost::upgrade_lock<Page>& lock) override {
        if (!page) return;
        page->pin();
    }

    virtual void unpin_page(Page* page, bool dirty, boost::upgrade_lock<Page>& lock) override {
        if (!page) return;
        
        if (dirty) {
            page->set_dirty(true);
        }
        
        page->unpin();
    }

    virtual void flush_page(Page* page, boost::upgrade_lock<Page>& lock) override {
        if (!page) return;
        page->set_dirty(false);
    }

    virtual void flush_all_pages() override {
        std::shared_lock<std::shared_mutex> guard(mutex);
        
        for (auto& entry : page_map) {
            if (entry.second->is_dirty()) {
                entry.second->set_dirty(false);
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
        boost::upgrade_lock<Page> lock;
        fetch_page(id, lock);
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
        stats = CacheStats{};
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
    
    // Synchronization
    mutable std::shared_mutex mutex;
    std::atomic<PageID> next_id;
    
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
        // Start from the back of the LRU list
        for (auto it = lru_list.rbegin(); it != lru_list.rend(); ++it) {
            PageID id = *it;
            auto page_it = page_map.find(id);
            
            if (page_it != page_map.end()) {
                // Only remove if pin count is 0
                if (page_it->second->unpin() <= 0) {
                    // Write back if dirty
                    if (page_it->second->is_dirty()) {
                        // Would normally write to backing store here
                        page_it->second->set_dirty(false);
                    }
                    
                    // Remove from map and LRU
                    page_map.erase(page_it);
                    lru_list.erase(std::next(it).base());
                    return;
                }
            }
        }
        
        // If we get here, all pages are pinned. In a real system we might
        // wait or throw an exception, but for testing we'll just not evict.
    }
};

} // namespace bptree

#endif // _BPTREE_SIMPLIFIED_CONFIGURABLE_CACHE_H_