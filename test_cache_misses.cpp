// test_cache_misses.cpp
#include "bptree/simplified_configurable_cache.h"
#include "bptree/tree.h"

#include <iostream>
#include <vector>
#include <chrono>
#include <random>
#include <thread>

int main() {
    std::cout << "=== CACHE MISS RATE VERIFICATION TEST ===\n\n";
    
    // Create a TINY cache - only 10 pages = 40KB
    const size_t CACHE_SIZE = 10 * 4096;
    std::cout << "Creating cache with only " << CACHE_SIZE/1024 << "KB capacity (10 pages)\n";
    
    bptree::SimplifiedConfigurableCache cache(
        CACHE_SIZE,
        4096,  // page size
        4096,  // line size
        bptree::SimplifiedConfigurableCache::Structure::FullyAssociative,
        8,
        true   // enable debug mode
    );
    
    std::cout << "Cache capacity: " << cache.get_capacity() << " pages\n\n";
    
    // PART 1: DIRECT CACHE ACCESS TEST
    std::cout << "PART 1: Direct cache access test\n";
    
    // Directly insert 20 pages (more than the cache capacity)
    std::cout << "Directly inserting 20 pages into the 10-page cache...\n";
    
    std::vector<bptree::PageID> page_ids;
    
    for (int i = 1; i <= 20; ++i) {
        boost::upgrade_lock<bptree::Page> lock;
        bptree::Page* page = cache.new_page(lock);
        
        // Write something to the page
        {
            boost::upgrade_to_unique_lock<bptree::Page> ulock(lock);
            auto* buf = page->get_buffer(ulock);
            *(reinterpret_cast<uint64_t*>(buf)) = i;
        }
        
        page_ids.push_back(page->get_id());
        cache.unpin_page(page, true, lock);
        
        // Small delay to give debug output time to flush
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    
    std::cout << "\nCache size after creating 20 pages: " << cache.size() << "/" << cache.get_capacity() << "\n";
    
    // Reset stats
    cache.reset_stats();
    std::cout << "Stats reset.\n";
    
    // First: Access the first 5 pages (should be cache misses if eviction worked)
    std::cout << "\nAccessing first 5 pages (should be misses if eviction worked)...\n";
    
    for (int i = 0; i < 5; ++i) {
        boost::upgrade_lock<bptree::Page> lock;
        bptree::PageID id = page_ids[i];
        
        std::cout << "Checking if page " << id << " is in cache: " << (cache.is_page_in_cache(id) ? "YES" : "NO") << std::endl;
        
        bptree::Page* page = cache.fetch_page(id, lock);
        
        if (page) {
            // Read from the page
            auto* buf = page->get_buffer(lock);
            uint64_t value = *(reinterpret_cast<const uint64_t*>(buf));
            std::cout << "Page " << id << " contains value: " << value << std::endl;
            
            cache.unpin_page(page, false, lock);
        } else {
            std::cout << "Failed to fetch page " << id << std::endl;
        }
        
        // Small delay to give debug output time to flush
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    
    // Print stats after first set of accesses
    auto stats = cache.get_stats();
    std::cout << "\nStats after accessing first 5 pages:\n";
    stats.print();
    
    // Check cache size
    std::cout << "Cache size: " << cache.size() << "/" << cache.get_capacity() << "\n";
    
    // Second: Access the last 5 pages (should mostly be hits since they were added last)
    std::cout << "\nAccessing last 5 pages (should be hits since they were added last)...\n";
    
    for (int i = 15; i < 20; ++i) {
        boost::upgrade_lock<bptree::Page> lock;
        bptree::PageID id = page_ids[i];
        
        std::cout << "Checking if page " << id << " is in cache: " << (cache.is_page_in_cache(id) ? "YES" : "NO") << std::endl;
        
        bptree::Page* page = cache.fetch_page(id, lock);
        
        if (page) {
            // Read from the page
            auto* buf = page->get_buffer(lock);
            uint64_t value = *(reinterpret_cast<const uint64_t*>(buf));
            std::cout << "Page " << id << " contains value: " << value << std::endl;
            
            cache.unpin_page(page, false, lock);
        } else {
            std::cout << "Failed to fetch page " << id << std::endl;
        }
        
        // Small delay to give debug output time to flush
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    
    // Print final stats
    stats = cache.get_stats();
    std::cout << "\nFinal direct cache test results:\n";
    stats.print();
    cache.dump_status();
    
    return 0;
}