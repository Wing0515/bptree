#ifndef _BPTREE_CACHE_PROFILER_H_
#define _BPTREE_CACHE_PROFILER_H_

#include <atomic>
#include <chrono>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include "bptree/page.h"

namespace bptree {

enum class AccessType {
    SEQUENTIAL,
    RANDOM,
    UNKNOWN
};

struct PageAccessStatsSnapshot {
    uint64_t hits;
    uint64_t misses;
    uint64_t reads;
    uint64_t writes;
    uint64_t total_access_time_ns;
    PageID last_accessed_page;
    int sequential_count;
    AccessType detected_pattern;
    
    double hit_rate() const {
        uint64_t total = hits + misses;
        return total > 0 ? (double)hits / total : 0.0;
    }
    
    double avg_access_time_ns() const {
        uint64_t total = hits + misses;
        return total > 0 ? (double)total_access_time_ns / total : 0.0;
    }
    
    AccessType access_pattern() const {
        return detected_pattern;
    }
};

struct PageAccessStats {
    std::atomic<uint64_t> hits{0};
    std::atomic<uint64_t> misses{0};
    std::atomic<uint64_t> reads{0};
    std::atomic<uint64_t> writes{0};
    std::atomic<uint64_t> total_access_time_ns{0};
    
    // For detecting sequential access patterns
    std::atomic<PageID> last_accessed_page{Page::INVALID_PAGE_ID};
    std::atomic<int> sequential_count{0};
    std::atomic<AccessType> detected_pattern{AccessType::UNKNOWN};
    
    void record_access(PageID page_id, bool is_hit, bool is_write, uint64_t time_ns) {
        if (is_hit) hits++; else misses++;
        if (is_write) writes++; else reads++;
        total_access_time_ns += time_ns;
        
        // Simple access pattern detection
        PageID last = last_accessed_page.load();
        if (last != Page::INVALID_PAGE_ID) {
            if (page_id == last + 1) {
                int seq_count = sequential_count.fetch_add(1) + 1;
                if (seq_count > 5) { // After 5 sequential accesses, mark as SEQUENTIAL
                    detected_pattern = AccessType::SEQUENTIAL;
                }
            } else if (page_id != last) {
                sequential_count = 0;
                // If we've seen enough non-sequential accesses, mark as RANDOM
                if (detected_pattern != AccessType::SEQUENTIAL && hits + misses > 10) {
                    detected_pattern = AccessType::RANDOM;
                }
            }
        }
        
        last_accessed_page = page_id;
    }
    
    double hit_rate() const {
        uint64_t total = hits + misses;
        return total > 0 ? (double)hits / total : 0.0;
    }
    
    double avg_access_time_ns() const {
        uint64_t total = hits + misses;
        return total > 0 ? (double)total_access_time_ns / total : 0.0;
    }
    
    AccessType access_pattern() const {
        return detected_pattern;
    }
};

class CacheProfiler {
public:
    static CacheProfiler& instance() {
        static CacheProfiler inst;
        return inst;
    }
    
    void record_access(const std::string& section_name, PageID page_id, bool is_hit, bool is_write) {
        auto start = std::chrono::high_resolution_clock::now();
        
        {
            std::lock_guard<std::mutex> lock(mutex);
            auto& stats = section_stats[section_name];
            stats.record_access(page_id, is_hit, is_write, 0); // We'll update time later
            
            // Also track per-page statistics
            page_access_counts[page_id]++;
            
            // Track page access sequence for pattern analysis
            if (access_sequence.size() < max_sequence_length) {
                access_sequence.push_back(page_id);
            }
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        
        // Update access time
        section_stats[section_name].total_access_time_ns += duration;
    }
    
    void print_stats() const {
        std::cout << "=== Cache Profiler Statistics ===" << std::endl;
        for (const auto& [section, stats] : section_stats) {
            PageAccessStatsSnapshot snapshot;
            snapshot.hits = stats.hits.load();
            snapshot.misses = stats.misses.load();
            snapshot.reads = stats.reads.load();
            snapshot.writes = stats.writes.load();
            snapshot.total_access_time_ns = stats.total_access_time_ns.load();
            snapshot.detected_pattern = stats.detected_pattern.load();
            
            std::cout << "Section: " << section << std::endl;
            std::cout << "  Hits: " << snapshot.hits << std::endl;
            std::cout << "  Misses: " << snapshot.misses << std::endl;
            std::cout << "  Hit Rate: " << (snapshot.hit_rate() * 100) << "%" << std::endl;
            std::cout << "  Reads: " << snapshot.reads << std::endl;
            std::cout << "  Writes: " << snapshot.writes << std::endl;
            std::cout << "  Avg Access Time: " << snapshot.avg_access_time_ns() << " ns" << std::endl;
            std::cout << "  Access Pattern: " << 
                (snapshot.detected_pattern == AccessType::SEQUENTIAL ? "Sequential" : 
                 snapshot.detected_pattern == AccessType::RANDOM ? "Random" : "Unknown") << std::endl;
        }
    }
    
    void reset() {
        std::lock_guard<std::mutex> lock(mutex);
        section_stats.clear();
        page_access_counts.clear();
        access_sequence.clear();
    }
    
    // Get statistics for a specific section
    PageAccessStatsSnapshot get_section_stats(const std::string& section_name) const {
        std::lock_guard<std::mutex> lock(mutex);
        auto it = section_stats.find(section_name);
        if (it != section_stats.end()) {
            PageAccessStatsSnapshot snapshot;
            snapshot.hits = it->second.hits.load();
            snapshot.misses = it->second.misses.load();
            snapshot.reads = it->second.reads.load();
            snapshot.writes = it->second.writes.load();
            snapshot.total_access_time_ns = it->second.total_access_time_ns.load();
            snapshot.last_accessed_page = it->second.last_accessed_page.load();
            snapshot.sequential_count = it->second.sequential_count.load();
            snapshot.detected_pattern = it->second.detected_pattern.load();
            return snapshot;
        }
        return PageAccessStatsSnapshot();
    }
    
    // Get access pattern for a specific section
    AccessType get_access_pattern(const std::string& section_name) const {
        std::lock_guard<std::mutex> lock(mutex);
        auto it = section_stats.find(section_name);
        if (it != section_stats.end()) {
            return it->second.detected_pattern.load();
        }
        return AccessType::UNKNOWN;
    }
    
private:
    CacheProfiler() = default;
    ~CacheProfiler() = default;
    
    mutable std::mutex mutex;
    std::unordered_map<std::string, PageAccessStats> section_stats;
    std::unordered_map<PageID, uint64_t> page_access_counts;
    std::vector<PageID> access_sequence;
    const size_t max_sequence_length = 10000; // Maximum number of accesses to track
};

} // namespace bptree

#endif // _BPTREE_CACHE_PROFILER_H_