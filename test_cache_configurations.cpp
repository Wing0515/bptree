// test_cache_configurations.cpp
#include "bptree/simplified_configurable_cache.h"
#include "bptree/tree.h"

#include <iostream>
#include <chrono>
#include <random>
#include <vector>
#include <iomanip>
#include <string>
#include <fstream>

using namespace std::chrono;

// Helper for measuring execution time
template<typename Func>
double measure_time_ms(Func&& func) {
    auto start = high_resolution_clock::now();
    func();
    auto end = high_resolution_clock::now();
    return duration<double, std::milli>(end - start).count();
}

// Benchmark configuration
struct BenchmarkConfig {
    std::string name;
    size_t cache_size;
    size_t num_keys;
    size_t num_queries;
    int network_latency_us;
    bptree::SimplifiedConfigurableCache::Structure structure;
    size_t line_size;
    size_t associativity;
};

// Benchmark result
struct BenchmarkResult {
    std::string name;
    double insert_time_ms;
    double seq_query_time_ms;
    double random_query_time_ms;
    double range_query_time_ms;
    double miss_rate;
};

// Run a single benchmark
BenchmarkResult run_benchmark(const BenchmarkConfig& config) {
    std::cout << "Running benchmark: " << config.name << std::endl;
    
    // Setup result
    BenchmarkResult result;
    result.name = config.name;
    
    // Configure network latency
    bptree::LatencySimulator::configure(config.network_latency_us);
    
    // Create configurable cache
    bptree::SimplifiedConfigurableCache cache(
        config.cache_size,
        4096, // page size
        config.line_size,
        config.structure,
        config.associativity,
        false // debug mode off
    );
    
    // Create B+Tree
    bptree::BTree<256, uint64_t, uint64_t> tree(&cache);
    
    // Insert keys
    result.insert_time_ms = measure_time_ms([&]() {
        for (uint64_t i = 0; i < config.num_keys; ++i) {
            tree.insert(i, i + 1);
        }
    });
    
    std::cout << "  Inserted " << config.num_keys << " keys in " 
              << result.insert_time_ms << " ms" << std::endl;
    
    // Reset statistics before queries
    cache.reset_stats();
    
    // Sequential query
    result.seq_query_time_ms = measure_time_ms([&]() {
        std::vector<uint64_t> values;
        for (uint64_t i = 0; i < std::min(config.num_queries, config.num_keys); ++i) {
            values.clear();
            tree.get_value(i, values);
        }
    });
    
    std::cout << "  Sequential query: " << result.seq_query_time_ms << " ms" << std::endl;
    
    // Random query
    {
        // Generate random keys
        std::vector<uint64_t> random_keys;
        std::mt19937 gen(42); // Fixed seed for reproducibility
        std::uniform_int_distribution<uint64_t> dist(0, config.num_keys - 1);
        
        for (size_t i = 0; i < config.num_queries; ++i) {
            random_keys.push_back(dist(gen));
        }
        
        // Reset statistics
        cache.reset_stats();
        
        // Perform random queries
        result.random_query_time_ms = measure_time_ms([&]() {
            std::vector<uint64_t> values;
            for (const auto& key : random_keys) {
                values.clear();
                tree.get_value(key, values);
            }
        });
        
        // Get miss rate from statistics
        auto stats = cache.get_stats();
        result.miss_rate = stats.miss_rate();
        
        std::cout << "  Random query: " << result.random_query_time_ms << " ms" << std::endl;
        std::cout << "  Cache accesses: " << stats.accesses 
                << ", misses: " << stats.misses 
                << ", miss rate: " << (result.miss_rate * 100.0) << "%" << std::endl;
    }
    
    // Range query
    result.range_query_time_ms = measure_time_ms([&]() {
        const size_t NUM_RANGE_QUERIES = 50;
        const size_t RANGE_SIZE = 100;
        
        for (size_t i = 0; i < NUM_RANGE_QUERIES; ++i) {
            size_t start = (i * config.num_keys / NUM_RANGE_QUERIES);
            
            size_t count = 0;
            for (auto it = tree.begin(start); it != tree.end() && count < RANGE_SIZE; ++it) {
                count++;
            }
        }
    });
    
    std::cout << "  Range query: " << result.range_query_time_ms << " ms" << std::endl;
    
    return result;
}

// Save results to CSV
void save_results_to_csv(const std::vector<BenchmarkResult>& results, const std::string& filename) {
    std::ofstream file(filename);
    
    // Write header
    file << "Name,Insert Time (ms),Sequential Query (ms),Random Query (ms),Range Query (ms),Miss Rate\n";
    
    // Write data
    for (const auto& result : results) {
        file << result.name << ","
             << result.insert_time_ms << ","
             << result.seq_query_time_ms << ","
             << result.random_query_time_ms << ","
             << result.range_query_time_ms << ","
             << result.miss_rate << "\n";
    }
    
    file.close();
    std::cout << "Results saved to " << filename << std::endl;
}

// Print results table
void print_results(const std::vector<BenchmarkResult>& results) {
    std::cout << "\nResults Summary:\n";
    std::cout << std::left << std::setw(35) << "Configuration"
              << " | " << std::setw(15) << "Insert (ms)"
              << " | " << std::setw(15) << "Seq Query (ms)"
              << " | " << std::setw(15) << "Random Query (ms)"
              << " | " << std::setw(15) << "Range Query (ms)"
              << " | " << std::setw(10) << "Miss Rate"
              << std::endl;
    
    std::cout << std::string(115, '-') << std::endl;
    
    for (const auto& result : results) {
        std::cout << std::left << std::setw(35) << result.name
                  << " | " << std::setw(15) << result.insert_time_ms
                  << " | " << std::setw(15) << result.seq_query_time_ms
                  << " | " << std::setw(15) << result.random_query_time_ms
                  << " | " << std::setw(15) << result.range_query_time_ms
                  << " | " << std::setw(10) << (result.miss_rate * 100.0) << "%"
                  << std::endl;
    }
}

int main() {
    // Test to ensure miss rates are properly calculated
    // Using small cache sizes to force misses
    
    std::cout << "Testing cache configurations based on Mira paper principles\n";
    std::cout << "=========================================================\n\n";
    
    // First, test with tiny cache to confirm miss rate tracking works properly
    {
        std::cout << "Running miss rate verification test...\n";
        
        // Create tiny cache (4 pages)
        bptree::SimplifiedConfigurableCache tiny_cache(4 * 4096, 4096, 4096, 
            bptree::SimplifiedConfigurableCache::Structure::FullyAssociative, 8, true);
        
        // Create tree
        bptree::BTree<256, uint64_t, uint64_t> tree(&tiny_cache);
        
        // Insert 1000 keys (far more than can fit in the tiny cache)
        for (uint64_t i = 0; i < 1000; ++i) {
            tree.insert(i, i + 1);
        }
        
        // Reset stats
        tiny_cache.reset_stats();
        
        // Access keys in random order
        std::mt19937 gen(42);
        std::uniform_int_distribution<uint64_t> dist(0, 999);
        std::vector<uint64_t> values;
        
        for (int i = 0; i < 100; ++i) {
            uint64_t key = dist(gen);
            values.clear();
            tree.get_value(key, values);
        }
        
        // Check stats
        auto stats = tiny_cache.get_stats();
        std::cout << "Miss rate verification results:\n";
        stats.print();
        
        if (stats.miss_rate() > 0.0) {
            std::cout << "PASS: Miss rate is non-zero as expected\n\n";
        } else {
            std::cout << "WARNING: Miss rate is zero, which is unexpected\n\n";
        }
    }
    
    // Now run the actual cache configuration benchmarks
    
    // Configure benchmarks with different cache configurations
    std::vector<BenchmarkConfig> configs = {
        // Baseline: Standard cache configuration
        {
            "Standard Cache",
            5 * 1024 * 1024, // 5MB cache size
            100000, // 100K keys
            10000,  // 10K queries
            1000,   // 1ms latency (simulate far memory)
            bptree::SimplifiedConfigurableCache::Structure::FullyAssociative,
            4096,   // 4KB line size
            8       // 8-way associative
        },
        
        // Direct-mapped cache (good for sequential access)
        {
            "Direct-Mapped Cache",
            5 * 1024 * 1024,
            100000,
            10000,
            1000,
            bptree::SimplifiedConfigurableCache::Structure::DirectMapped,
            4096,
            1
        },
        
        // Direct-mapped with large lines (better for sequential)
        {
            "Direct-Mapped Large Lines",
            5 * 1024 * 1024,
            100000,
            10000,
            1000,
            bptree::SimplifiedConfigurableCache::Structure::DirectMapped,
            16384, // 16KB lines
            1
        },
        
        // Set-associative (balanced)
        {
            "Set-Associative Cache",
            5 * 1024 * 1024,
            100000,
            10000,
            1000,
            bptree::SimplifiedConfigurableCache::Structure::SetAssociative,
            4096,
            4 // 4-way set associative
        },
        
        // Fully-associative (best for random access)
        {
            "Fully-Associative Cache",
            5 * 1024 * 1024,
            100000,
            10000,
            1000,
            bptree::SimplifiedConfigurableCache::Structure::FullyAssociative,
            4096,
            8
        },
        
        // Small cache to clearly show miss rate differences
        {
            "Small Fully-Associative Cache",
            1 * 1024 * 1024, // 1MB cache
            100000,
            10000,
            1000,
            bptree::SimplifiedConfigurableCache::Structure::FullyAssociative,
            4096,
            8
        },
        
        // Small direct-mapped (should have high miss rate)
        {
            "Small Direct-Mapped Cache",
            1 * 1024 * 1024,
            100000,
            10000,
            1000,
            bptree::SimplifiedConfigurableCache::Structure::DirectMapped,
            4096,
            1
        },
        
        // Tiny cache configuration
        {
            "Very Small Cache (10KB)",
            10 * 1024, // Only 10KB cache
            100000,
            10000,
            1000,
            bptree::SimplifiedConfigurableCache::Structure::FullyAssociative,
            4096,
            8
        },
    };
    
    // Run benchmarks
    std::vector<BenchmarkResult> results;
    
    for (const auto& config : configs) {
        results.push_back(run_benchmark(config));
        std::cout << "-------------------------------------------\n";
    }
    
    // Print and save results
    print_results(results);
    save_results_to_csv(results, "cache_configuration_results.csv");
    
    return 0;
}