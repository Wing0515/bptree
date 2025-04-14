#include "bptree/simplified_configurable_cache.h"
#include "bptree/mem_page_cache.h"
#include "bptree/tree.h"
#include "bptree/latency_simulator.h"

#include <iostream>
#include <iomanip>
#include <fstream>
#include <chrono>
#include <random>
#include <vector>
#include <string>
#include <memory>
#include <algorithm>
#include <functional>

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
    size_t num_keys;
    size_t num_queries;
    size_t local_memory_size;
    int network_latency_us;
    bool use_configurable_cache;
    bptree::SimplifiedConfigurableCache::Structure structure;
    size_t line_size;
    size_t associativity;
};

// Benchmark result
struct BenchmarkResult {
    std::string name;
    double insert_time_ms;
    double point_query_time_ms;
    double range_query_time_ms;
    double random_query_time_ms;
    double miss_rate;
};

// Generate random keys
std::vector<uint64_t> generate_random_keys(size_t count, uint64_t max_key) {
    std::vector<uint64_t> keys;
    std::mt19937 gen(42); // Fixed seed for reproducibility
    std::uniform_int_distribution<uint64_t> dist(0, max_key - 1);
    
    for (size_t i = 0; i < count; ++i) {
        keys.push_back(dist(gen));
    }
    
    return keys;
}

// Run a benchmark with a specific configuration
BenchmarkResult run_benchmark(const BenchmarkConfig& config) {
    std::cout << "Running benchmark: " << config.name << std::endl;
    
    // Setup result structure
    BenchmarkResult result;
    result.name = config.name;
    
    // Configure latency simulation
    bptree::LatencySimulator::configure(config.network_latency_us);
    
    try {
        // Create page cache based on configuration
        std::shared_ptr<bptree::AbstractPageCache> cache;
        
        if (config.use_configurable_cache) {
            auto configurable_cache = std::make_shared<bptree::SimplifiedConfigurableCache>(
                config.local_memory_size,
                4096, // Page size
                config.line_size,
                config.structure,
                config.associativity
            );
            
            cache = configurable_cache;
        } else {
            // Use memory page cache as baseline
            cache = std::make_shared<bptree::MemPageCache>(4096);
        }
        
        // Create B+Tree
        auto tree = std::make_unique<bptree::BTree<256, uint64_t, uint64_t>>(cache.get());
        
        // Insert keys
        result.insert_time_ms = measure_time_ms([&]() {
            for (size_t i = 0; i < config.num_keys; ++i) {
                tree->insert(i, i + 1);
            }
        });
        
        std::cout << "  Inserted " << config.num_keys << " keys in " 
                  << result.insert_time_ms << " ms" << std::endl;
        
        // Reset cache statistics
        if (config.use_configurable_cache) {
            static_cast<bptree::SimplifiedConfigurableCache*>(cache.get())->reset_stats();
        }
        
        // Generate random keys for queries
        auto random_keys = generate_random_keys(config.num_queries, config.num_keys);
        
        // Point queries (sequential)
        result.point_query_time_ms = measure_time_ms([&]() {
            std::vector<uint64_t> values;
            for (size_t i = 0; i < config.num_queries; ++i) {
                values.clear();
                tree->get_value(i % config.num_keys, values);
            }
        });
        
        std::cout << "  Sequential queries: " << config.num_queries << " in " 
                  << result.point_query_time_ms << " ms" << std::endl;
        
        // Random queries
        result.random_query_time_ms = measure_time_ms([&]() {
            std::vector<uint64_t> values;
            for (const auto& key : random_keys) {
                values.clear();
                tree->get_value(key, values);
            }
        });
        
        std::cout << "  Random queries: " << config.num_queries << " in " 
                  << result.random_query_time_ms << " ms" << std::endl;
        
        // Range queries
        const size_t NUM_RANGE_QUERIES = 100;
        const size_t RANGE_SIZE = 100;
        
        result.range_query_time_ms = measure_time_ms([&]() {
            for (size_t i = 0; i < NUM_RANGE_QUERIES; ++i) {
                uint64_t start_key = random_keys[i % random_keys.size()];
                size_t count = 0;
                
                for (auto it = tree->begin(start_key); it != tree->end() && count < RANGE_SIZE; ++it) {
                    count++;
                }
            }
        });
        
        std::cout << "  Range queries: " << NUM_RANGE_QUERIES << " in " 
                  << result.range_query_time_ms << " ms" << std::endl;
        
        // Get miss rate
        if (config.use_configurable_cache) {
            auto stats = static_cast<bptree::SimplifiedConfigurableCache*>(cache.get())->get_stats();
            result.miss_rate = stats.miss_rate();
            std::cout << "  Cache accesses: " << stats.accesses 
                    << ", misses: " << stats.misses 
                    << ", miss rate: " << (result.miss_rate * 100.0) << "%" << std::endl;
        } else {
            result.miss_rate = 0.0; // Not available for standard cache
        }
        
        // Make sure tree is destroyed before cache
        tree.reset();
        
    } catch (const std::exception& e) {
        std::cerr << "  Error in benchmark: " << e.what() << std::endl;
    }
    
    return result;
}

// Save benchmark results to CSV
void save_results_to_csv(const std::vector<BenchmarkResult>& results, const std::string& filename) {
    std::ofstream file(filename);
    
    file << "Name,Insert Time (ms),Point Query Time (ms),Range Query Time (ms),Random Query Time (ms),Miss Rate\n";
    
    for (const auto& result : results) {
        file << result.name << ","
             << result.insert_time_ms << ","
             << result.point_query_time_ms << ","
             << result.range_query_time_ms << ","
             << result.random_query_time_ms << ","
             << result.miss_rate << "\n";
    }
    
    file.close();
    
    std::cout << "Results saved to " << filename << std::endl;
}

// Print benchmark results
void print_results(const std::vector<BenchmarkResult>& results) {
    std::cout << "\nBenchmark Results:\n";
    std::cout << "=================\n\n";
    
    std::cout << std::left 
              << std::setw(35) << "Configuration" 
              << " | " << std::setw(15) << "Insert (ms)"
              << " | " << std::setw(15) << "Point Query (ms)"
              << " | " << std::setw(15) << "Range Query (ms)"
              << " | " << std::setw(15) << "Random Query (ms)"
              << " | " << std::setw(10) << "Miss Rate"
              << std::endl;
    
    std::cout << std::string(120, '-') << std::endl;
    
    for (const auto& result : results) {
        std::cout << std::left 
                  << std::setw(35) << result.name 
                  << " | " << std::setw(15) << result.insert_time_ms
                  << " | " << std::setw(15) << result.point_query_time_ms
                  << " | " << std::setw(15) << result.range_query_time_ms
                  << " | " << std::setw(15) << result.random_query_time_ms
                  << " | " << std::setw(10) << (result.miss_rate * 100.0) << "%"
                  << std::endl;
    }
}

// Compare results and print analysis
void analyze_results(const std::vector<BenchmarkResult>& results) {
    if (results.size() < 2) {
        std::cout << "Not enough results for analysis.\n";
        return;
    }
    
    std::cout << "\nPerformance Analysis:\n";
    std::cout << "====================\n\n";
    
    // Find baseline result (standard cache)
    auto baseline_it = std::find_if(results.begin(), results.end(), 
        [](const auto& result) { return result.name.find("Standard") != std::string::npos; });
    
    if (baseline_it == results.end()) {
        baseline_it = results.begin();  // Use first result as baseline if standard not found
    }
    
    const auto& baseline = *baseline_it;
    
    // Calculate improvements for each configuration
    for (const auto& result : results) {
        if (&result == &baseline) continue;
        
        double insert_improvement = (baseline.insert_time_ms - result.insert_time_ms) / baseline.insert_time_ms * 100.0;
        double point_query_improvement = (baseline.point_query_time_ms - result.point_query_time_ms) / baseline.point_query_time_ms * 100.0;
        double range_query_improvement = (baseline.range_query_time_ms - result.range_query_time_ms) / baseline.range_query_time_ms * 100.0;
        double random_query_improvement = (baseline.random_query_time_ms - result.random_query_time_ms) / baseline.random_query_time_ms * 100.0;
        
        std::cout << "Improvements with " << result.name << " compared to " << baseline.name << ":\n";
        std::cout << "  Insert Time: " << (insert_improvement > 0 ? "+" : "") << insert_improvement << "%\n";
        std::cout << "  Point Query Time: " << (point_query_improvement > 0 ? "+" : "") << point_query_improvement << "%\n";
        std::cout << "  Range Query Time: " << (range_query_improvement > 0 ? "+" : "") << range_query_improvement << "%\n";
        std::cout << "  Random Query Time: " << (random_query_improvement > 0 ? "+" : "") << random_query_improvement << "%\n";
        std::cout << std::endl;
    }
}

int main() {
    // Define benchmark configurations
    std::vector<BenchmarkConfig> configs = {
        // Standard cache (baseline)
        {
            "Standard Cache",
            1000000,   // 100K keys
            500000,    // 10K queries
            64 * 1024,  // 1MB
            0,        // No latency
            false,    // Use standard cache
            bptree::SimplifiedConfigurableCache::Structure::FullyAssociative,
            4096,     // Default line size
            8         // Default associativity
        },
        
        // With simulated far memory latency
        {
            "Standard Cache with Latency",
            1000000,
            500000,
            64 * 1024,
            1000,     // 1ms latency
            false,
            bptree::SimplifiedConfigurableCache::Structure::FullyAssociative,
            4096,
            8
        },
        
        // Configurable cache with direct mapping (better for sequential access)
        {
            "Direct-Mapped Cache",
            100000,
            500000,
            64 * 1024,
            1000,
            true,
            bptree::SimplifiedConfigurableCache::Structure::DirectMapped,
            4096,
            1
        },
        
        // Configurable cache with set associative mapping (balance)
        {
            "Set-Associative Cache",
            1000000,
            500000,
            64 * 1024,
            1000,
            true,
            bptree::SimplifiedConfigurableCache::Structure::SetAssociative,
            4096,
            4
        },
        
        // Configurable cache with fully associative mapping (flexible but overhead)
        {
            "Fully-Associative Cache",
            1000000,
            500000,
            64 * 1024,
            1000,
            true,
            bptree::SimplifiedConfigurableCache::Structure::FullyAssociative,
            4096,
            8
        },
        
        // Small line size (better for random access)
        {
            "Small Line Size Cache",
            1000000,
            500000,
            64 * 1024,
            1000,
            true,
            bptree::SimplifiedConfigurableCache::Structure::FullyAssociative,
            4096,     // 4KB line size
            8
        },
        
        // Large line size (better for sequential access)
        {
            "Large Line Size Cache",
            1000000,
            500000,
            64 * 1024,
            1000,
            true,
            bptree::SimplifiedConfigurableCache::Structure::FullyAssociative,
            16384,    // 16KB line size
            8
        },
        
        // Optimized for sequential access
        {
            "Optimized for Sequential Access",
            1000000,
            500000,
            64 * 1024,
            1000,
            true,
            bptree::SimplifiedConfigurableCache::Structure::DirectMapped,
            16384,    // Large line size
            1
        },
        
        // Optimized for random access
        {
            "Optimized for Random Access",
            1000000,
            500000,
            64 * 1024,
            1000,
            true,
            bptree::SimplifiedConfigurableCache::Structure::FullyAssociative,
            4096,     // Small line size
            16        // High associativity
        }
    };
    
    // Run benchmarks
    std::vector<BenchmarkResult> results;
    
    for (const auto& config : configs) {
        results.push_back(run_benchmark(config));
        std::cout << "-----------------------------------------\n";
    }
    
    // Print results
    print_results(results);
    
    // Analyze results
    analyze_results(results);
    
    // Save results to CSV
    save_results_to_csv(results, "simplified_cache_benchmark_results.csv");
    
    return 0;
}