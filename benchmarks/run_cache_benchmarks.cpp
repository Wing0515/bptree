#include "bptree/configurable_cache.h"
#include "bptree/mem_page_cache.h"
#include "bptree/heap_page_cache.h"
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

// Debug flag
#define DEBUG_LOG 1

#if DEBUG_LOG
#define DEBUG_PRINT(x) std::cout << "[DEBUG] " << x << std::endl
#else
#define DEBUG_PRINT(x)
#endif

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
    bool optimize_cache_sections;
};

// Benchmark result
struct BenchmarkResult {
    std::string name;
    double insert_time_ms;
    double point_query_time_ms;
    double range_query_time_ms;
    double miss_rate;
};

// Simplified benchmark that tests only a few configurations
void run_simplified_benchmark() {
    DEBUG_PRINT("Starting simplified benchmark");
    
    // Configure latency simulation for test
    bptree::LatencySimulator::configure(100);
    
    // Create a memory page cache as baseline
    DEBUG_PRINT("Creating MemPageCache");
    auto mem_cache = std::make_shared<bptree::MemPageCache>(4096);
    
    DEBUG_PRINT("Creating B+Tree with MemPageCache");
    std::unique_ptr<bptree::BTree<256, uint64_t, uint64_t>> tree1;
    
    try {
        tree1 = std::make_unique<bptree::BTree<256, uint64_t, uint64_t>>(mem_cache.get());
        DEBUG_PRINT("B+Tree created successfully");
    } catch (const std::exception& e) {
        DEBUG_PRINT("Exception creating B+Tree: " << e.what());
        return;
    }
    
    // Insert some keys
    DEBUG_PRINT("Inserting keys into tree");
    for (uint64_t i = 0; i < 1000; ++i) {
        tree1->insert(i, i + 1);
    }
    
    // Test some queries
    DEBUG_PRINT("Querying tree");
    std::vector<uint64_t> values;
    for (uint64_t i = 0; i < 100; ++i) {
        values.clear();
        tree1->get_value(i, values);
    }
    
    // Clean up 
    DEBUG_PRINT("Destroying tree1");
    tree1.reset();
    
    DEBUG_PRINT("Testing with ConfigurableCache");
    
    // Create a configurable cache
    DEBUG_PRINT("Creating ConfigurableCache");
    auto config_cache = std::make_shared<bptree::ConfigurableCache>(10 * 1024 * 1024, 4096);
    
    // Create sections
    DEBUG_PRINT("Creating cache sections");
    size_t section1 = config_cache->create_section(
        5 * 1024 * 1024,
        4096,
        bptree::CacheSection::Structure::FullyAssociative
    );
    
    // Map ranges
    DEBUG_PRINT("Mapping page ranges");
    config_cache->map_page_range_to_section(1, 1000, section1);
    
    // Create tree
    DEBUG_PRINT("Creating B+Tree with ConfigurableCache");
    std::unique_ptr<bptree::BTree<256, uint64_t, uint64_t>> tree2;
    
    try {
        tree2 = std::make_unique<bptree::BTree<256, uint64_t, uint64_t>>(config_cache.get());
        DEBUG_PRINT("B+Tree created successfully");
    } catch (const std::exception& e) {
        DEBUG_PRINT("Exception creating B+Tree: " << e.what());
        return;
    }
    
    // Insert some keys
    DEBUG_PRINT("Inserting keys into tree");
    for (uint64_t i = 0; i < 1000; ++i) {
        tree2->insert(i, i + 1);
    }
    
    // Test some queries
    DEBUG_PRINT("Querying tree");
    for (uint64_t i = 0; i < 100; ++i) {
        values.clear();
        tree2->get_value(i, values);
    }
    
    // Clean up - destroy tree before cache
    DEBUG_PRINT("Destroying tree2");
    tree2.reset();
    
    DEBUG_PRINT("Cleanup shared_ptrs");
    config_cache.reset();
    mem_cache.reset();
    
    DEBUG_PRINT("Simplified benchmark completed successfully");
}

// Run a benchmark with a specific configuration
BenchmarkResult run_benchmark(const BenchmarkConfig& config) {
    DEBUG_PRINT("Starting benchmark: " << config.name);
    
    // Setup result structure
    BenchmarkResult result;
    result.name = config.name;
    
    // Configure latency simulation
    bptree::LatencySimulator::configure(config.network_latency_us);
    
    // Create page cache based on configuration
    std::shared_ptr<bptree::AbstractPageCache> cache;
    std::unique_ptr<bptree::BTree<256, uint64_t, uint64_t>> tree;
    
    try {
        if (config.use_configurable_cache) {
            DEBUG_PRINT("Creating ConfigurableCache");
            auto configurable_cache = std::make_shared<bptree::ConfigurableCache>(
                config.local_memory_size, 
                4096  // Page size
            );
            
            if (config.optimize_cache_sections) {
                DEBUG_PRINT("Creating optimized sections");
                
                // Inner nodes section (random access pattern)
                size_t inner_section = configurable_cache->create_section(
                    config.local_memory_size / 3,
                    4096,  // Standard page size
                    bptree::CacheSection::Structure::FullyAssociative
                );
                
                // Leaf nodes section (sequential access pattern for range queries)
                size_t leaf_section = configurable_cache->create_section(
                    config.local_memory_size / 3,
                    4096 * 4,  // Larger line size
                    bptree::CacheSection::Structure::SetAssociative,
                    8  // 8-way associative
                );
                
                // Random access section
                size_t random_section = configurable_cache->create_section(
                    config.local_memory_size / 3,
                    4096,  // Standard page size
                    bptree::CacheSection::Structure::FullyAssociative
                );
                
                // Map page ranges to sections (estimated ranges)
                configurable_cache->map_page_range_to_section(1, 1000, inner_section);
                configurable_cache->map_page_range_to_section(1001, 100000, leaf_section);
                configurable_cache->map_page_range_to_section(100001, UINT32_MAX, random_section);
            }
            
            cache = configurable_cache;
        } else {
            DEBUG_PRINT("Creating MemPageCache");
            cache = std::make_shared<bptree::MemPageCache>(4096);
        }
        
        // Create B+Tree
        DEBUG_PRINT("Creating B+Tree");
        tree = std::make_unique<bptree::BTree<256, uint64_t, uint64_t>>(cache.get());
        DEBUG_PRINT("B+Tree created successfully");
        
        // Generate random keys for queries
        std::mt19937 gen(42);  // Fixed seed for reproducibility
        std::uniform_int_distribution<uint64_t> dist(0, config.num_keys - 1);
        
        std::vector<uint64_t> random_keys;
        for (size_t i = 0; i < config.num_queries; ++i) {
            random_keys.push_back(dist(gen));
        }
        
        // Insert keys
        DEBUG_PRINT("Inserting keys");
        result.insert_time_ms = measure_time_ms([&]() {
            for (size_t i = 0; i < config.num_keys; ++i) {
                tree->insert(i, i + 1);
            }
        });
        
        // Reset cache statistics if available
        if (config.use_configurable_cache) {
            static_cast<bptree::ConfigurableCache*>(cache.get())->reset_all_stats();
        }
        
        // Point queries
        DEBUG_PRINT("Running point queries");
        result.point_query_time_ms = measure_time_ms([&]() {
            std::vector<uint64_t> values;
            for (const auto& key : random_keys) {
                values.clear();
                tree->get_value(key, values);
            }
        });
        
        // Range queries
        DEBUG_PRINT("Running range queries");
        result.range_query_time_ms = measure_time_ms([&]() {
            const size_t NUM_RANGE_QUERIES = 100;
            const size_t RANGE_SIZE = 100;
            
            for (size_t i = 0; i < NUM_RANGE_QUERIES; ++i) {
                uint64_t start_key = dist(gen);
                size_t count = 0;
                
                for (auto it = tree->begin(start_key); it != tree->end() && count < RANGE_SIZE; ++it) {
                    count++;
                }
            }
        });
        
        // Get miss rate if configurable cache
        if (config.use_configurable_cache) {
            auto stats = static_cast<bptree::ConfigurableCache*>(cache.get())->get_all_section_stats();
            
            size_t total_misses = 0;
            size_t total_accesses = 0;
            
            for (const auto& stat : stats) {
                total_misses += stat.misses;
                total_accesses += stat.accesses;
            }
            
            result.miss_rate = total_accesses > 0 ? 
                static_cast<double>(total_misses) / total_accesses : 0.0;
        } else {
            result.miss_rate = 0.0;  // Not available for standard cache
        }
        
        // Make sure tree is destroyed before cache
        DEBUG_PRINT("Destroying tree before returning");
        tree.reset();
        
    } catch (const std::exception& e) {
        DEBUG_PRINT("Exception in benchmark: " << e.what());
        
        // Cleanup in case of exception
        if (tree) {
            DEBUG_PRINT("Cleaning up tree after exception");
            tree.reset();
        }
        
        // Return empty result with just the name
        return result;
    }
    
    DEBUG_PRINT("Benchmark completed successfully");
    return result;
}

// Save benchmark results to CSV
void save_results_to_csv(const std::vector<BenchmarkResult>& results, const std::string& filename) {
    std::ofstream file(filename);
    
    file << "Name,Insert Time (ms),Point Query Time (ms),Range Query Time (ms),Miss Rate\n";
    
    for (const auto& result : results) {
        file << result.name << ","
             << result.insert_time_ms << ","
             << result.point_query_time_ms << ","
             << result.range_query_time_ms << ","
             << result.miss_rate << "\n";
    }
    
    file.close();
}

// Print benchmark results
void print_results(const std::vector<BenchmarkResult>& results) {
    std::cout << "\nBenchmark Results:\n";
    std::cout << "=================\n\n";
    
    std::cout << std::left 
              << std::setw(30) << "Configuration" 
              << " | " << std::setw(15) << "Insert (ms)"
              << " | " << std::setw(15) << "Point Query (ms)"
              << " | " << std::setw(15) << "Range Query (ms)"
              << " | " << std::setw(10) << "Miss Rate"
              << std::endl;
    
    std::cout << std::string(100, '-') << std::endl;
    
    for (const auto& result : results) {
        std::cout << std::left 
                  << std::setw(30) << result.name 
                  << " | " << std::setw(15) << result.insert_time_ms
                  << " | " << std::setw(15) << result.point_query_time_ms
                  << " | " << std::setw(15) << result.range_query_time_ms
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
        
        std::cout << "Improvements with " << result.name << " compared to " << baseline.name << ":\n";
        std::cout << "  Insert Time: " << (insert_improvement > 0 ? "+" : "") << insert_improvement << "%\n";
        std::cout << "  Point Query Time: " << (point_query_improvement > 0 ? "+" : "") << point_query_improvement << "%\n";
        std::cout << "  Range Query Time: " << (range_query_improvement > 0 ? "+" : "") << range_query_improvement << "%\n";
        std::cout << std::endl;
    }
}

int main(int argc, char* argv[]) {
    // First, run a simplified test to check for memory issues
    DEBUG_PRINT("Running simplified test for debugging");
    run_simplified_benchmark();
    
    // Reduced benchmark configurations for faster testing
    BenchmarkConfig config1 = {
        "Standard Cache - Test",
        10000,   // Reduced number of keys
        1000,    // Reduced number of queries
        1024 * 1024, // 1MB cache
        100,     // 0.1ms latency
        false,   // Standard cache
        false    // No optimization
    };
    
    BenchmarkConfig config2 = {
        "Configurable Cache - Test",
        10000,   // Same as above
        1000,
        1024 * 1024,
        100,
        true,    // Use configurable cache
        true     // With optimization
    };
    
    std::cout << "Running test benchmarks...\n";
    
    // Run test benchmarks
    auto result1 = run_benchmark(config1);
    auto result2 = run_benchmark(config2);
    
    // Print results
    std::vector<BenchmarkResult> results = {result1, result2};
    print_results(results);
    
    std::cout << "\nTest benchmarks completed successfully.\n";
    std::cout << "If you want to run the full benchmark suite, use the --full flag.\n";
    
    // Only run full benchmarks if explicitly requested
    if (argc > 1 && std::string(argv[1]) == "--full") {
        std::cout << "\nRunning full benchmark suite...\n";
        
        // Define benchmark configurations
        std::vector<BenchmarkConfig> configs;
        
        // Memory size configurations
        const std::vector<size_t> memory_sizes = {
            10 * 1024 * 1024,   // 10 MB
            50 * 1024 * 1024,   // 50 MB
        };
        
        // Network latency configurations
        const std::vector<int> latencies = {
            100,    // 0.1 ms
            500,    // 0.5 ms
        };
        
        // Different workload sizes
        const std::vector<size_t> workload_sizes = {
            50000,     // Small
            100000     // Medium
        };
        
        // Generate configurations
        for (size_t num_keys : workload_sizes) {
            size_t num_queries = num_keys / 10;  // 10% read ratio
            
            for (size_t memory_size : memory_sizes) {
                for (int latency : latencies) {
                    // Standard cache
                    configs.push_back({
                        "Standard Cache - " + std::to_string(memory_size / (1024*1024)) + "MB - " + 
                        std::to_string(latency) + "us",
                        num_keys,
                        num_queries,
                        memory_size,
                        latency,
                        false,
                        false
                    });
                    
                    // Basic configurable cache (single section)
                    configs.push_back({
                        "Basic Configurable - " + std::to_string(memory_size / (1024*1024)) + "MB - " + 
                        std::to_string(latency) + "us",
                        num_keys,
                        num_queries,
                        memory_size,
                        latency,
                        true,
                        false
                    });
                    
                    // Optimized configurable cache
                    configs.push_back({
                        "Optimized Configurable - " + std::to_string(memory_size / (1024*1024)) + "MB - " + 
                        std::to_string(latency) + "us",
                        num_keys,
                        num_queries,
                        memory_size,
                        latency,
                        true,
                        true
                    });
                }
            }
        }
        
        // Run benchmarks
        std::vector<BenchmarkResult> full_results;
        
        std::cout << "Running " << configs.size() << " benchmark configurations...\n";
        
        for (size_t i = 0; i < configs.size(); ++i) {
            const auto& config = configs[i];
            std::cout << "Running benchmark " << (i + 1) << "/" << configs.size() 
                    << ": " << config.name << "..." << std::endl;
            
            full_results.push_back(run_benchmark(config));
        }
        
        // Print and analyze results
        print_results(full_results);
        analyze_results(full_results);
        
        // Save results to CSV
        save_results_to_csv(full_results, "cache_benchmark_results.csv");
        std::cout << "Results saved to cache_benchmark_results.csv\n";
    }
    
    return 0;
}