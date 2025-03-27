#include <gtest/gtest.h>
#include <random>
#include <chrono>
#include <algorithm>
#include <fstream>
#include <iomanip>
#include <thread>
#include <vector>
#include <atomic>

#include "bptree/heap_page_cache.h"
#include "bptree/mem_page_cache.h"
#include "bptree/tree.h"
#include "bptree/latency_simulator.h"

using namespace std::chrono;
using KeyType = uint64_t;
using ValueType = uint64_t;

// Helper function to measure execution time
template<typename Func>
double measure_time_ms(Func&& func) {
    auto start = high_resolution_clock::now();
    func();
    auto end = high_resolution_clock::now();
    return duration<double, std::milli>(end - start).count();
}

// Test configuration
struct TestConfig {
    size_t num_keys;
    size_t num_queries;
    size_t num_threads;
    int network_latency_us;
    bool enable_prefetching;
    std::string description;
};

// Results structure
struct TestResults {
    double insert_time_ms;
    double point_query_time_ms;
    double range_query_time_ms;
    double random_query_time_ms;
    
    void print() const {
        std::cout << "Insert time: " << insert_time_ms << " ms\n";
        std::cout << "Point query time: " << point_query_time_ms << " ms\n";
        std::cout << "Range query time: " << range_query_time_ms << " ms\n";
        std::cout << "Random query time: " << random_query_time_ms << " ms\n";
    }
};

// Run test with given configuration
TestResults run_performance_test(const TestConfig& config) {
    TestResults results;
    
    // Setup latency simulation
    bptree::LatencySimulator::configure(
        config.enable_prefetching ? config.network_latency_us : 0
    );
    
    // Create a temporary file for the test
    char tmp_filename[L_tmpnam];
    std::tmpnam(tmp_filename);
    std::string filename = std::string(tmp_filename) + ".heap";
    
    // Create the page cache
    bptree::HeapPageCache page_cache(filename, true, 1000, 4096);
    
    // Create the B+Tree
    bptree::BTree<256, KeyType, ValueType> tree(&page_cache);
    
    std::cout << "Running test: " << config.description << std::endl;
    std::cout << "Number of keys: " << config.num_keys << std::endl;
    std::cout << "Number of queries: " << config.num_queries << std::endl;
    std::cout << "Number of threads: " << config.num_threads << std::endl;
    std::cout << "Network latency: " << config.network_latency_us << " μs" << std::endl;
    std::cout << "Prefetching: " << (config.enable_prefetching ? "Enabled" : "Disabled") << std::endl;
    
    // 1. Insert test
    {
        results.insert_time_ms = measure_time_ms([&]() {
            std::vector<std::thread> threads;
            size_t keys_per_thread = config.num_keys / config.num_threads;
            
            for (size_t t = 0; t < config.num_threads; t++) {
                threads.emplace_back([&tree, t, keys_per_thread]() {
                    for (size_t i = 0; i < keys_per_thread; i++) {
                        KeyType key = t * keys_per_thread + i;
                        tree.insert(key, key + 1);
                    }
                });
            }
            
            for (auto& thread : threads) {
                thread.join();
            }
        });
        
        std::cout << "Inserted " << config.num_keys << " keys in " 
                  << results.insert_time_ms << " ms" << std::endl;
    }
    
    // 2. Point query test (sequential keys)
    {
        results.point_query_time_ms = measure_time_ms([&]() {
            std::vector<std::thread> threads;
            size_t queries_per_thread = config.num_queries / config.num_threads;
            
            for (size_t t = 0; t < config.num_threads; t++) {
                threads.emplace_back([&tree, t, queries_per_thread]() {
                    for (size_t i = 0; i < queries_per_thread; i++) {
                        KeyType key = t * queries_per_thread + i;
                        std::vector<ValueType> values;
                        tree.get_value(key, values);
                    }
                });
            }
            
            for (auto& thread : threads) {
                thread.join();
            }
        });
        
        std::cout << "Point query (sequential): " << config.num_queries 
                  << " queries in " << results.point_query_time_ms << " ms" << std::endl;
    }
    
    // 3. Range query test
    {
        results.range_query_time_ms = measure_time_ms([&]() {
            std::vector<std::thread> threads;
            size_t ranges_per_thread = std::min<size_t>(100, config.num_queries / config.num_threads);
            size_t range_size = 100; // Number of keys in each range
            
            for (size_t t = 0; t < config.num_threads; t++) {
                threads.emplace_back([&tree, t, ranges_per_thread, range_size]() {
                    for (size_t i = 0; i < ranges_per_thread; i++) {
                        KeyType start_key = (t * ranges_per_thread + i) * range_size;
                        
                        // Perform range query
                        auto it = tree.begin(start_key);
                        size_t count = 0;
                        while (it != tree.end() && count < range_size) {
                            count++;
                            ++it;
                        }
                    }
                });
            }
            
            for (auto& thread : threads) {
                thread.join();
            }
        });
        
        std::cout << "Range query: " << config.num_threads * 100 
                  << " ranges in " << results.range_query_time_ms << " ms" << std::endl;
    }
    
    // 4. Random access query test
    {
        // Generate random keys first
        std::vector<KeyType> random_keys;
        std::mt19937 gen(42); // Fixed seed for reproducibility
        std::uniform_int_distribution<KeyType> dist(0, config.num_keys - 1);
        
        for (size_t i = 0; i < config.num_queries; i++) {
            random_keys.push_back(dist(gen));
        }
        
        results.random_query_time_ms = measure_time_ms([&]() {
            std::vector<std::thread> threads;
            size_t queries_per_thread = config.num_queries / config.num_threads;
            
            for (size_t t = 0; t < config.num_threads; t++) {
                threads.emplace_back([&tree, &random_keys, t, queries_per_thread]() {
                    for (size_t i = 0; i < queries_per_thread; i++) {
                        KeyType key = random_keys[t * queries_per_thread + i];
                        std::vector<ValueType> values;
                        tree.get_value(key, values);
                    }
                });
            }
            
            for (auto& thread : threads) {
                thread.join();
            }
        });
        
        std::cout << "Random query: " << config.num_queries 
                  << " queries in " << results.random_query_time_ms << " ms" << std::endl;
    }
    
    // Clean up temporary file
    std::remove(filename.c_str());
    
    return results;
}

// Save results to CSV
void save_results_to_csv(const std::vector<TestConfig>& configs, 
                         const std::vector<TestResults>& results, 
                         const std::string& filename) {
    std::ofstream file(filename);
    
    // Write header
    file << "Description,Keys,Queries,Threads,Latency(μs),Prefetching,"
         << "Insert(ms),PointQuery(ms),RangeQuery(ms),RandomQuery(ms)\n";
    
    // Write data
    for (size_t i = 0; i < configs.size(); i++) {
        const auto& config = configs[i];
        const auto& result = results[i];
        
        file << "\"" << config.description << "\","
             << config.num_keys << ","
             << config.num_queries << ","
             << config.num_threads << ","
             << config.network_latency_us << ","
             << (config.enable_prefetching ? "Yes" : "No") << ","
             << result.insert_time_ms << ","
             << result.point_query_time_ms << ","
             << result.range_query_time_ms << ","
             << result.random_query_time_ms << "\n";
    }
    
    file.close();
    std::cout << "Results saved to " << filename << std::endl;
}

TEST(MiraPerformanceTest, CompareWithAndWithoutPrefetching) {
    // Test configurations
    std::vector<TestConfig> configs = {
        // No network latency, no prefetching (baseline)
        {
            .num_keys = 100000,
            .num_queries = 10000,
            .num_threads = 4,
            .network_latency_us = 0,
            .enable_prefetching = false,
            .description = "Baseline (No Latency)"
        },
        
        // Low latency tests
        {
            .num_keys = 100000,
            .num_queries = 10000,
            .num_threads = 4,
            .network_latency_us = 100,
            .enable_prefetching = false,
            .description = "Low Latency (100μs) - No Prefetching"
        },
        {
            .num_keys = 100000,
            .num_queries = 10000,
            .num_threads = 4,
            .network_latency_us = 100,
            .enable_prefetching = true,
            .description = "Low Latency (100μs) - With Prefetching"
        },
        
        // Medium latency tests
        {
            .num_keys = 100000,
            .num_queries = 10000,
            .num_threads = 4,
            .network_latency_us = 500,
            .enable_prefetching = false,
            .description = "Medium Latency (500μs) - No Prefetching"
        },
        {
            .num_keys = 100000,
            .num_queries = 10000,
            .num_threads = 4,
            .network_latency_us = 500,
            .enable_prefetching = true,
            .description = "Medium Latency (500μs) - With Prefetching"
        },
        
        // High latency tests (simulating far memory)
        {
            .num_keys = 100000,
            .num_queries = 10000,
            .num_threads = 4,
            .network_latency_us = 1000,
            .enable_prefetching = false,
            .description = "High Latency (1ms) - No Prefetching"
        },
        {
            .num_keys = 100000,
            .num_queries = 10000,
            .num_threads = 4,
            .network_latency_us = 1000,
            .enable_prefetching = true,
            .description = "High Latency (1ms) - With Prefetching"
        },
        
        // Multi-threaded tests
        {
            .num_keys = 100000,
            .num_queries = 10000,
            .num_threads = 8,
            .network_latency_us = 500,
            .enable_prefetching = false,
            .description = "8 Threads, Medium Latency - No Prefetching"
        },
        {
            .num_keys = 100000,
            .num_queries = 10000,
            .num_threads = 8,
            .network_latency_us = 500,
            .enable_prefetching = true,
            .description = "8 Threads, Medium Latency - With Prefetching"
        }
    };
    
    // Run tests and collect results
    std::vector<TestResults> results;
    for (const auto& config : configs) {
        results.push_back(run_performance_test(config));
        std::cout << "-------------------------------------------\n";
    }
    
    // Save results
    save_results_to_csv(configs, results, "mira_prefetching_results.csv");
    
    // Display summary
    std::cout << "\nPerformance Summary:\n";
    for (size_t i = 0; i < configs.size(); i++) {
        std::cout << "Test: " << configs[i].description << "\n";
        results[i].print();
        std::cout << "-------------------------------------------\n";
    }
    
    // Calculate improvement percentages for paired tests
    for (size_t i = 1; i < configs.size(); i += 2) {
        const auto& without_prefetch = results[i-1];
        const auto& with_prefetch = results[i];
        
        double point_query_improvement = 100.0 * (without_prefetch.point_query_time_ms - with_prefetch.point_query_time_ms) / without_prefetch.point_query_time_ms;
        double range_query_improvement = 100.0 * (without_prefetch.range_query_time_ms - with_prefetch.range_query_time_ms) / without_prefetch.range_query_time_ms;
        double random_query_improvement = 100.0 * (without_prefetch.random_query_time_ms - with_prefetch.random_query_time_ms) / without_prefetch.random_query_time_ms;
        
        std::cout << "Improvement with prefetching for " << configs[i].description << ":\n";
        std::cout << "  Point queries: " << std::fixed << std::setprecision(2) << point_query_improvement << "%\n";
        std::cout << "  Range queries: " << range_query_improvement << "%\n";
        std::cout << "  Random queries: " << random_query_improvement << "%\n";
        std::cout << "-------------------------------------------\n";
    }
}