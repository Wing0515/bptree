#include <gtest/gtest.h>
#include <random>
#include <chrono>
#include <algorithm>
#include <fstream>
#include <iomanip>
#include <thread>
#include <vector>
#include <atomic>
#include <numeric>
#include <cmath>

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
    size_t iterations; // Number of iterations to run for this config
};

// Results structure
struct TestResults {
    std::vector<double> insert_time_ms;
    std::vector<double> point_query_time_ms;
    std::vector<double> range_query_time_ms;
    std::vector<double> random_query_time_ms;
    
    // Get averages
    double avg_insert_time() const {
        if (insert_time_ms.empty()) return 0.0;
        return std::accumulate(insert_time_ms.begin(), insert_time_ms.end(), 0.0) / insert_time_ms.size();
    }
    
    double avg_point_query_time() const {
        if (point_query_time_ms.empty()) return 0.0;
        return std::accumulate(point_query_time_ms.begin(), point_query_time_ms.end(), 0.0) / point_query_time_ms.size();
    }
    
    double avg_range_query_time() const {
        if (range_query_time_ms.empty()) return 0.0;
        return std::accumulate(range_query_time_ms.begin(), range_query_time_ms.end(), 0.0) / range_query_time_ms.size();
    }
    
    double avg_random_query_time() const {
        if (random_query_time_ms.empty()) return 0.0;
        return std::accumulate(random_query_time_ms.begin(), random_query_time_ms.end(), 0.0) / random_query_time_ms.size();
    }
    
    // Standard deviations
    double stddev_insert_time() const {
        if (insert_time_ms.size() <= 1) return 0.0;
        double mean = avg_insert_time();
        double sum = 0.0;
        for (double val : insert_time_ms) {
            sum += (val - mean) * (val - mean);
        }
        return std::sqrt(sum / (insert_time_ms.size() - 1));
    }
    
    double stddev_point_query_time() const {
        if (point_query_time_ms.size() <= 1) return 0.0;
        double mean = avg_point_query_time();
        double sum = 0.0;
        for (double val : point_query_time_ms) {
            sum += (val - mean) * (val - mean);
        }
        return std::sqrt(sum / (point_query_time_ms.size() - 1));
    }
    
    double stddev_range_query_time() const {
        if (range_query_time_ms.size() <= 1) return 0.0;
        double mean = avg_range_query_time();
        double sum = 0.0;
        for (double val : range_query_time_ms) {
            sum += (val - mean) * (val - mean);
        }
        return std::sqrt(sum / (range_query_time_ms.size() - 1));
    }
    
    double stddev_random_query_time() const {
        if (random_query_time_ms.size() <= 1) return 0.0;
        double mean = avg_random_query_time();
        double sum = 0.0;
        for (double val : random_query_time_ms) {
            sum += (val - mean) * (val - mean);
        }
        return std::sqrt(sum / (random_query_time_ms.size() - 1));
    }
    
    void print() const {
        std::cout << "Insert time: " << avg_insert_time() << " ± " << stddev_insert_time() << " ms\n";
        std::cout << "Point query time: " << avg_point_query_time() << " ± " << stddev_point_query_time() << " ms\n";
        std::cout << "Range query time: " << avg_range_query_time() << " ± " << stddev_range_query_time() << " ms\n";
        std::cout << "Random query time: " << avg_random_query_time() << " ± " << stddev_random_query_time() << " ms\n";
    }
};

// Run a single iteration of the test with given configuration
void run_single_test_iteration(const TestConfig& config, TestResults& results) {
    // Setup latency simulation
    bptree::LatencySimulator::configure(
        config.enable_prefetching ? config.network_latency_us : 0
    );
    
    try {
        // Use in-memory cache to avoid file-related issues
        bptree::MemPageCache page_cache(4096);
        
        // Create the B+Tree
        bptree::BTree<256, KeyType, ValueType> tree(&page_cache);
        
        // 1. Insert test
        double insert_time = measure_time_ms([&]() {
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
        
        results.insert_time_ms.push_back(insert_time);
        
        // 2. Point query test (sequential keys)
        double point_query_time = measure_time_ms([&]() {
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
        
        results.point_query_time_ms.push_back(point_query_time);
        
        // 3. Range query test
        double range_query_time = measure_time_ms([&]() {
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
        
        results.range_query_time_ms.push_back(range_query_time);
        
        // 4. Random access query test
        {
            // Generate random keys first
            std::vector<KeyType> random_keys;
            std::mt19937 gen(42); // Fixed seed for reproducibility
            std::uniform_int_distribution<KeyType> dist(0, config.num_keys - 1);
            
            for (size_t i = 0; i < config.num_queries; i++) {
                random_keys.push_back(dist(gen));
            }
            
            double random_query_time = measure_time_ms([&]() {
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
            
            results.random_query_time_ms.push_back(random_query_time);
        }
    } catch (const std::exception& e) {
        std::cerr << "Error during test iteration: " << e.what() << std::endl;
        // Don't propagate the exception, just report it
    }
}

// Run test with given configuration (multiple iterations)
TestResults run_performance_test(const TestConfig& config) {
    TestResults results;
    
    std::cout << "Running test: " << config.description << std::endl;
    std::cout << "Number of keys: " << config.num_keys << std::endl;
    std::cout << "Number of queries: " << config.num_queries << std::endl;
    std::cout << "Number of threads: " << config.num_threads << std::endl;
    std::cout << "Network latency: " << config.network_latency_us << " μs" << std::endl;
    std::cout << "Prefetching: " << (config.enable_prefetching ? "Enabled" : "Disabled") << std::endl;
    std::cout << "Iterations: " << config.iterations << std::endl;
    
    for (size_t iter = 0; iter < config.iterations; iter++) {
        std::cout << "  Running iteration " << (iter + 1) << "/" << config.iterations << "..." << std::endl;
        run_single_test_iteration(config, results);
    }
    
    std::cout << "Results:\n";
    std::cout << "  Insert: " << results.avg_insert_time() << " ± " << results.stddev_insert_time() << " ms\n";
    std::cout << "  Point query: " << results.avg_point_query_time() << " ± " << results.stddev_point_query_time() << " ms\n";
    std::cout << "  Range query: " << results.avg_range_query_time() << " ± " << results.stddev_range_query_time() << " ms\n";
    std::cout << "  Random query: " << results.avg_random_query_time() << " ± " << results.stddev_random_query_time() << " ms\n";
    
    return results;
}

// Save results to CSV
void save_results_to_csv(const std::vector<TestConfig>& configs, 
                         const std::vector<TestResults>& results, 
                         const std::string& filename) {
    std::ofstream file(filename);
    
    // Write header
    file << "Description,Keys,Queries,Threads,Latency(μs),Prefetching,Iterations,"
         << "Insert_Avg(ms),Insert_StdDev(ms),"
         << "PointQuery_Avg(ms),PointQuery_StdDev(ms),"
         << "RangeQuery_Avg(ms),RangeQuery_StdDev(ms),"
         << "RandomQuery_Avg(ms),RandomQuery_StdDev(ms)\n";
    
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
             << config.iterations << ","
             << result.avg_insert_time() << "," << result.stddev_insert_time() << ","
             << result.avg_point_query_time() << "," << result.stddev_point_query_time() << ","
             << result.avg_range_query_time() << "," << result.stddev_range_query_time() << ","
             << result.avg_random_query_time() << "," << result.stddev_random_query_time() << "\n";
    }
    
    file.close();
    std::cout << "Results saved to " << filename << std::endl;
}

TEST(MiraPerformanceTest, CompareWithAndWithoutPrefetching) {
    // Determine number of iterations based on system performance
    // More iterations = more accurate results but longer test time
    const size_t NUM_ITERATIONS = 25;

    // Adjust these numbers based on your system's capacity
    // Smaller numbers for faster testing, larger for more realistic results
    const size_t NUM_KEYS = 200000;
    const size_t NUM_QUERIES = 20000;
    
    // Test configurations
    std::vector<TestConfig> configs = {
        // No network latency, no prefetching (baseline)
        {
            .num_keys = NUM_KEYS,
            .num_queries = NUM_QUERIES,
            .num_threads = 4,
            .network_latency_us = 0,
            .enable_prefetching = false,
            .description = "Baseline (No Latency)",
            .iterations = NUM_ITERATIONS
        },
        
        // Low latency tests
        {
            .num_keys = NUM_KEYS,
            .num_queries = NUM_QUERIES,
            .num_threads = 4,
            .network_latency_us = 100,
            .enable_prefetching = false,
            .description = "Low Latency (100μs) - No Prefetching",
            .iterations = NUM_ITERATIONS
        },
        {
            .num_keys = NUM_KEYS,
            .num_queries = NUM_QUERIES,
            .num_threads = 4,
            .network_latency_us = 100,
            .enable_prefetching = true,
            .description = "Low Latency (100μs) - With Prefetching",
            .iterations = NUM_ITERATIONS
        },
        
        // Medium latency tests
        {
            .num_keys = NUM_KEYS,
            .num_queries = NUM_QUERIES,
            .num_threads = 4,
            .network_latency_us = 500,
            .enable_prefetching = false,
            .description = "Medium Latency (500μs) - No Prefetching",
            .iterations = NUM_ITERATIONS
        },
        {
            .num_keys = NUM_KEYS,
            .num_queries = NUM_QUERIES,
            .num_threads = 4,
            .network_latency_us = 500,
            .enable_prefetching = true,
            .description = "Medium Latency (500μs) - With Prefetching",
            .iterations = NUM_ITERATIONS
        },
        
        // High latency tests (simulating far memory)
        {
            .num_keys = NUM_KEYS,
            .num_queries = NUM_QUERIES,
            .num_threads = 4,
            .network_latency_us = 1000,
            .enable_prefetching = false,
            .description = "High Latency (1ms) - No Prefetching",
            .iterations = NUM_ITERATIONS
        },
        {
            .num_keys = NUM_KEYS,
            .num_queries = NUM_QUERIES,
            .num_threads = 4,
            .network_latency_us = 1000,
            .enable_prefetching = true,
            .description = "High Latency (1ms) - With Prefetching",
            .iterations = NUM_ITERATIONS
        },
        
        // Multi-threaded tests
        {
            .num_keys = NUM_KEYS,
            .num_queries = NUM_QUERIES,
            .num_threads = 8,
            .network_latency_us = 500,
            .enable_prefetching = false,
            .description = "8 Threads, Medium Latency - No Prefetching",
            .iterations = NUM_ITERATIONS
        },
        {
            .num_keys = NUM_KEYS,
            .num_queries = NUM_QUERIES,
            .num_threads = 8,
            .network_latency_us = 500,
            .enable_prefetching = true,
            .description = "8 Threads, Medium Latency - With Prefetching",
            .iterations = NUM_ITERATIONS
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
    std::cout << "\nPERFORMANCE IMPROVEMENT PERCENTAGES:\n";
    std::cout << "==================================\n\n";
    
    for (size_t i = 1; i < configs.size(); i += 2) {
        const auto& without_prefetch = results[i-1];
        const auto& with_prefetch = results[i];
        
        double point_query_improvement = 100.0 * (without_prefetch.avg_point_query_time() - with_prefetch.avg_point_query_time()) / without_prefetch.avg_point_query_time();
        double range_query_improvement = 100.0 * (without_prefetch.avg_range_query_time() - with_prefetch.avg_range_query_time()) / without_prefetch.avg_range_query_time();
        double random_query_improvement = 100.0 * (without_prefetch.avg_random_query_time() - with_prefetch.avg_random_query_time()) / without_prefetch.avg_random_query_time();
        
        std::cout << "Improvement with prefetching for " << configs[i].description << ":\n";
        std::cout << "  Point queries: " << std::fixed << std::setprecision(2) << point_query_improvement << "%\n";
        std::cout << "  Range queries: " << range_query_improvement << "%\n";
        std::cout << "  Random queries: " << random_query_improvement << "%\n";
        std::cout << "-------------------------------------------\n";
    }
    
    // Calculate average improvements across all latency levels
    double avg_point_improvement = 0.0;
    double avg_range_improvement = 0.0;
    double avg_random_improvement = 0.0;
    int count = 0;
    
    for (size_t i = 1; i < configs.size(); i += 2) {
        const auto& without_prefetch = results[i-1];
        const auto& with_prefetch = results[i];
        
        avg_point_improvement += 100.0 * (without_prefetch.avg_point_query_time() - with_prefetch.avg_point_query_time()) / without_prefetch.avg_point_query_time();
        avg_range_improvement += 100.0 * (without_prefetch.avg_range_query_time() - with_prefetch.avg_range_query_time()) / without_prefetch.avg_range_query_time();
        avg_random_improvement += 100.0 * (without_prefetch.avg_random_query_time() - with_prefetch.avg_random_query_time()) / without_prefetch.avg_random_query_time();
        count++;
    }
    
    avg_point_improvement /= count;
    avg_range_improvement /= count;
    avg_random_improvement /= count;
    
    std::cout << "\n\nAVERAGE IMPROVEMENT ACROSS ALL CONFIGURATIONS:\n";
    std::cout << "==============================================\n";
    std::cout << "  Point queries: " << std::fixed << std::setprecision(2) << avg_point_improvement << "%\n";
    std::cout << "  Range queries: " << avg_range_improvement << "%\n";
    std::cout << "  Random queries: " << avg_random_improvement << "%\n";
}