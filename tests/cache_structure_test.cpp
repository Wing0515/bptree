#include <gtest/gtest.h>
#include "bptree/direct_mapped_cache.h"
#include "bptree/fully_associative_cache.h"
#include "bptree/mem_page_cache.h"
#include "bptree/tree.h"
#include "bptree/latency_simulator.h"
#include <random>
#include <chrono>
#include <iostream>
#include <iomanip>

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

// Test demonstrating different cache structures
TEST(CacheStructureTest, ComparePerformance) {
    const size_t CACHE_SIZE = 50;   // Pages in cache
    const size_t PAGE_SIZE = 4096;  // Bytes per page
    
    // Create different cache implementations
    bptree::DirectMappedCache direct_mapped_cache(CACHE_SIZE, PAGE_SIZE, 8192); // Larger line size
    bptree::FullyAssociativeCache fully_associative_cache(CACHE_SIZE, PAGE_SIZE, 512); // Smaller line size
    
    // Set up far memory latency
    bptree::LatencySimulator::configure(500); // 500μs latency
    
    // Create B+ trees using each cache
    bptree::BTree<256, KeyType, ValueType> direct_mapped_tree(&direct_mapped_cache);
    bptree::BTree<256, KeyType, ValueType> fully_associative_tree(&fully_associative_cache);
    
    // Generate test data
    const int NUM_KEYS = 500;
    
    // Sequential keys
    std::vector<KeyType> sequential_keys;
    for (int i = 1; i <= NUM_KEYS; i++) {  // Start at 1 to avoid page 0
        sequential_keys.push_back(i);
    }
    
    // Random keys
    std::mt19937 gen(42);
    std::uniform_int_distribution<KeyType> dist(1000, 1000000);  // Start above 1000 to avoid small numbers
    std::vector<KeyType> random_keys;
    for (int i = 0; i < NUM_KEYS; i++) {
        random_keys.push_back(dist(gen));
    }
    
    std::cout << "\n=== Testing Sequential Insert ===\n";
    // Insert sequential keys into both trees
    double dm_seq_insert_time = measure_time_ms([&]() {
        for (KeyType key : sequential_keys) {
            direct_mapped_tree.insert(key, key + 1);
        }
    });
    
    double fa_seq_insert_time = measure_time_ms([&]() {
        for (KeyType key : sequential_keys) {
            fully_associative_tree.insert(key, key + 1);
        }
    });
    
    std::cout << "Direct Mapped: " << dm_seq_insert_time << " ms\n";
    std::cout << "Fully Associative: " << fa_seq_insert_time << " ms\n";
    
    std::cout << "\n=== Testing Random Insert ===\n";
    // Create new trees
    bptree::BTree<256, KeyType, ValueType> dm_random_tree(&direct_mapped_cache);
    bptree::BTree<256, KeyType, ValueType> fa_random_tree(&fully_associative_cache);
    
    // Insert random keys
    double dm_rand_insert_time = measure_time_ms([&]() {
        for (KeyType key : random_keys) {
            dm_random_tree.insert(key, key + 1);
        }
    });
    
    double fa_rand_insert_time = measure_time_ms([&]() {
        for (KeyType key : random_keys) {
            fa_random_tree.insert(key, key + 1);
        }
    });
    
    std::cout << "Direct Mapped: " << dm_rand_insert_time << " ms\n";
    std::cout << "Fully Associative: " << fa_rand_insert_time << " ms\n";
    
    std::cout << "\n=== Testing Sequential Query ===\n";
    // Query sequential keys
    double dm_seq_query_time = measure_time_ms([&]() {
        for (KeyType key : sequential_keys) {
            std::vector<ValueType> values;
            direct_mapped_tree.get_value(key, values);
        }
    });
    
    double fa_seq_query_time = measure_time_ms([&]() {
        for (KeyType key : sequential_keys) {
            std::vector<ValueType> values;
            fully_associative_tree.get_value(key, values);
        }
    });
    
    std::cout << "Direct Mapped: " << dm_seq_query_time << " ms\n";
    std::cout << "Fully Associative: " << fa_seq_query_time << " ms\n";
    
    std::cout << "\n=== Testing Random Query ===\n";
    // Query random keys
    double dm_rand_query_time = measure_time_ms([&]() {
        for (KeyType key : random_keys) {
            std::vector<ValueType> values;
            dm_random_tree.get_value(key, values);
        }
    });
    
    double fa_rand_query_time = measure_time_ms([&]() {
        for (KeyType key : random_keys) {
            std::vector<ValueType> values;
            fa_random_tree.get_value(key, values);
        }
    });
    
    std::cout << "Direct Mapped: " << dm_rand_query_time << " ms\n";
    std::cout << "Fully Associative: " << fa_rand_query_time << " ms\n";
    
    // Performance analysis
    std::cout << "\n=== Performance Analysis ===\n";
    double dm_seq_advantage = fa_seq_query_time / dm_seq_query_time;
    double fa_rand_advantage = dm_rand_query_time / fa_rand_query_time;
    
    std::cout << "Direct mapped advantage for sequential access: " 
              << std::fixed << std::setprecision(2) << dm_seq_advantage << "x\n";
    std::cout << "Fully associative advantage for random access: " 
              << std::fixed << std::setprecision(2) << fa_rand_advantage << "x\n";
    
    // Overall effectiveness of cache structure matching
    double overall_improvement = dm_seq_advantage * fa_rand_advantage;
    std::cout << "Overall cache structure matching effectiveness: " 
              << std::fixed << std::setprecision(2) << overall_improvement << "x\n";
    
    // We expect the direct mapped cache to be better for sequential access
    // and the fully associative cache to be better for random access
    EXPECT_GT(overall_improvement, 1.0);
}

// Main function allows running this test individually
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}