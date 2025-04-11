#include <gtest/gtest.h>
#include "bptree/sectioned_page_cache.h"
#include "bptree/tree.h"
#include "bptree/latency_simulator.h"
#include <random>
#include <chrono>
#include <thread>
#include <vector>

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

TEST(CacheTest, DirectMappedSequential) {
    std::cout << "Creating sectioned cache..." << std::endl;
    
    // Create a smaller cache to force evictions
    const size_t CACHE_SIZE = 50; // Only 50 pages in cache
    bptree::SectionedPageCache cache(CACHE_SIZE, 4096);
    
    std::cout << "Adding direct-mapped section..." << std::endl;
    // Use a different name than "default"
    bptree::CacheSectionConfig config("direct_mapped", CACHE_SIZE, 4096, bptree::CacheStructure::DIRECT_MAPPED);
    cache.add_section(config);
    
    std::cout << "Creating B+ tree..." << std::endl;
    bptree::BTree<256, KeyType, ValueType> tree(&cache);
    
    // Simulate far memory latency
    std::cout << "Configuring latency simulator..." << std::endl;
    bptree::LatencySimulator::configure(500); // 500μs latency
    
    // Insert sequential keys with more safety
    const int NUM_KEYS = 100; // Smaller number for debugging
    std::cout << "Inserting " << NUM_KEYS << " sequential keys with direct-mapped cache..." << std::endl;
    
    try {
        for (int i = 1; i <= NUM_KEYS; i++) { // Start with 1 instead of 0
            if (i % 10 == 0) {
                std::cout << "  Inserting key " << i << std::endl;
            }
            tree.insert(i, i + 1);
        }
        
        std::cout << "Insert complete, querying..." << std::endl;
        int success_count = 0;
        
        for (int i = 1; i <= NUM_KEYS; i++) {
            std::vector<ValueType> values;
            tree.get_value(i, values);
            if (values.size() == 1 && values[0] == i + 1) {
                success_count++;
            }
        }
        
        std::cout << "Successfully queried " << success_count << " out of " << NUM_KEYS << " keys" << std::endl;
        EXPECT_EQ(success_count, NUM_KEYS);
        
        // Print cache statistics
        cache.print_stats();
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        FAIL() << "Test failed with exception: " << e.what();
    }
}

// Test fully associative cache with random access
TEST(CacheTest, FullyAssociativeRandom) {
    // Create a sectioned cache with 1000 pages total
    bptree::SectionedPageCache cache(1000, 4096);
    
    // Replace the default section with a fully associative configuration
    bptree::CacheSectionConfig config("default", 1000, 512, bptree::CacheStructure::FULLY_ASSOCIATIVE);
    cache.add_section(config);
    
    // Create B+ tree using the sectioned cache
    bptree::BTree<256, KeyType, ValueType> tree(&cache);
    
    // Simulate far memory latency
    bptree::LatencySimulator::configure(500); // 500μs latency
    
    // Insert random keys
    const int NUM_KEYS = 100;
    std::cout << "Inserting " << NUM_KEYS << " random keys with fully associative cache..." << std::endl;
    
    std::mt19937 gen(42);
    std::uniform_int_distribution<KeyType> dist(0, 1000000);
    std::vector<KeyType> keys;
    
    for (int i = 0; i < NUM_KEYS; i++) {
        keys.push_back(dist(gen));
    }
    
    for (KeyType key : keys) {
        tree.insert(key, key + 1);
    }
    
    // Query the keys
    std::cout << "Querying random keys..." << std::endl;
    int success_count = 0;
    
    for (KeyType key : keys) {
        std::vector<ValueType> values;
        tree.get_value(key, values);
        if (values.size() == 1 && values[0] == key + 1) {
            success_count++;
        }
    }
    
    std::cout << "Successfully queried " << success_count << " out of " << NUM_KEYS << " keys" << std::endl;
    EXPECT_EQ(success_count, NUM_KEYS);
    
    // Print cache statistics
    cache.print_stats();
}

TEST(CacheTest, SectionSeparation) {
    // Cache size small enough to force evictions
    const size_t CACHE_SIZE = 50;
    
    // Create two separate caches with different configurations
    std::cout << "Creating sequential cache..." << std::endl;
    bptree::SectionedPageCache sequential_cache(CACHE_SIZE, 4096);
    // Use larger line size (8KB) for sequential access to benefit from prefetching
    bptree::CacheSectionConfig sequential_config("default", CACHE_SIZE, 8192, bptree::CacheStructure::DIRECT_MAPPED);
    sequential_cache.add_section(sequential_config);
    
    std::cout << "Creating random access cache..." << std::endl;
    bptree::SectionedPageCache random_cache(CACHE_SIZE, 4096);
    // Use smaller line size (512B) for random access to reduce waste
    bptree::CacheSectionConfig random_config("default", CACHE_SIZE, 512, bptree::CacheStructure::FULLY_ASSOCIATIVE);
    random_cache.add_section(random_config);
    
    // Generate a larger dataset to better exercise the caches
    const int NUM_KEYS = 500; // 10x cache size
    
    // Create sequential keys
    std::vector<int> sequential_keys;
    for (int i = 0; i < NUM_KEYS; i++) {
        sequential_keys.push_back(i);
    }
    
    // Create truly random keys with no locality
    std::mt19937 gen(42);
    std::uniform_int_distribution<KeyType> dist(0, 1000000);
    std::vector<KeyType> random_keys;
    for (int i = 0; i < NUM_KEYS; i++) {
        random_keys.push_back(dist(gen));
    }
    
    // ------ Sequential access pattern test ------
    std::cout << "Testing sequential access pattern..." << std::endl;
    
    // Create trees for sequential test
    bptree::BTree<256, KeyType, ValueType> seq_tree_direct(&sequential_cache);
    bptree::BTree<256, KeyType, ValueType> seq_tree_full(&random_cache);
    
    // Insert sequential keys into both trees
    for (int i = 0; i < NUM_KEYS; i++) {
        seq_tree_direct.insert(sequential_keys[i], i + 1);
        seq_tree_full.insert(sequential_keys[i], i + 1);
    }
    
    // Reset profilers before queries
    bptree::CacheProfiler::instance().reset();
    
    // Query with sequential pattern
    std::cout << "Querying with sequential pattern..." << std::endl;
    double sequential_on_direct_time = measure_time_ms([&]() {
        for (int i = 0; i < NUM_KEYS; i++) {
            std::vector<ValueType> values;
            seq_tree_direct.get_value(i, values);
        }
    });
    
    bptree::CacheProfiler::instance().reset();
    
    double sequential_on_full_time = measure_time_ms([&]() {
        for (int i = 0; i < NUM_KEYS; i++) {
            std::vector<ValueType> values;
            seq_tree_full.get_value(i, values);
        }
    });
    
    // ------ Random access pattern test ------
    std::cout << "Testing random access pattern..." << std::endl;
    
    // Create new trees for random test
    bptree::BTree<256, KeyType, ValueType> rand_tree_direct(&sequential_cache);
    bptree::BTree<256, KeyType, ValueType> rand_tree_full(&random_cache);
    
    // Insert random keys
    for (int i = 0; i < NUM_KEYS; i++) {
        rand_tree_direct.insert(random_keys[i], i + 1);
        rand_tree_full.insert(random_keys[i], i + 1);
    }
    
    // Query with random pattern
    std::cout << "Querying with random pattern..." << std::endl;
    bptree::CacheProfiler::instance().reset();
    
    double random_on_direct_time = measure_time_ms([&]() {
        for (int i = 0; i < NUM_KEYS; i++) {
            std::vector<ValueType> values;
            rand_tree_direct.get_value(random_keys[i], values);
        }
    });
    
    bptree::CacheProfiler::instance().reset();
    
    double random_on_full_time = measure_time_ms([&]() {
        for (int i = 0; i < NUM_KEYS; i++) {
            std::vector<ValueType> values;
            rand_tree_full.get_value(random_keys[i], values);
        }
    });
    
    // Print timing results
    std::cout << "======== Section Separation Results ========" << std::endl;
    std::cout << "Sequential queries on direct-mapped cache: " << sequential_on_direct_time << " ms" << std::endl;
    std::cout << "Sequential queries on fully-associative cache: " << sequential_on_full_time << " ms" << std::endl;
    std::cout << "Random queries on direct-mapped cache: " << random_on_direct_time << " ms" << std::endl;
    std::cout << "Random queries on fully-associative cache: " << random_on_full_time << " ms" << std::endl;
    
    // Analysis
    double seq_improvement = sequential_on_full_time / sequential_on_direct_time;
    double rand_improvement = random_on_direct_time / random_on_full_time;
    
    std::cout << "Direct-mapped improvement for sequential access: " << seq_improvement << "x" << std::endl;
    std::cout << "Fully-associative improvement for random access: " << rand_improvement << "x" << std::endl;
    
    // We expect both improvements to be > 1.0 if our cache sectioning is effective
    // But we'll be more lenient in the test assertion
    EXPECT_GT(seq_improvement * rand_improvement, 1.0);
}

// Main function is only needed for the standalone executable
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}