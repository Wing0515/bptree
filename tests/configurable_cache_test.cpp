#include <gtest/gtest.h>

#include "bptree/configurable_cache.h"
#include "bptree/tree.h"
#include "bptree/latency_simulator.h"

#include <chrono>
#include <iostream>
#include <random>
#include <thread>
#include <vector>
#include <atomic>
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

// Test basic functionality of configurable cache
TEST(ConfigurableCacheTest, BasicFunctionality) {
    const size_t TOTAL_SIZE = 1024 * 1024; // 1MB cache
    const size_t PAGE_SIZE = 4096;
    
    bptree::ConfigurableCache cache(TOTAL_SIZE, PAGE_SIZE);
    
    // Create a page
    boost::upgrade_lock<bptree::Page> lock;
    bptree::Page* page = cache.new_page(lock);
    ASSERT_NE(page, nullptr);
    
    // Write to the page
    {
        boost::upgrade_to_unique_lock<bptree::Page> ulock(lock);
        auto* buffer = page->get_buffer(ulock);
        memset(buffer, 0xAA, PAGE_SIZE);
    }
    
    // Fetch the page
    bptree::PageID id = page->get_id();
    cache.unpin_page(page, true, lock);
    
    boost::upgrade_lock<bptree::Page> lock2;
    bptree::Page* fetched_page = cache.fetch_page(id, lock2);
    ASSERT_NE(fetched_page, nullptr);
    
    // Check content
    {
        const auto* buffer = fetched_page->get_buffer(lock2);
        for (size_t i = 0; i < PAGE_SIZE; ++i) {
            ASSERT_EQ(buffer[i], 0xAA);
        }
    }
    
    cache.unpin_page(fetched_page, false, lock2);
}

// Test cache section configuration
TEST(ConfigurableCacheTest, SectionConfiguration) {
    const size_t TOTAL_SIZE = 1024 * 1024; // 1MB cache
    const size_t PAGE_SIZE = 4096;
    
    bptree::ConfigurableCache cache(TOTAL_SIZE, PAGE_SIZE);
    
    // Create sections with different configurations
    size_t direct_mapped_section = cache.create_section(
        TOTAL_SIZE / 4, 
        PAGE_SIZE, 
        bptree::CacheSection::Structure::DirectMapped
    );
    
    size_t set_associative_section = cache.create_section(
        TOTAL_SIZE / 4, 
        PAGE_SIZE, 
        bptree::CacheSection::Structure::SetAssociative, 
        4 // associativity
    );
    
    size_t fully_associative_section = cache.create_section(
        TOTAL_SIZE / 4, 
        PAGE_SIZE, 
        bptree::CacheSection::Structure::FullyAssociative
    );
    
    // Map page ranges to sections
    cache.map_page_range_to_section(100, 199, direct_mapped_section);
    cache.map_page_range_to_section(200, 299, set_associative_section);
    cache.map_page_range_to_section(300, 399, fully_associative_section);
    
    // Create pages in each section
    for (bptree::PageID id = 100; id < 400; ++id) {
        boost::upgrade_lock<bptree::Page> lock;
        bptree::Page* page = cache.fetch_page(id, lock);
        ASSERT_NE(page, nullptr);
        
        // Modify page
        {
            boost::upgrade_to_unique_lock<bptree::Page> ulock(lock);
            auto* buffer = page->get_buffer(ulock);
            memset(buffer, id & 0xFF, PAGE_SIZE);
        }
        
        cache.unpin_page(page, true, lock);
    }
    
    // Fetch and verify pages
    for (bptree::PageID id = 100; id < 400; ++id) {
        boost::upgrade_lock<bptree::Page> lock;
        bptree::Page* page = cache.fetch_page(id, lock);
        ASSERT_NE(page, nullptr);
        
        // Verify content
        const auto* buffer = page->get_buffer(lock);
        for (size_t i = 0; i < 10; ++i) { // Check first 10 bytes
            ASSERT_EQ(buffer[i], id & 0xFF);
        }
        
        cache.unpin_page(page, false, lock);
    }
}

// Test comparison of different cache configurations for sequential access
TEST(ConfigurableCacheTest, SequentialAccessPerformance) {
    const size_t TOTAL_SIZE = 10 * 1024 * 1024; // 10MB cache
    const size_t PAGE_SIZE = 4096;
    const size_t NUM_PAGES = 10000;
    const int LATENCY_US = 500; // 0.5ms simulated network latency
    
    // Configure latency simulation
    bptree::LatencySimulator::configure(LATENCY_US);
    
    std::cout << "Testing sequential access performance with different cache configurations\n";
    std::cout << "--------------------------------------------------------------------\n";
    
    // Test with different line sizes for sequential access
    struct TestCase {
        std::string name;
        bptree::CacheSection::Structure structure;
        size_t line_size;
        int associativity;
    };
    
    std::vector<TestCase> test_cases = {
        {"Default Cache", bptree::CacheSection::Structure::FullyAssociative, PAGE_SIZE, 8},
        {"Direct Mapped - Small Lines", bptree::CacheSection::Structure::DirectMapped, PAGE_SIZE, 1},
        {"Direct Mapped - Large Lines", bptree::CacheSection::Structure::DirectMapped, PAGE_SIZE * 4, 1},
        {"Set Associative - Small Lines", bptree::CacheSection::Structure::SetAssociative, PAGE_SIZE, 4},
        {"Set Associative - Large Lines", bptree::CacheSection::Structure::SetAssociative, PAGE_SIZE * 4, 4},
        {"Fully Associative - Small Lines", bptree::CacheSection::Structure::FullyAssociative, PAGE_SIZE, 8},
        {"Fully Associative - Large Lines", bptree::CacheSection::Structure::FullyAssociative, PAGE_SIZE * 4, 8}
    };
    
    for (const auto& test_case : test_cases) {
        // Create cache with specific configuration
        bptree::ConfigurableCache cache(TOTAL_SIZE, PAGE_SIZE);
        
        // Create test section
        size_t section_id = cache.create_section(
            TOTAL_SIZE, 
            test_case.line_size, 
            test_case.structure, 
            test_case.associativity
        );
        
        // Map all pages to this section
        cache.map_page_range_to_section(0, NUM_PAGES - 1, section_id);
        
        // Initialize pages (write phase)
        double write_time = measure_time_ms([&]() {
            for (size_t i = 0; i < NUM_PAGES; ++i) {
                boost::upgrade_lock<bptree::Page> lock;
                bptree::Page* page = cache.fetch_page(i, lock);
                
                {
                    boost::upgrade_to_unique_lock<bptree::Page> ulock(lock);
                    auto* buffer = page->get_buffer(ulock);
                    memset(buffer, i & 0xFF, PAGE_SIZE);
                }
                
                cache.unpin_page(page, true, lock);
            }
        });
        
        // Reset statistics
        cache.reset_all_stats();
        
        // Sequential read phase
        double read_time = measure_time_ms([&]() {
            for (size_t i = 0; i < NUM_PAGES; ++i) {
                boost::upgrade_lock<bptree::Page> lock;
                bptree::Page* page = cache.fetch_page(i, lock);
                
                volatile uint8_t sum = 0;
                const auto* buffer = page->get_buffer(lock);
                for (size_t j = 0; j < 100; ++j) { // Read first 100 bytes
                    sum += buffer[j];
                }
                
                cache.unpin_page(page, false, lock);
            }
        });
        
        // Get statistics
        auto stats = cache.get_all_section_stats();
        double miss_rate = stats[section_id].miss_rate();
        
        // Print results
        std::cout << std::left << std::setw(30) << test_case.name
                  << " | Write: " << std::setw(10) << write_time << " ms"
                  << " | Read: " << std::setw(10) << read_time << " ms"
                  << " | Miss Rate: " << std::setw(10) << (miss_rate * 100.0) << "%"
                  << std::endl;
    }
}

// Test comparison of different cache configurations for random access
TEST(ConfigurableCacheTest, RandomAccessPerformance) {
    const size_t TOTAL_SIZE = 10 * 1024 * 1024; // 10MB cache
    const size_t PAGE_SIZE = 4096;
    const size_t NUM_PAGES = 10000;
    const size_t NUM_ACCESSES = 5000;
    const int LATENCY_US = 500; // 0.5ms simulated network latency
    
    // Configure latency simulation
    bptree::LatencySimulator::configure(LATENCY_US);
    
    // Create random page IDs
    std::mt19937 gen(42); // Fixed seed for reproducibility
    std::uniform_int_distribution<bptree::PageID> dist(0, NUM_PAGES - 1);
    
    std::vector<bptree::PageID> random_pages;
    for (size_t i = 0; i < NUM_ACCESSES; ++i) {
        random_pages.push_back(dist(gen));
    }
    
    std::cout << "\nTesting random access performance with different cache configurations\n";
    std::cout << "------------------------------------------------------------------\n";
    
    // Test with different cache configurations
    struct TestCase {
        std::string name;
        bptree::CacheSection::Structure structure;
        size_t line_size;
        int associativity;
    };
    
    std::vector<TestCase> test_cases = {
        {"Default Cache", bptree::CacheSection::Structure::FullyAssociative, PAGE_SIZE, 8},
        {"Direct Mapped - Small Lines", bptree::CacheSection::Structure::DirectMapped, PAGE_SIZE, 1},
        {"Direct Mapped - Large Lines", bptree::CacheSection::Structure::DirectMapped, PAGE_SIZE * 4, 1},
        {"Set Associative - Small Lines", bptree::CacheSection::Structure::SetAssociative, PAGE_SIZE, 4},
        {"Set Associative - Large Lines", bptree::CacheSection::Structure::SetAssociative, PAGE_SIZE * 4, 4},
        {"Fully Associative - Small Lines", bptree::CacheSection::Structure::FullyAssociative, PAGE_SIZE, 8},
        {"Fully Associative - Large Lines", bptree::CacheSection::Structure::FullyAssociative, PAGE_SIZE * 4, 8}
    };
    
    for (const auto& test_case : test_cases) {
        // Create cache with specific configuration
        bptree::ConfigurableCache cache(TOTAL_SIZE, PAGE_SIZE);
        
        // Create test section
        size_t section_id = cache.create_section(
            TOTAL_SIZE, 
            test_case.line_size, 
            test_case.structure, 
            test_case.associativity
        );
        
        // Map all pages to this section
        cache.map_page_range_to_section(0, NUM_PAGES - 1, section_id);
        
        // Initialize all pages (to ensure they exist)
        for (size_t i = 0; i < NUM_PAGES; ++i) {
            boost::upgrade_lock<bptree::Page> lock;
            bptree::Page* page = cache.fetch_page(i, lock);
            
            {
                boost::upgrade_to_unique_lock<bptree::Page> ulock(lock);
                auto* buffer = page->get_buffer(ulock);
                memset(buffer, i & 0xFF, PAGE_SIZE);
            }
            
            cache.unpin_page(page, true, lock);
        }
        
        // Reset statistics
        cache.reset_all_stats();
        
        // Random access phase
        double access_time = measure_time_ms([&]() {
            for (const auto& page_id : random_pages) {
                boost::upgrade_lock<bptree::Page> lock;
                bptree::Page* page = cache.fetch_page(page_id, lock);
                
                volatile uint8_t sum = 0;
                const auto* buffer = page->get_buffer(lock);
                for (size_t j = 0; j < 100; ++j) { // Read first 100 bytes
                    sum += buffer[j];
                }
                
                cache.unpin_page(page, false, lock);
            }
        });
        
        // Get statistics
        auto stats = cache.get_all_section_stats();
        double miss_rate = stats[section_id].miss_rate();
        
        // Print results
        std::cout << std::left << std::setw(30) << test_case.name
                  << " | Access Time: " << std::setw(10) << access_time << " ms"
                  << " | Miss Rate: " << std::setw(10) << (miss_rate * 100.0) << "%"
                  << std::endl;
    }
}

// Test mixed workload with cache section separation
TEST(ConfigurableCacheTest, MixedWorkloadWithCacheSections) {
    const size_t TOTAL_SIZE = 10 * 1024 * 1024; // 10MB cache
    const size_t PAGE_SIZE = 4096;
    const size_t SEQUENTIAL_PAGES = 5000;
    const size_t RANDOM_PAGES = 10000;
    const size_t NUM_RANDOM_ACCESSES = 5000;
    const int LATENCY_US = 500; // 0.5ms simulated network latency
    
    // Configure latency simulation
    bptree::LatencySimulator::configure(LATENCY_US);
    
    // Create random page IDs
    std::mt19937 gen(42); // Fixed seed for reproducibility
    std::uniform_int_distribution<bptree::PageID> dist(SEQUENTIAL_PAGES, SEQUENTIAL_PAGES + RANDOM_PAGES - 1);
    
    std::vector<bptree::PageID> random_pages;
    for (size_t i = 0; i < NUM_RANDOM_ACCESSES; ++i) {
        random_pages.push_back(dist(gen));
    }
    
    std::cout << "\nTesting mixed workload with cache section separation\n";
    std::cout << "------------------------------------------------\n";
    
    // Test cases
    struct TestCase {
        std::string name;
        bool use_sections;
        bptree::CacheSection::Structure sequential_structure;
        size_t sequential_line_size;
        bptree::CacheSection::Structure random_structure;
        size_t random_line_size;
    };
    
    std::vector<TestCase> test_cases = {
        {
            "Single Generic Section",
            false,
            bptree::CacheSection::Structure::FullyAssociative,
            PAGE_SIZE,
            bptree::CacheSection::Structure::FullyAssociative,
            PAGE_SIZE
        },
        {
            "Separate Optimized Sections",
            true,
            bptree::CacheSection::Structure::DirectMapped,
            PAGE_SIZE * 4, // Larger lines for sequential
            bptree::CacheSection::Structure::FullyAssociative,
            PAGE_SIZE // Smaller lines for random
        }
    };
    
    for (const auto& test_case : test_cases) {
        // Create cache
        bptree::ConfigurableCache cache(TOTAL_SIZE, PAGE_SIZE);
        
        if (test_case.use_sections) {
            // Create specialized sections
            size_t sequential_section = cache.create_section(
                TOTAL_SIZE / 2,
                test_case.sequential_line_size,
                test_case.sequential_structure
            );
            
            size_t random_section = cache.create_section(
                TOTAL_SIZE / 2,
                test_case.random_line_size,
                test_case.random_structure
            );
            
            // Map pages to appropriate sections
            cache.map_page_range_to_section(0, SEQUENTIAL_PAGES - 1, sequential_section);
            cache.map_page_range_to_section(SEQUENTIAL_PAGES, SEQUENTIAL_PAGES + RANDOM_PAGES - 1, random_section);
        }
        
        // Initialize all pages
        for (size_t i = 0; i < SEQUENTIAL_PAGES + RANDOM_PAGES; ++i) {
            boost::upgrade_lock<bptree::Page> lock;
            bptree::Page* page = cache.fetch_page(i, lock);
            
            {
                boost::upgrade_to_unique_lock<bptree::Page> ulock(lock);
                auto* buffer = page->get_buffer(ulock);
                memset(buffer, i & 0xFF, PAGE_SIZE);
            }
            
            cache.unpin_page(page, true, lock);
        }
        
        // Reset statistics
        cache.reset_all_stats();
        
        // Sequential access
        double sequential_time = measure_time_ms([&]() {
            for (size_t i = 0; i < SEQUENTIAL_PAGES; ++i) {
                boost::upgrade_lock<bptree::Page> lock;
                bptree::Page* page = cache.fetch_page(i, lock);
                
                volatile uint8_t sum = 0;
                const auto* buffer = page->get_buffer(lock);
                for (size_t j = 0; j < 100; ++j) {
                    sum += buffer[j];
                }
                
                cache.unpin_page(page, false, lock);
            }
        });
        
        // Random access
        double random_time = measure_time_ms([&]() {
            for (const auto& page_id : random_pages) {
                boost::upgrade_lock<bptree::Page> lock;
                bptree::Page* page = cache.fetch_page(page_id, lock);
                
                volatile uint8_t sum = 0;
                const auto* buffer = page->get_buffer(lock);
                for (size_t j = 0; j < 100; ++j) {
                    sum += buffer[j];
                }
                
                cache.unpin_page(page, false, lock);
            }
        });
        
        // Get statistics
        auto stats = cache.get_all_section_stats();
        double overall_miss_rate = 0.0;
        size_t total_accesses = 0;
        
        for (const auto& stat : stats) {
            overall_miss_rate += stat.misses;
            total_accesses += stat.accesses;
        }
        
        if (total_accesses > 0) {
            overall_miss_rate /= total_accesses;
        }
        
        // Print results
        std::cout << std::left << std::setw(30) << test_case.name
                  << " | Sequential: " << std::setw(10) << sequential_time << " ms"
                  << " | Random: " << std::setw(10) << random_time << " ms"
                  << " | Miss Rate: " << std::setw(10) << (overall_miss_rate * 100.0) << "%"
                  << std::endl;
    }
}

// Test automatic cache size optimization
TEST(ConfigurableCacheTest, CacheSizeOptimization) {
    const size_t TOTAL_SIZE = 10 * 1024 * 1024; // 10MB cache
    const size_t PAGE_SIZE = 4096;
    const size_t SEQUENTIAL_PAGES = 5000;
    const size_t RANDOM_PAGES = 10000;
    const size_t NUM_RANDOM_ACCESSES = 5000;
    const int LATENCY_US = 500; // 0.5ms simulated network latency
    
    // Configure latency simulation
    bptree::LatencySimulator::configure(LATENCY_US);
    
    // Create random page IDs
    std::mt19937 gen(42); // Fixed seed for reproducibility
    std::uniform_int_distribution<bptree::PageID> dist(SEQUENTIAL_PAGES, SEQUENTIAL_PAGES + RANDOM_PAGES - 1);
    
    std::vector<bptree::PageID> random_pages;
    for (size_t i = 0; i < NUM_RANDOM_ACCESSES; ++i) {
        random_pages.push_back(dist(gen));
    }
    
    std::cout << "\nTesting automatic cache size optimization\n";
    std::cout << "--------------------------------------\n";
    
    // Create cache with specialized sections
    bptree::ConfigurableCache cache(TOTAL_SIZE, PAGE_SIZE);
    
    // Create sections with initial equal sizes
    size_t sequential_section = cache.create_section(
        TOTAL_SIZE / 3,
        PAGE_SIZE * 4, // Larger lines for sequential
        bptree::CacheSection::Structure::DirectMapped
    );
    
    size_t random_section = cache.create_section(
        TOTAL_SIZE / 3,
        PAGE_SIZE, // Smaller lines for random
        bptree::CacheSection::Structure::FullyAssociative
    );
    
    // Map pages to appropriate sections
    cache.map_page_range_to_section(0, SEQUENTIAL_PAGES - 1, sequential_section);
    cache.map_page_range_to_section(SEQUENTIAL_PAGES, SEQUENTIAL_PAGES + RANDOM_PAGES - 1, random_section);
    
    // Initialize all pages
    for (size_t i = 0; i < SEQUENTIAL_PAGES + RANDOM_PAGES; ++i) {
        boost::upgrade_lock<bptree::Page> lock;
        bptree::Page* page = cache.fetch_page(i, lock);
        
        {
            boost::upgrade_to_unique_lock<bptree::Page> ulock(lock);
            auto* buffer = page->get_buffer(ulock);
            memset(buffer, i & 0xFF, PAGE_SIZE);
        }
        
        cache.unpin_page(page, true, lock);
    }
    
    // Test with initial equal section sizes
    cache.reset_all_stats();
    
    // Mixed workload
    auto run_mixed_workload = [&]() {
        // Sequential access
        for (size_t i = 0; i < SEQUENTIAL_PAGES; ++i) {
            boost::upgrade_lock<bptree::Page> lock;
            bptree::Page* page = cache.fetch_page(i, lock);
            
            volatile uint8_t sum = 0;
            const auto* buffer = page->get_buffer(lock);
            for (size_t j = 0; j < 100; ++j) {
                sum += buffer[j];
            }
            
            cache.unpin_page(page, false, lock);
        }
        
        // Random access
        for (const auto& page_id : random_pages) {
            boost::upgrade_lock<bptree::Page> lock;
            bptree::Page* page = cache.fetch_page(page_id, lock);
            
            volatile uint8_t sum = 0;
            const auto* buffer = page->get_buffer(lock);
            for (size_t j = 0; j < 100; ++j) {
                sum += buffer[j];
            }
            
            cache.unpin_page(page, false, lock);
        }
    };
    
    // Run with initial configuration
    double initial_time = measure_time_ms(run_mixed_workload);
    
    // Get statistics
    auto initial_stats = cache.get_all_section_stats();
    
    // Print initial results
    std::cout << "Initial configuration:\n";
    std::cout << "  Sequential section size: " << (TOTAL_SIZE / 3) / (1024 * 1024) << " MB\n";
    std::cout << "  Random section size: " << (TOTAL_SIZE / 3) / (1024 * 1024) << " MB\n";
    std::cout << "  Total execution time: " << initial_time << " ms\n";
    std::cout << "  Sequential section miss rate: " << (initial_stats[sequential_section].miss_rate() * 100.0) << "%\n";
    std::cout << "  Random section miss rate: " << (initial_stats[random_section].miss_rate() * 100.0) << "%\n";
    
    // Run size optimization
    cache.optimize_section_sizes();
    
    // Reset statistics
    cache.reset_all_stats();
    
    // Run with optimized configuration
    double optimized_time = measure_time_ms(run_mixed_workload);
    
    // Get statistics
    auto optimized_stats = cache.get_all_section_stats();
    
    // Print optimized results
    std::cout << "\nOptimized configuration:\n";
    std::cout << "  Sequential section miss rate: " << (optimized_stats[sequential_section].miss_rate() * 100.0) << "%\n";
    std::cout << "  Random section miss rate: " << (optimized_stats[random_section].miss_rate() * 100.0) << "%\n";
    std::cout << "  Total execution time: " << optimized_time << " ms\n";
    std::cout << "  Improvement: " << ((initial_time - optimized_time) / initial_time * 100.0) << "%\n";
}

// Test with B+Tree integration
TEST(ConfigurableCacheTest, BTreeIntegration) {
    const size_t TOTAL_SIZE = 50 * 1024 * 1024; // 50MB cache
    const size_t PAGE_SIZE = 4096;
    const size_t NUM_KEYS = 100000;
    const int LATENCY_US = 500; // 0.5ms simulated network latency
    
    // Configure latency simulation
    bptree::LatencySimulator::configure(LATENCY_US);
    
    std::cout << "\nTesting B+Tree integration with configurable cache\n";
    std::cout << "------------------------------------------------\n";
    
    // Create test cases
    struct TestCase {
        std::string name;
        bool use_optimized_cache;
    };
    
    std::vector<TestCase> test_cases = {
        {"Default Cache", false},
        {"Optimized Cache", true}
    };
    
    for (const auto& test_case : test_cases) {
        // Create cache
        auto cache = std::make_unique<bptree::ConfigurableCache>(TOTAL_SIZE, PAGE_SIZE);
        
        if (test_case.use_optimized_cache) {
            // Create optimized sections
            
            // Inner nodes section (random access pattern)
            size_t inner_section = cache->create_section(
                TOTAL_SIZE / 2,
                PAGE_SIZE, // Small line size for random access
                bptree::CacheSection::Structure::FullyAssociative
            );
            
            // Leaf nodes section (more sequential access)
            size_t leaf_section = cache->create_section(
                TOTAL_SIZE / 2,
                PAGE_SIZE * 4, // Larger line size for more sequential access
                bptree::CacheSection::Structure::SetAssociative,
                8 // Higher associativity
            );
            
            // Map page ranges to sections
            // We use the knowledge that B+Tree places inner nodes and leaf nodes
            // in different page ranges
            cache->map_page_range_to_section(1, 1000, inner_section); // Inner nodes (estimate)
            cache->map_page_range_to_section(1001, 1000000, leaf_section); // Leaf nodes (estimate)
        }
        
        // Create B+Tree with the cache
        bptree::BTree<256, KeyType, ValueType> tree(cache.get());
        
        // Insert keys
        double insert_time = measure_time_ms([&]() {
            for (size_t i = 0; i < NUM_KEYS; ++i) {
                tree.insert(i, i + 1);
            }
        });
        
        // Reset statistics
        cache->reset_all_stats();
        
        // Sequential query
        double seq_query_time = measure_time_ms([&]() {
            std::vector<ValueType> values;
            for (size_t i = 0; i < NUM_KEYS; ++i) {
                values.clear();
                tree.get_value(i, values);
                ASSERT_EQ(values.size(), 1);
                ASSERT_EQ(values[0], i + 1);
            }
        });
        
        // Random query
        std::vector<KeyType> random_keys;
        std::mt19937 gen(42);
        std::uniform_int_distribution<KeyType> dist(0, NUM_KEYS - 1);
        
        for (size_t i = 0; i < 10000; ++i) {
            random_keys.push_back(dist(gen));
        }
        
        double random_query_time = measure_time_ms([&]() {
            std::vector<ValueType> values;
            for (const auto& key : random_keys) {
                values.clear();
                tree.get_value(key, values);
                ASSERT_EQ(values.size(), 1);
                ASSERT_EQ(values[0], key + 1);
            }
        });
        
        // Range query
        double range_query_time = measure_time_ms([&]() {
            for (size_t i = 0; i < 1000; i += 100) {
                size_t count = 0;
                for (auto it = tree.begin(i); it != tree.end() && count < 100; ++it) {
                    ASSERT_EQ(it->second, it->first + 1);
                    count++;
                }
            }
        });
        
        // Get statistics
        auto stats = cache->get_all_section_stats();
        double overall_miss_rate = 0.0;
        size_t total_accesses = 0;
        
        for (const auto& stat : stats) {
            overall_miss_rate += stat.misses;
            total_accesses += stat.accesses;
        }
        
        if (total_accesses > 0) {
            overall_miss_rate /= total_accesses;
        }
        
        // Print results
        std::cout << std::left << std::setw(20) << test_case.name
                  << " | Insert: " << std::setw(10) << insert_time << " ms"
                  << " | Seq Query: " << std::setw(10) << seq_query_time << " ms"
                  << " | Random Query: " << std::setw(10) << random_query_time << " ms"
                  << " | Range Query: " << std::setw(10) << range_query_time << " ms"
                  << " | Miss Rate: " << std::setw(6) << (overall_miss_rate * 100.0) << "%"
                  << std::endl;
    }
}

// Test multi-threaded performance with configurable cache
TEST(ConfigurableCacheTest, MultiThreadedPerformance) {
    const size_t TOTAL_SIZE = 50 * 1024 * 1024; // 50MB cache
    const size_t PAGE_SIZE = 4096;
    const size_t NUM_KEYS_PER_THREAD = 10000;
    const size_t NUM_THREADS = 4;
    const int LATENCY_US = 500; // 0.5ms simulated network latency
    
    // Configure latency simulation
    bptree::LatencySimulator::configure(LATENCY_US);
    
    std::cout << "\nTesting multi-threaded performance with configurable cache\n";
    std::cout << "--------------------------------------------------------\n";
    
    // Create test cases
    struct TestCase {
        std::string name;
        bool use_optimized_cache;
    };
    
    std::vector<TestCase> test_cases = {
        {"Default Cache", false},
        {"Optimized Cache", true}
    };
    
    for (const auto& test_case : test_cases) {
        // Create cache
        auto cache = std::make_unique<bptree::ConfigurableCache>(TOTAL_SIZE, PAGE_SIZE);
        
        if (test_case.use_optimized_cache) {
            // Create optimized sections for different access patterns
            size_t inner_section = cache->create_section(
                TOTAL_SIZE / 2,
                PAGE_SIZE,
                bptree::CacheSection::Structure::FullyAssociative
            );
            
            size_t leaf_section = cache->create_section(
                TOTAL_SIZE / 2,
                PAGE_SIZE * 4,
                bptree::CacheSection::Structure::SetAssociative,
                8
            );
            
            // Map page ranges
            cache->map_page_range_to_section(1, 1000, inner_section);
            cache->map_page_range_to_section(1001, 1000000, leaf_section);
        }
        
        // Create B+Tree
        bptree::BTree<256, KeyType, ValueType> tree(cache.get());
        
        // Concurrent insert
        double insert_time = measure_time_ms([&]() {
            std::vector<std::thread> threads;
            
            for (size_t t = 0; t < NUM_THREADS; ++t) {
                threads.emplace_back([&tree, t, NUM_KEYS_PER_THREAD]() {
                    for (size_t i = 0; i < NUM_KEYS_PER_THREAD; ++i) {
                        KeyType key = t * NUM_KEYS_PER_THREAD + i;
                        tree.insert(key, key + 1);
                    }
                });
            }
            
            for (auto& thread : threads) {
                thread.join();
            }
        });
        
        // Reset cache statistics
        cache->reset_all_stats();
        
        // Concurrent query
        double query_time = measure_time_ms([&]() {
            std::vector<std::thread> threads;
            
            for (size_t t = 0; t < NUM_THREADS; ++t) {
                threads.emplace_back([&tree, t, NUM_KEYS_PER_THREAD]() {
                    std::vector<ValueType> values;
                    for (size_t i = 0; i < NUM_KEYS_PER_THREAD; ++i) {
                        KeyType key = t * NUM_KEYS_PER_THREAD + i;
                        values.clear();
                        tree.get_value(key, values);
                        ASSERT_EQ(values.size(), 1);
                        ASSERT_EQ(values[0], key + 1);
                    }
                });
            }
            
            for (auto& thread : threads) {
                thread.join();
            }
        });
        
        // Get statistics
        auto stats = cache->get_all_section_stats();
        double overall_miss_rate = 0.0;
        size_t total_accesses = 0;
        
        for (const auto& stat : stats) {
            overall_miss_rate += stat.misses;
            total_accesses += stat.accesses;
        }
        
        if (total_accesses > 0) {
            overall_miss_rate /= total_accesses;
        }
        
        // Print results
        std::cout << std::left << std::setw(20) << test_case.name
                  << " | Insert: " << std::setw(10) << insert_time << " ms"
                  << " | Query: " << std::setw(10) << query_time << " ms"
                  << " | Miss Rate: " << std::setw(6) << (overall_miss_rate * 100.0) << "%"
                  << std::endl;
    }
}