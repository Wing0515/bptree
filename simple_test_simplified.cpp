#include "bptree/simplified_configurable_cache.h"
#include "bptree/mem_page_cache.h"
#include "bptree/tree.h"

#include <iostream>
#include <vector>
#include <chrono>
#include <random>

// Helper for measuring execution time
template<typename Func>
double measure_time_ms(Func&& func) {
    auto start = std::chrono::high_resolution_clock::now();
    func();
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::milli>(end - start).count();
}

int main() {
    try {
        // First test with standard MemPageCache
        std::cout << "Testing with MemPageCache..." << std::endl;
        {
            bptree::MemPageCache cache(4096);
            bptree::BTree<256, uint64_t, uint64_t> tree(&cache);
            
            // Insert some keys
            double insert_time = measure_time_ms([&]() {
                for (uint64_t i = 0; i < 10000; ++i) {
                    tree.insert(i, i + 1);
                }
            });
            
            // Query some keys
            double query_time = measure_time_ms([&]() {
                std::vector<uint64_t> values;
                for (uint64_t i = 0; i < 1000; ++i) {
                    values.clear();
                    tree.get_value(i, values);
                    if (values.size() != 1 || values[0] != i + 1) {
                        throw std::runtime_error("Value mismatch");
                    }
                }
            });
            
            std::cout << "  Inserted 10,000 keys in " << insert_time << " ms" << std::endl;
            std::cout << "  Queried 1,000 keys in " << query_time << " ms" << std::endl;
        }
        
        std::cout << "\nTesting with SimplifiedConfigurableCache..." << std::endl;
        
        // Compare different configurations
        struct CacheConfig {
            std::string name;
            bptree::SimplifiedConfigurableCache::Structure structure;
            size_t line_size;
            size_t associativity;
        };
        
        std::vector<CacheConfig> configs = {
            {"Default", bptree::SimplifiedConfigurableCache::Structure::FullyAssociative, 4096, 8},
            {"DirectMapped-Small", bptree::SimplifiedConfigurableCache::Structure::DirectMapped, 4096, 1},
            {"DirectMapped-Large", bptree::SimplifiedConfigurableCache::Structure::DirectMapped, 16384, 1},
            {"SetAssociative", bptree::SimplifiedConfigurableCache::Structure::SetAssociative, 4096, 4},
            {"FullyAssociative", bptree::SimplifiedConfigurableCache::Structure::FullyAssociative, 4096, 8},
        };
        
        for (const auto& config : configs) {
            std::cout << "\nConfiguration: " << config.name << std::endl;
            
            // Configure network latency to simulate far memory
            bptree::LatencySimulator::configure(1000); // 1ms latency
            
            // Create configurable cache
            bptree::SimplifiedConfigurableCache cache(
                50 * 1024 * 1024, // 50MB cache
                4096,             // 4KB page size
                config.line_size,
                config.structure,
                config.associativity
            );
            
            // Create B+Tree
            bptree::BTree<256, uint64_t, uint64_t> tree(&cache);
            
            // Insert keys
            double insert_time = measure_time_ms([&]() {
                for (uint64_t i = 0; i < 10000; ++i) {
                    tree.insert(i, i + 1);
                }
            });
            
            // Reset cache statistics
            cache.reset_stats();
            
            // Sequential query
            double seq_query_time = measure_time_ms([&]() {
                std::vector<uint64_t> values;
                for (uint64_t i = 0; i < 1000; ++i) {
                    values.clear();
                    tree.get_value(i, values);
                }
            });
            
            // Random query
            std::mt19937 gen(42); // Fixed seed for reproducibility
            std::uniform_int_distribution<uint64_t> dist(0, 9999);
            std::vector<uint64_t> random_keys;
            for (int i = 0; i < 1000; ++i) {
                random_keys.push_back(dist(gen));
            }
            
            double random_query_time = measure_time_ms([&]() {
                std::vector<uint64_t> values;
                for (const auto& key : random_keys) {
                    values.clear();
                    tree.get_value(key, values);
                }
            });
            
            // Get statistics
            auto stats = cache.get_stats();
            
            // Print results
            std::cout << "  Insert time: " << insert_time << " ms" << std::endl;
            std::cout << "  Sequential query time: " << seq_query_time << " ms" << std::endl;
            std::cout << "  Random query time: " << random_query_time << " ms" << std::endl;
            std::cout << "  Cache hit rate: " << (100.0 - stats.miss_rate() * 100.0) << "%" << std::endl;
            std::cout << "  Cache size: " << cache.size() << " pages" << std::endl;
        }
        
        std::cout << "\nAll tests completed successfully!" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}