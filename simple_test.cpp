#include "bptree/configurable_cache.h"
#include "bptree/mem_page_cache.h"
#include "bptree/tree.h"

#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <stdexcept>

// Helper function to report exceptions safely
void try_function(const std::string& description, std::function<void()> func) {
    std::cout << "  " << description << "..." << std::flush;
    try {
        func();
        std::cout << " OK" << std::endl;
    } catch (const std::exception& e) {
        std::cout << " ERROR: " << e.what() << std::endl;
        throw; // Rethrow to stop the test
    }
}

int main() {
    try {
        std::cout << "Testing with MemPageCache..." << std::endl;
        // First test with MemPageCache (this should work)
        {
            bptree::MemPageCache cache(4096);
            
            try_function("Creating B+Tree", [&cache]() {
                bptree::BTree<8, uint32_t, uint32_t> tree(&cache);
                
                // Insert a few keys
                for (uint32_t i = 0; i < 10; ++i) {
                    tree.insert(i, i);
                }
                
                // Read back the keys
                std::vector<uint32_t> values;
                for (uint32_t i = 0; i < 10; ++i) {
                    values.clear();
                    tree.get_value(i, values);
                    if (values.empty() || values[0] != i) {
                        throw std::runtime_error("Value mismatch");
                    }
                }
            });
            
            std::cout << "  MemPageCache test completed successfully." << std::endl;
        }

        // Let's try to debug the ConfigurableCache step by step
        std::cout << "\nTesting with ConfigurableCache..." << std::endl;
        {
            std::shared_ptr<bptree::ConfigurableCache> cache;
            
            try_function("Creating ConfigurableCache", [&cache]() {
                cache = std::make_shared<bptree::ConfigurableCache>(1024 * 1024, 4096);
            });
            
            size_t section_id = 0;
            try_function("Creating cache section", [&cache, &section_id]() {
                section_id = cache->create_section(
                    1024 * 1024, // 1MB
                    4096, // 4KB page size
                    bptree::CacheSection::Structure::FullyAssociative
                );
            });
            
            try_function("Mapping page ranges", [&cache, section_id]() {
                cache->map_page_range_to_section(0, 1000, section_id);
            });
            
            std::unique_ptr<bptree::BTree<8, uint32_t, uint32_t>> tree;
            try_function("Creating B+Tree", [&cache, &tree]() {
                tree = std::make_unique<bptree::BTree<8, uint32_t, uint32_t>>(cache.get());
            });
            
            try_function("Inserting keys", [&tree]() {
                for (uint32_t i = 0; i < 5; ++i) {
                    tree->insert(i, i);
                }
            });
            
            try_function("Reading keys", [&tree]() {
                std::vector<uint32_t> values;
                for (uint32_t i = 0; i < 5; ++i) {
                    values.clear();
                    tree->get_value(i, values);
                    if (values.empty() || values[0] != i) {
                        throw std::runtime_error("Value mismatch");
                    }
                }
            });
            
            try_function("Destroying tree", [&tree]() {
                tree.reset();
            });
            
            try_function("Destroying cache", [&cache]() {
                cache.reset();
            });
        }
        
        std::cout << "\nAll tests completed successfully!" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "\nTest failed: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}