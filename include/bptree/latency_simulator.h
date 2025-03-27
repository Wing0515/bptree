#ifndef _BPTREE_LATENCY_SIMULATOR_H_
#define _BPTREE_LATENCY_SIMULATOR_H_

#include <chrono>
#include <random>
#include <thread>

namespace bptree {

class LatencySimulator {
public:
    static void configure(int base_latency_us, int jitter_us = 0) {
        base_latency = base_latency_us;
        jitter = jitter_us;
    }
    
    static void simulate_network_latency() {
        if (base_latency <= 0) return;
        
        int delay = base_latency;
        if (jitter > 0) {
            static thread_local std::mt19937 gen(std::random_device{}());
            std::uniform_int_distribution<> dist(-jitter, jitter);
            delay += dist(gen);
            if (delay < 0) delay = 0;
        }
        
        std::this_thread::sleep_for(std::chrono::microseconds(delay));
    }
    
private:
    static inline int base_latency = 0;
    static inline int jitter = 0;
};

} // namespace bptree

#endif