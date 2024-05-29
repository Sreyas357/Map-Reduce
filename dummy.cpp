#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <map>
#include <algorithm>
#include <queue>

class MapReduce {
public:
    void mymap(const std::vector<int>& inputarray, bool newdec) {
        if (newdec) {
            size_t num_chunks = (inputarray.size() + chunk_size - 1) / chunk_size;
            for (size_t i = 0; i < num_chunks; ++i) {
                std::vector<int> chunk(inputarray.begin() + i * chunk_size, inputarray.begin() + std::min((i + 1) * chunk_size, inputarray.size()));
                std::unique_lock<std::mutex> lock(queue_mutex);
                map_queue.push(chunk);
            }
            map_cv.notify_all();
        } else {
            for (int i : inputarray) {
                std::lock_guard<std::mutex> lock(map_mutex);
                pvtmapbuffer.push_back({i, 1});
            }
        }
    }

    void myred(int key, const std::vector<int>& nums) {
        int currentsum = 0;
        for (int num : nums) {
            currentsum += num;
        }

        std::lock_guard<std::mutex> lock(red_mutex);
        if (pvtsumbuffer.find(key) != pvtsumbuffer.end()) {
            pvtsumbuffer[key] += currentsum;
        } else {
            pvtsumbuffer[key] = currentsum;
        }
        sendsignal(key);
    }

    void run_map_worker() {
        while (true) {
            std::vector<int> chunk;
            {
                std::unique_lock<std::mutex> lock(queue_mutex);
                map_cv.wait(lock, [this] { return !map_queue.empty() || map_done; });
                if (map_queue.empty()) {
                    break;
                }
                chunk = map_queue.front();
                map_queue.pop();
            }
            mymap(chunk, false);
        }
    }

    void run_red_worker() {
        while (true) {
            std::pair<int, std::vector<int>> task;
            {
                std::unique_lock<std::mutex> lock(queue_mutex);
                red_cv.wait(lock, [this] { return !bigbuffer_queue.empty() || red_done; });
                if (bigbuffer_queue.empty()) {
                    break;
                }
                task = bigbuffer_queue.front();
                bigbuffer_queue.pop();
            }
            myred(task.first, task.second);
        }
    }

    void master_thread(const std::vector<int>& inputarray) {
        mymap(inputarray, true);

        // Wait for map tasks to complete
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            map_done = true;
            map_cv.notify_all();
        }

        for (auto& worker : map_workers) {
            worker.join();
        }

        sortAndGroup();

        // Start reduce phase
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            for (const auto& pair : bigbuffer) {
                bigbuffer_queue.push(pair);
            }
            red_cv.notify_all();
        }

        for (auto& worker : red_workers) {
            worker.join();
        }

        print_results();
    }

    void start(const std::vector<int>& inputarray) {
        map_done = false;
        red_done = false;

        for (int i = 0; i < num_workers; ++i) {
            map_workers.emplace_back(&MapReduce::run_map_worker, this);
            red_workers.emplace_back(&MapReduce::run_red_worker, this);
        }

        std::thread master(&MapReduce::master_thread, this, std::ref(inputarray));
        master.join();

        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            red_done = true;
            red_cv.notify_all();
        }
    }

private:
    static constexpr size_t chunk_size = 4;
    static constexpr int num_workers = 5;

    std::vector<std::pair<int, int>> pvtmapbuffer;
    std::map<int, int> pvtsumbuffer;
    std::map<int, std::vector<int>> bigbuffer;

    std::queue<std::vector<int>> map_queue;
    std::queue<std::pair<int, std::vector<int>>> bigbuffer_queue;

    std::vector<std::thread> map_workers;
    std::vector<std::thread> red_workers;

    bool map_done = false;
    bool red_done = false;

    std::mutex map_mutex;
    std::mutex red_mutex;
    std::mutex queue_mutex;
    std::condition_variable map_cv;
    std::condition_variable red_cv;

    void sendsignal(int key) {
        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            bigbuffer.erase(key);
        }
        if (bigbuffer.empty()) {
            std::lock_guard<std::mutex> lock(queue_mutex);
            red_done = true;
            red_cv.notify_all();
        }
    }

    void sortAndGroup() {
        std::lock_guard<std::mutex> lock(map_mutex);
        for (const auto& pair : pvtmapbuffer) {
            bigbuffer[pair.first].push_back(pair.second);
        }
    }

    void print_results() {
        std::lock_guard<std::mutex> lock(red_mutex);
        for (const auto& pair : pvtsumbuffer) {
            std::cout << pair.first << ": " << pair.second << std::endl;
        }
    }
};

int main() {
    MapReduce mapReduce;

    // Example usage
    std::vector<int> input = {1,1,1,1,1,1, 2};
    mapReduce.start(input);

    return 0;
}
