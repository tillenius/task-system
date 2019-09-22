#pragma once
#include <atomic>
#include <vector>
#include <functional>
#include <mutex>
#include <iostream>
#include <unordered_map>
#include <condition_variable>

#define LOGGING
#include "Logging.h"

struct Task;
struct TaskQueue;
class TaskSystem;

const static int MAX_DEPENDENCIES = 4;

std::atomic<unsigned long long> t_inc_executed = 0;
std::atomic<unsigned long long> t_queue_added = 0;
std::atomic<unsigned long long> n_enqueue_start = 0;
std::atomic<unsigned long long> n_enqueue_retry = 0;
std::atomic<unsigned long long> t_taskbuffer_store = 0;
std::atomic<unsigned long long> t_queueidx_store = 0;
std::atomic<unsigned long long> t_checkdone = 0;
std::atomic<unsigned long long> t_fetch = 0;
std::atomic<unsigned long long> t_maincheckdone = 0;
std::atomic<unsigned long long> t_mainwait = 0;

struct Handle {
	std::atomic<int> version{ 0 };
	std::atomic<uint64_t> next{ 0 };
	std::atomic_flag mutex = ATOMIC_FLAG_INIT;
	std::vector<Task *> queue;
	int queueIndex = 0;

	int schedule_read();
	int schedule_write();
	void release() { ++version; }
	void enqueue(Task * task);
	Task * dequeue();
};

struct Dependency {
	Handle * handle;
	int version;
	Dependency() {};
	Dependency(Handle * handle, int version) : handle(handle), version(version) {}
};

struct Context {
	TaskSystem * task_system;
	Task * current_task;

	Context(TaskSystem * task_system, Task * current_task) : task_system(task_system), current_task(current_task) {}
};

struct alignas(64) Task {
	Dependency dependencies[MAX_DEPENDENCIES];
	std::function<void(Context)> fn;
	TaskQueue * queue = nullptr;
};

//struct TaskBuffer {
//	alignas(128) std::atomic<Task *> tasks[1024];
//};

struct TaskQueue {
	alignas(64) std::atomic<bool> gotWork{ false };

	alignas(64) std::atomic_flag mutex = ATOMIC_FLAG_INIT;
	std::vector< Task * > tasks;
	alignas(64) std::atomic<int> next = 0;

	//alignas(128) std::atomic<Task *> taskbuffer[1024];

	//std::condition_variable condVar;
	alignas(64) std::atomic<int> executed = 0;
	alignas(64) std::atomic<int> added = 0;
	alignas(64) TaskSystem & task_system;
	std::atomic<uint32_t> queueidx = 0;

	TaskQueue(TaskSystem & task_system) : task_system(task_system) {
	}

	void enqueue(Task * t);
	bool done();
	Task * fetch();
	Task * run_task(Task * t, bool get_successor);
};

struct TaskHandleMap {
	std::atomic_flag mutex = ATOMIC_FLAG_INIT;
	std::unordered_map< void *, Handle *> handle_map;

	~TaskHandleMap() {
		for (auto & it : handle_map) {
			delete it.second;
		}
		handle_map.clear();
	}

	Handle * get(void * ptr) {
		while (mutex.test_and_set(std::memory_order_acquire));
		Handle *& h = handle_map[ptr];
		if (h == nullptr) {
			h = new Handle();
		}
		mutex.clear(std::memory_order_release);
		return h;
	}
};

class TaskSystem {
public:
	TaskSystem();

	void submit(TaskQueue & queue, Task * task);
	void submit(TaskQueue & queue, std::function<void(Context)> && fn);
	void submit1(TaskQueue & queue, std::initializer_list<void *> inputList, std::initializer_list<void *> outputList, std::function<void(Context)> && fn);
	void submit2(TaskQueue & queue, std::initializer_list<Handle *> inputList, std::initializer_list<Handle *> outputList, std::function<void(Context)> && fn);
	void run(int num_workers);

	TaskQueue main;
	TaskQueue shared;
	TaskHandleMap handle_map;
	std::vector<std::thread> workers;
	alignas(128) std::atomic<int> taskindex;
	std::vector<Task> g_tasks;
	std::atomic<bool> done = false;

};

//class SplitTask {
//public:
//	SplitTask(Context c) : task_system(*c.task_system) {
//		handle = new Handle();
//		join_task = new Task();
//		std::swap(join_task->dependencies, c.current_task->dependencies);
//		join_task->fn = [](Context c) {
//			// the last handle of join_task is the join handle.
//			// delete it and remove from the dependencies list so it won't get accessed after this task is finished.
//			delete c.current_task->dependencies.back().handle;
//			c.current_task->dependencies.pop_back();
//		};
//	}
//
//	~SplitTask() {
//		join_task->dependencies.push_back(Dependency(handle,handle->schedule_write()));
//		task_system.submit(task_system.shared, join_task);
//	}
//
//	void subtask(TaskQueue & queue, std::function<void(Context)> && fn);
//	void subtask(TaskQueue & queue, std::initializer_list<void *> inputList, std::initializer_list<void *> outputList, std::function<void(Context)> && fn);
//	void subtask(TaskQueue & queue, std::initializer_list<Handle *> inputList, std::initializer_list<Handle *> outputList, std::function<void(Context)> && fn);
//
//private:
//	Task * join_task;
//	Handle * handle;
//	TaskSystem & task_system;
//};

inline int Handle::schedule_read() {
	uint64_t was = next.fetch_add(0x0000000100000000);
	return was & 0xffffffff;
}

inline int Handle::schedule_write() {
	for (;;) {
		uint64_t was = next.load();
		uint64_t newValue = was;
		newValue &= 0xffffffff00000000;
		newValue += 0x0000000100000000;
		newValue |= newValue >> 32;

		if (next.compare_exchange_weak(was, newValue)) {
			return (was >> 32) & 0xffffffff;
		}
	}
}

inline void Handle::enqueue(Task * task) {
	while (mutex.test_and_set(std::memory_order_acquire));
	queue.push_back(task);
	mutex.clear(std::memory_order_release);
}

inline Task * Handle::dequeue() {
	while (mutex.test_and_set(std::memory_order_acquire));
	if (queueIndex >= queue.size()) {
		mutex.clear(std::memory_order_release);
		return nullptr;
	}
	Task * t = queue[queueIndex];
	for (int i = 0; i < MAX_DEPENDENCIES && t->dependencies[i].handle != nullptr; ++i) {
		if (t->dependencies[i].handle == this) {
			if (t->dependencies[i].version > version.load()) {
				mutex.clear(std::memory_order_release);
				return nullptr;
			}
			++queueIndex;
			mutex.clear(std::memory_order_release);
			return t;
		}
	}
	std::cout << "###ERROR\n";
	exit(0);
}

inline void TaskQueue::enqueue(Task * t) {
	{
		LOG_TimeUnit t0 = LOG_getTimeStart();

		while (mutex.test_and_set(std::memory_order_acquire));
		tasks.push_back(t);
		mutex.clear(std::memory_order_release);
		//++n_enqueue_start;
		//for (;;) {
		//	uint32_t was = queueidx.load();
		//	uint32_t first = (was & 0x3ff);
		//	uint32_t ready = (was & 0xffc00) >> 10;
		//	uint32_t last = (was & 0x3ff00000) >> 20;
		//	if (ready != last) {
		//		// another thread is adding, step back
		//		//std::this_thread::yield();
		//		continue;
		//	}
		//	last = (last + 1) & (1024 - 1);
		//	if (last == first) {
		//		// no room!
		//		Task * task = fetch();
		//		if (task == nullptr) {
		//			continue;
		//		}
		//		run_task(task, false);
		//		continue;
		//	}
		//	uint32_t newValue = first | (ready << 10) | (last << 20);
		//	if (!queueidx.compare_exchange_weak(was, newValue)) {
		//		++n_enqueue_retry;
		//		continue;
		//	}
		//	LOG_TimeUnit ta0 = LOG_getTimeStart();
		//	taskbuffer[ready].store(t);
		//	LOG_TimeUnit ta1 = LOG_getTimeStart();
		//	ready = (ready + 1) & (1024 - 1);
		//	newValue = first | (ready << 10) | (last << 20);
		//	LOG_TimeUnit tb0 = LOG_getTimeStart();
		//	queueidx.store(newValue);
		//	LOG_TimeUnit tb1 = LOG_getTimeStart();

		//	t_taskbuffer_store += ta1 - ta0;
		//	t_queueidx_store += tb1 - tb0;

		//	//printf("enqueue %d => %p\n", (ready - 1), t);
		//	break;
		//}
		gotWork.store(true);

		LOG_TimeUnit t1 = LOG_getTimeStart();
		LOG_add("enqueue", t0, t1);

	}
	//condVar.notify_one();
}

inline bool TaskQueue::done() {
	//if (added == executed.load()) {
	//	return true;
	//}
	//std::unique_lock<std::mutex> guard(mutex);
	//condVar.wait(guard);
	return added.load() == executed.load();
}

inline Task * TaskQueue::fetch() {
	//for (;;) {
	//	uint32_t was = queueidx.load();
	//	uint32_t first = (was & 0x3ff);
	//	uint32_t ready = (was & 0xffc00) >> 10;
	//	uint32_t last = (was & 0x3ff00000) >> 20;
	//	if (ready != last) {
	//		// another thread is adding, step back
	//		//std::this_thread::yield();
	//		continue;
	//	}
	//	if (last == first) {
	//		// empty queue
	//		gotWork.store(false);
	//		return nullptr;
	//	}
	//	Task * task = taskbuffer[first].load();
	//	first = (first + 1) & (1024-1);
	//	uint32_t newValue = first | (ready << 10) | (last << 20);
	//	if (queueidx.compare_exchange_weak(was, newValue)) {
	//		//printf("fetch %d => %p\n", (first - 1), task);
	//		return task;
	//	}
	//}

	//std::lock_guard<std::mutex> guard(mutex);
	while (mutex.test_and_set(std::memory_order_acquire));
	if (next >= tasks.size()) {
		//tasks.clear();
		//next = 0;
		gotWork.store(false);
		mutex.clear(std::memory_order_release);
		return nullptr;
	}
	Task * task = tasks[next++];
	if (next == tasks.size()) {
		gotWork.store(false);
	}
	mutex.clear(std::memory_order_release);
	return task;
}

inline Task * TaskQueue::run_task(Task * t, bool get_successor) {
	//printf("run %p\n", t);
	t->fn(Context(&task_system, t));

	Task * first_successor = nullptr;
	for (int i = 0; i < MAX_DEPENDENCIES; ++i) {
		if (t->dependencies[i].handle == nullptr) {
			break;
		}

		Dependency & d(t->dependencies[i]);
		d.handle->release();
		Task * successor = d.handle->dequeue();
		if (successor != nullptr) {

			TaskQueue * target_queue = successor->queue == nullptr ? this : successor->queue;
			if (get_successor && first_successor == nullptr && target_queue == this) {
				first_successor = successor;
			}
			else {
				target_queue->enqueue(successor);
			}
		}
	}

	unsigned long long int t0 = __rdtsc();
	++executed;
	unsigned long long int t1 = __rdtsc();
	t_inc_executed += t1 - t0;
	//if (++executed == added) {
	//	//condVar.notify_all();
	//}

	//delete t;
	return first_successor;
}

//inline void TaskSystem::submit(TaskQueue & queue, Task * t) {
//	unsigned long long int t0 = __rdtsc();
//	++queue.added;
//	unsigned long long int t1 = __rdtsc();
//	t_queue_added += t1 - t0;
//	for (Dependency & dep : t->dependencies) {
//		if (dep.version > dep.handle->version.load()) {
//			dep.handle->enqueue(t);
//			return;
//		}
//	}
//	queue.enqueue(t);
//}
//
//inline void TaskSystem::submit(TaskQueue & queue, std::function<void(Context)> && fn) {
//	++queue.added;
//	Task * t = new Task();
//	t->fn = fn;
//	t->queue = &queue;
//	queue.enqueue(t);
//}
//
//inline void TaskSystem::submit1(TaskQueue & queue, std::initializer_list<void *> inputList, std::initializer_list<void *> outputList, std::function<void(Context)> && fn) {
//	++queue.added;
//	Task * t = new Task();
//	t->fn = fn;
//	t->queue = &queue;
//	Handle * wait_for = nullptr;
//	int idx = 0;
//	for (void * ptr : inputList) {
//		Handle * h = handle_map.get(ptr);
//		const int curr = h->version.load();
//		const int required = h->schedule_read();
//		t->dependencies[idx++] = Dependency(h, required);
//		if (wait_for == nullptr && curr < required) {
//			wait_for = h;
//		}
//	}
//	for (void * ptr : outputList) {
//		Handle * h = handle_map.get(ptr);
//		const int curr = h->version.load();
//		const int required = h->schedule_write();
//		t->dependencies[idx++] = Dependency(h, required);
//		if (wait_for == nullptr && curr < required) {
//			wait_for = h;
//		}
//	}
//	t->dependencies[idx++].handle = nullptr;
//	if (wait_for == nullptr) {
//		queue.enqueue(t);
//	}
//	else {
//		wait_for->enqueue(t);
//	}
//}


inline void TaskSystem::submit2(TaskQueue & queue, std::initializer_list<Handle *> inputList, std::initializer_list<Handle *> outputList, std::function<void(Context)> && fn) {
	unsigned long long int t0 = __rdtsc();
	++queue.added;
	unsigned long long int t1 = __rdtsc();
	t_queue_added += t1 - t0;

	//Task * t = new Task();
	int index = taskindex.fetch_add(1);
	Task * t = &g_tasks[index];
	t->fn = fn;
	t->queue = &queue;
	Handle * wait_for = nullptr;

	int idx = 0;
	for (Handle * handle : inputList) {
		const int curr = handle->version.load();
		const int required = handle->schedule_read();
		t->dependencies[idx++] = Dependency(handle, required);
		if (wait_for == nullptr && curr < required) {
			wait_for = handle;
		}
	}
	for (Handle * handle : outputList) {
		const int curr = handle->version.load();
		const int required = handle->schedule_write();
		t->dependencies[idx++] = Dependency(handle, required);
		if (wait_for == nullptr && curr < required) {
			wait_for = handle;
		}
	}

	t->dependencies[idx++].handle = nullptr;
	if (wait_for == nullptr) {
		queue.enqueue(t);
	}
	else {
		wait_for->enqueue(t);
	}
}

//inline void SplitTask::subtask(TaskQueue & queue, std::function<void(Context)> && fn) {
//	++queue.added;
//	Task * t = new Task();
//	t->fn = fn;
//	t->queue = &queue;
//	t->dependencies.push_back(Dependency(handle, handle->schedule_read()));
//	queue.enqueue(t);
//}
//
//inline void SplitTask::subtask(TaskQueue & queue, std::initializer_list<void *> inputList, std::initializer_list<void *> outputList, std::function<void(Context)> && fn) {
//	++queue.added;
//	Task * t = new Task();
//	t->fn = fn;
//	t->queue = &queue;
//	t->dependencies.push_back(Dependency(handle, handle->schedule_read()));
//	Handle * wait_for = nullptr;
//	for (void * ptr : inputList) {
//		Handle * h = task_system.handle_map.get(ptr);
//		const int curr = h->version.load();
//		const int required = h->schedule_read();
//		t->dependencies.push_back(Dependency(h, required));
//		if (wait_for == nullptr && curr < required) {
//			wait_for = h;
//		}
//	}
//	for (void * ptr : outputList) {
//		Handle * h = task_system.handle_map.get(ptr);
//		const int curr = h->version.load();
//		const int required = h->schedule_write();
//		t->dependencies.push_back(Dependency(h, required));
//		if (wait_for == nullptr && curr < required) {
//			wait_for = h;
//		}
//	}
//	if (wait_for == nullptr) {
//		queue.enqueue(t);
//	}
//	else {
//		wait_for->enqueue(t);
//	}
//}
//
//inline void SplitTask::subtask(TaskQueue & queue, std::initializer_list<Handle *> inputList, std::initializer_list<Handle *> outputList, std::function<void(Context)> fn) {
//	++queue.added;
//	Task * t = new Task();
//	t->fn = fn;
//	t->queue = &queue;
//	t->dependencies.push_back(Dependency(handle, handle->schedule_read()));
//	Handle * wait_for = nullptr;
//	for (Handle * handle : inputList) {
//		const int curr = handle->version.load();
//		const int required = handle->schedule_read();
//		t->dependencies.push_back(Dependency(handle, required));
//		if (wait_for == nullptr && curr < required) {
//			wait_for = handle;
//		}
//	}
//	for (Handle * handle : outputList) {
//		const int curr = handle->version.load();
//		const int required = handle->schedule_write();
//		t->dependencies.push_back(Dependency(handle, required));
//		if (wait_for == nullptr && curr < required) {
//			wait_for = handle;
//		}
//	}
//	if (wait_for == nullptr) {
//		queue.enqueue(t);
//	}
//	else {
//		wait_for->enqueue(t);
//	}
//}

inline TaskSystem::TaskSystem() : main(*this), shared(*this), g_tasks(1000000) {
	LOG_add1("main.start");

	int num_workers = 3;
	// worker threads. runs only tasks from the ts.shared queue
	for (int i = 0; i < num_workers; ++i) {
		workers.push_back(std::thread([this] {
			LOG_add1("thread");

			for (;;) {
				{
					LOG_TimeUnit t0 = LOG_getTimeStart();
					while (!shared.gotWork.load() && !shared.done());
					LOG_TimeUnit t1 = LOG_getTimeStart();
					LOG_add("shared.wait", t0, t1);
				}


				LOG_TimeUnit t0 = LOG_getTimeStart();
				Task * t = shared.fetch();
				LOG_TimeUnit t1 = LOG_getTimeStart();
				LOG_add("shared.fetch", t0, t1);
				t_fetch += t1 - t0;

				LOG_TimeUnit t0a = LOG_getTimeStart();

				while (t != nullptr) {
					t = shared.run_task(t, true);
				}

				LOG_TimeUnit t1a = LOG_getTimeStart();
				LOG_add("shared.run", t0a, t1a);

				{
					LOG_TimeUnit t0 = LOG_getTimeStart();
					if (done.load()) {
						break;
					}
					LOG_TimeUnit t1 = LOG_getTimeStart();
					LOG_add("checkdone", t0, t1);
					t_checkdone += t1 - t0;
				}
			}
			LOG_add1("done");
		}));
	}
}

inline void TaskSystem::run(int num_workers) {
	//// worker threads. runs only tasks from the ts.shared queue
	//std::vector<std::thread> workers;
	//for (int i = 0; i < num_workers; ++i) {
	//	workers.push_back(std::thread([this] {
	//		do {
	//			Task * t = shared.fetch();
	//			while (t != nullptr) {
	//				t = shared.run_task(t, true);
	//				if (t == nullptr) {
	//					t = shared.fetch();
	//				}
	//			}
	//		} while (!shared.wait() || !main.wait());
	//	}));
	//}

	// main thread.

	LOG_add1("main.work");

	for (;;) {
		while (main.gotWork.load()) {
			LOG_TimeUnit t0 = LOG_getTimeStart();
			Task * t = main.fetch();
			LOG_TimeUnit t1 = LOG_getTimeStart();
			LOG_add("main.fetch", t0, t1);
			while (t != nullptr) {
				t = main.run_task(t, true);
				if (t == nullptr) {
					t0 = LOG_getTimeStart();
					t = main.fetch();
					t1 = LOG_getTimeStart();
					LOG_add("main.fetch2", t0, t1);
				}
			}
		}

		if (shared.gotWork.load()) {
			LOG_TimeUnit t0 = LOG_getTimeStart();
			Task * t = shared.fetch();
			LOG_TimeUnit t1 = LOG_getTimeStart();
			LOG_add("shared.fetch2", t0, t1);
			if (t != nullptr) {
				shared.run_task(t, false);
				continue;
			}
		}

		{
			LOG_TimeUnit t0 = LOG_getTimeStart();
			if (main.done() && shared.done()) {
				break;
			}
			LOG_TimeUnit t1 = LOG_getTimeStart();
			LOG_add("checkdone", t0, t1);
			t_maincheckdone += t1 - t0;
		}

		{
			LOG_TimeUnit t0 = LOG_getTimeStart();
			while (!main.gotWork.load() && !shared.gotWork.load()) {
				if (main.done() && shared.done()) {
					break;
				}
			}
			LOG_TimeUnit t1 = LOG_getTimeStart();
			LOG_add("both.wait", t0, t1);
			t_mainwait += t1 - t0;
		}
	}

	done.store(true);

	// wait for all threads to finish
	for (int i = 0; i < workers.size(); ++i) {
		workers[i].join();
	}
}
