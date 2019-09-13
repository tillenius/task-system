#pragma once
#include <atomic>
#include <vector>
#include <functional>
#include <mutex>
#include <iostream>
#include <unordered_map>
#include <condition_variable>

struct Task;
struct TaskQueue;
class TaskSystem;

struct Handle {
	std::atomic<int> version{ 0 };
	std::atomic<int64_t> next{ 0 };
	std::mutex mutex;
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
	Dependency(Handle * handle, int version) : handle(handle), version(version) {}
};

struct Context {
	TaskSystem * task_system;
	Task * current_task;

	Context(TaskSystem * task_system, Task * current_task) : task_system(task_system), current_task(current_task) {}
};

struct Task {
	std::vector<Dependency> dependencies;
	std::function<void(Context)> fn;
	TaskQueue * queue = nullptr;
};

struct TaskQueue {
	std::vector< Task * > tasks;
	std::mutex mutex;
	std::condition_variable condVar;
	std::atomic<int> executed = 0;
	std::atomic<int> added = 0;
	TaskSystem & task_system;
	int next = 0;

	TaskQueue(TaskSystem & task_system) : task_system(task_system) {}

	void enqueue(Task * t);
	bool wait();
	Task * fetch();
	Task * run_task(Task * t, bool get_successor);
};

struct TaskHandleMap {
	std::mutex mutex;
	std::unordered_map< void *, Handle *> handle_map;

	~TaskHandleMap() {
		for (auto & it : handle_map) {
			delete it.second;
		}
		handle_map.clear();
	}

	Handle * get(void * ptr) {
		std::lock_guard<std::mutex> guard(mutex);
		Handle *& h = handle_map[ptr];
		if (h == nullptr) {
			h = new Handle();
		}
		return h;
	}

};

class TaskSystem {
public:
	TaskSystem() : main(*this), shared(*this) {}

	void submit(TaskQueue & queue, Task * task);
	void submit(TaskQueue & queue, std::function<void(Context)> fn);
	void submit(TaskQueue & queue, std::initializer_list<void *> inputList, std::initializer_list<void *> outputList, std::function<void(Context)> fn);
	void submit(TaskQueue & queue, std::initializer_list<Handle *> inputList, std::initializer_list<Handle *> outputList, std::function<void(Context)> fn);
	void run(int num_workers);

	TaskQueue main;
	TaskQueue shared;
	TaskHandleMap handle_map;
};

class SplitTask {
public:
	SplitTask(Context c) : task_system(*c.task_system) {
		handle = new Handle();
		join_task = new Task();
		std::swap(join_task->dependencies, c.current_task->dependencies);
		join_task->fn = [](Context c) {
			// the last handle of join_task is the join handle.
			// delete it and remove from the dependencies list so it won't get accessed after this task is finished.
			delete c.current_task->dependencies.back().handle;
			c.current_task->dependencies.pop_back();
		};
	}

	~SplitTask() {
		join_task->dependencies.push_back(Dependency(handle,handle->schedule_write()));
		task_system.submit(task_system.shared, join_task);
	}

	void subtask(TaskQueue & queue, std::function<void(Context)> fn);
	void subtask(TaskQueue & queue, std::initializer_list<void *> inputList, std::initializer_list<void *> outputList, std::function<void(Context)> fn);
	void subtask(TaskQueue & queue, std::initializer_list<Handle *> inputList, std::initializer_list<Handle *> outputList, std::function<void(Context)> fn);

private:
	Task * join_task;
	Handle * handle;
	TaskSystem & task_system;
};

inline int Handle::schedule_read() {
	int64_t was = next.fetch_add(0x0000000100000000);
	return was & 0xffffffff;
}

inline int Handle::schedule_write() {
	for (;;) {
		int64_t was = next.load();
		int64_t newValue = was;
		newValue &= 0xffffffff00000000;
		newValue += 0x0000000100000000;
		newValue |= newValue >> 32;

		if (next.compare_exchange_weak(was, newValue)) {
			return (was >> 32) & 0xffffffff;
		}
	}
}

inline void Handle::enqueue(Task * task) {
	std::lock_guard<std::mutex> guard(mutex);
	queue.push_back(task);
}

inline Task * Handle::dequeue() {
	std::lock_guard<std::mutex> guard(mutex);
	if (queueIndex >= queue.size()) {
		return nullptr;
	}
	Task * t = queue[queueIndex];
	for (int i = 0; i < t->dependencies.size(); ++i) {
		if (t->dependencies[i].handle == this) {
			if (t->dependencies[i].version > version.load()) {
				return nullptr;
			}
			++queueIndex;
			return t;
		}
	}
	std::cout << "###ERROR\n";
	exit(0);
}

inline void TaskQueue::enqueue(Task * t) {
	{
		std::lock_guard<std::mutex> guard(mutex);
		tasks.push_back(t);
	}
	condVar.notify_one();
}

inline bool TaskQueue::wait() {
	if (added == executed.load()) {
		return true;
	}
	std::unique_lock<std::mutex> guard(mutex);
	condVar.wait(guard);
	return added == executed.load();
}

inline Task * TaskQueue::fetch() {
	std::lock_guard<std::mutex> guard(mutex);
	if (next >= tasks.size()) {
		tasks.clear();
		next = 0;
		return nullptr;
	}
	return tasks[next++];
}

inline Task * TaskQueue::run_task(Task * t, bool get_successor) {
	t->fn(Context(&task_system, t));

	Task * first_successor = nullptr;
	for (Dependency & d : t->dependencies) {
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

	if (++executed == added) {
		condVar.notify_all();
	}

	delete t;
	return first_successor;
}

inline void TaskSystem::submit(TaskQueue & queue, Task * t) {
	++queue.added;
	for (Dependency & dep : t->dependencies) {
		if (dep.version > dep.handle->version.load()) {
			dep.handle->enqueue(t);
			return;
		}
	}
	queue.enqueue(t);
}

inline void TaskSystem::submit(TaskQueue & queue, std::function<void(Context)> fn) {
	++queue.added;
	Task * t = new Task();
	t->fn = fn;
	t->queue = &queue;
	queue.enqueue(t);
}

inline void TaskSystem::submit(TaskQueue & queue, std::initializer_list<void *> inputList, std::initializer_list<void *> outputList, std::function<void(Context)> fn) {
	++queue.added;
	Task * t = new Task();
	t->fn = fn;
	t->queue = &queue;
	Handle * wait_for = nullptr;
	for (void * ptr : inputList) {
		Handle * h = handle_map.get(ptr);
		const int curr = h->version.load();
		const int required = h->schedule_read();
		t->dependencies.push_back(Dependency(h, required));
		if (wait_for == nullptr && curr < required) {
			wait_for = h;
		}
	}
	for (void * ptr : outputList) {
		Handle * h = handle_map.get(ptr);
		const int curr = h->version.load();
		const int required = h->schedule_write();
		t->dependencies.push_back(Dependency(h, required));
		if (wait_for == nullptr && curr < required) {
			wait_for = h;
		}
	}
	if (wait_for == nullptr) {
		queue.enqueue(t);
	}
	else {
		wait_for->enqueue(t);
	}
}

inline void TaskSystem::submit(TaskQueue & queue, std::initializer_list<Handle *> inputList, std::initializer_list<Handle *> outputList, std::function<void(Context)> fn) {
	++queue.added;
	Task * t = new Task();
	t->fn = fn;
	t->queue = &queue;
	Handle * wait_for = nullptr;
	for (Handle * handle : inputList) {
		const int curr = handle->version.load();
		const int required = handle->schedule_read();
		t->dependencies.push_back(Dependency(handle, required));
		if (wait_for == nullptr && curr < required) {
			wait_for = handle;
		}
	}
	for (Handle * handle : outputList) {
		const int curr = handle->version.load();
		const int required = handle->schedule_write();
		t->dependencies.push_back(Dependency(handle, required));
		if (wait_for == nullptr && curr < required) {
			wait_for = handle;
		}
	}
	if (wait_for == nullptr) {
		queue.enqueue(t);
	}
	else {
		wait_for->enqueue(t);
	}
}

inline void SplitTask::subtask(TaskQueue & queue, std::function<void(Context)> fn) {
	++queue.added;
	Task * t = new Task();
	t->fn = fn;
	t->queue = &queue;
	t->dependencies.push_back(Dependency(handle, handle->schedule_read()));
	queue.enqueue(t);
}

inline void SplitTask::subtask(TaskQueue & queue, std::initializer_list<void *> inputList, std::initializer_list<void *> outputList, std::function<void(Context)> fn) {
	++queue.added;
	Task * t = new Task();
	t->fn = fn;
	t->queue = &queue;
	t->dependencies.push_back(Dependency(handle, handle->schedule_read()));
	Handle * wait_for = nullptr;
	for (void * ptr : inputList) {
		Handle * h = task_system.handle_map.get(ptr);
		const int curr = h->version.load();
		const int required = h->schedule_read();
		t->dependencies.push_back(Dependency(h, required));
		if (wait_for == nullptr && curr < required) {
			wait_for = h;
		}
	}
	for (void * ptr : outputList) {
		Handle * h = task_system.handle_map.get(ptr);
		const int curr = h->version.load();
		const int required = h->schedule_write();
		t->dependencies.push_back(Dependency(h, required));
		if (wait_for == nullptr && curr < required) {
			wait_for = h;
		}
	}
	if (wait_for == nullptr) {
		queue.enqueue(t);
	}
	else {
		wait_for->enqueue(t);
	}
}

inline void SplitTask::subtask(TaskQueue & queue, std::initializer_list<Handle *> inputList, std::initializer_list<Handle *> outputList, std::function<void(Context)> fn) {
	++queue.added;
	Task * t = new Task();
	t->fn = fn;
	t->queue = &queue;
	t->dependencies.push_back(Dependency(handle, handle->schedule_read()));
	Handle * wait_for = nullptr;
	for (Handle * handle : inputList) {
		const int curr = handle->version.load();
		const int required = handle->schedule_read();
		t->dependencies.push_back(Dependency(handle, required));
		if (wait_for == nullptr && curr < required) {
			wait_for = handle;
		}
	}
	for (Handle * handle : outputList) {
		const int curr = handle->version.load();
		const int required = handle->schedule_write();
		t->dependencies.push_back(Dependency(handle, required));
		if (wait_for == nullptr && curr < required) {
			wait_for = handle;
		}
	}
	if (wait_for == nullptr) {
		queue.enqueue(t);
	}
	else {
		wait_for->enqueue(t);
	}
}

inline void TaskSystem::run(int num_workers) {
	// worker threads. runs only tasks from the ts.shared queue
	std::vector<std::thread> workers;
	for (int i = 0; i < num_workers; ++i) {
		workers.push_back(std::thread([this] {
			do {
				Task * t = shared.fetch();
				while (t != nullptr) {
					t = shared.run_task(t, true);
					if (t == nullptr) {
						t = shared.fetch();
					}
				}
			} while (!shared.wait() || !main.wait());
		}));
	}

	// main thread.

	do {
		Task * t = main.fetch();
		while (t != nullptr) {
			t = main.run_task(t, true);
			if (t == nullptr) {
				t = main.fetch();
			}
		}

		t = shared.fetch();
		if (t != nullptr) {
			shared.run_task(t, false);
			continue;
		}
	} while (!main.wait() || !shared.wait());

	// wait for all threads to finish
	for (int i = 0; i < workers.size(); ++i) {
		workers[i].join();
	}
}
