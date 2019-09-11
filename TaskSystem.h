#pragma once
#include <atomic>
#include <vector>
#include <functional>
#include <mutex>
#include <iostream>
#include <unordered_map>
#include <condition_variable>

struct task;

struct handle {
	std::atomic<int> version{ 0 };
	int next_read = 0;
	int next_write = 0;
	int queueIndex = 0;
	std::vector<task *> queue;
	std::mutex mutex;

	int schedule_read();
	int schedule_write();
	void release() { ++version; }
	void enqueue(task * task);
	void enqueue_unsafe(task * task);
	task * dequeue();
};

struct dependency {
	handle * h;
	int version;
	dependency(handle * h, int version) : h(h), version(version) {}
};

struct TaskQueue;

struct task {
	std::vector<dependency> dependencies;
	std::function<void()> fn;
	TaskQueue * queue = nullptr;
};

inline int handle::schedule_read() {
	++next_write;
	return next_read;
}

inline int handle::schedule_write() {
	const int next = next_write;
	++next_write;
	next_read = next_write;
	return next;
}

inline void handle::enqueue(task * task) {
	std::lock_guard<std::mutex> guard(mutex);
	enqueue_unsafe(task);
}

inline void handle::enqueue_unsafe(task * task) {
	queue.push_back(task);
}

inline task * handle::dequeue() {
	std::lock_guard<std::mutex> guard(mutex);
	if (queueIndex >= queue.size()) {
		return nullptr;
	}
	task * t = queue[queueIndex];
	for (int i = 0; i < t->dependencies.size(); ++i) {
		if (t->dependencies[i].h == this) {
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

struct TaskQueue {
	std::vector< task * > tasks;
	std::mutex mutex;
	std::condition_variable condVar;
	std::atomic<int> next = 0;
	std::atomic<int> executed = 0;
	int added = 0;

	void enqueue(task * t) {
		{
			std::lock_guard<std::mutex> guard(mutex);
			tasks.push_back(t);
		}
		condVar.notify_one();
	}

	void enqueue_unsafe(task * t) {
		tasks.push_back(t);
	}

	bool wait_for_done() {
		if (added == executed.load()) {
			return true;
		}
		std::unique_lock<std::mutex> guard(mutex);
		condVar.wait(guard);
		return added == executed.load();
	}

	task * fetch() {
		std::lock_guard<std::mutex> guard(mutex);
		if (next.load() >= tasks.size()) {
			tasks.clear();
			next = 0;
			return nullptr;
		}
		return tasks[next++];
	}
};

struct TaskHandleMap {
	std::unordered_map< void *, handle *> handle_map;

	~TaskHandleMap() {
		for (auto & it : handle_map) {
			delete it.second;
		}
		handle_map.clear();
	}

	handle * get(void * ptr) {
		handle *& h = handle_map[ptr];
		if (h == nullptr) {
			h = new handle();
		}
		return h;
	}

};

struct TaskSystem {

	TaskQueue main;
	TaskQueue shared;
	TaskHandleMap handle_map;

	void submit(TaskQueue & queue, std::initializer_list<void *> inputList, std::initializer_list<void *> outputList, std::function<void()> fn) {
		task * t = new task();
		t->fn = fn;
		t->queue = &queue;
		handle * wait_for = nullptr;
		for (void * ptr : inputList) {
			handle * h = handle_map.get(ptr);	// not thread safe
			const int curr = h->version.load();
			const int required = h->schedule_read();
			t->dependencies.push_back(dependency(h, required));
			if (wait_for == nullptr && curr < required) {
				wait_for = h;
			}
		}
		for (void * ptr : outputList) {
			handle * h = handle_map.get(ptr);
			const int curr = h->version.load();
			const int required = h->schedule_write();
			t->dependencies.push_back(dependency(h, required));
			if (wait_for == nullptr && curr < required) {
				wait_for = h;
			}
		}
		if (wait_for == nullptr) {
			queue.enqueue_unsafe(t);
		}
		else {
			wait_for->enqueue_unsafe(t);
		}
		++queue.added;
	}
};


struct TaskExecutor {
	TaskQueue & queue;

	TaskExecutor(TaskQueue & queue) : queue(queue) {}

	task * run_task(task * t) {
		t->fn();

		task * first_successor = nullptr;
		for (dependency & d : t->dependencies) {
			d.h->release();
			task * successor = d.h->dequeue();
			if (successor != nullptr) {

				TaskQueue * target_queue = successor->queue;
				if (target_queue == nullptr) {
					target_queue = &queue;
				}

				if (first_successor == nullptr && target_queue == &queue) {
					first_successor = successor;
				}
				else {
					target_queue->enqueue(successor);
				}
			}
		}

		if (++queue.executed == queue.added) {
			queue.condVar.notify_all();
		}

		delete t;
		return first_successor;
	}

	void run_until_done() {
		do {
			task * t = queue.fetch();

			while (t != nullptr) {
				t = run_task(t);
				if (t == nullptr) {
					t = queue.fetch();
				}
			}
		} while (!queue.wait_for_done());
	}
};
