#include "TaskSystem.h"
#include <iostream>

int main() {

	TaskSystem ts;

	// submit tasks (must be done in serial before any task is executed)

	int a = 2;
	int b = 3;
	int c = 0;
	int d = 0;
	// read a and b, write to c
	ts.submit(ts.shared, { &a, &b }, { &c }, [&a, &b, &c]() { c = a + b; std::cout << std::this_thread::get_id() << " c=" << c << std::endl; });
	// read a and b, write to d
	ts.submit(ts.shared, { &a, &b }, { &d }, [&a, &b, &d]() { d = a + b; std::cout << std::this_thread::get_id() << " d=" << d << std::endl; });
	// submit to the 'main thread' queue
	ts.submit(ts.main, { &c, &d }, { &a }, [&a, &c, &d]() { a = c + d; std::cout << std::this_thread::get_id() << " a=" << a << std::endl; });

	ts.submit(ts.shared, { &a, &b }, { &d }, [&a, &b, &d]() { d = a + b; std::cout << std::this_thread::get_id() << " d=" << d << std::endl; });
	ts.submit(ts.shared, { &a, &c }, { &d }, [&a, &c, &d]() { d += a + c; std::cout << std::this_thread::get_id() << " d=" << d << std::endl; });

	// worker threads. runs only tasks from the ts.shared queue
	std::vector< std::thread > workers;
	for (int i = 0; i < 4; ++i) {
		workers.push_back(std::thread([&ts] {
			TaskExecutor te(ts.shared);
			te.run_until_done();
		}));
	}

	// main thread. runs only tasks from the ts.main queue
	TaskExecutor mainThread(ts.main);
	mainThread.run_until_done();

	// wait for all threads to finish
	for (int i = 0; i < workers.size(); ++i) {
		workers[i].join();
	}

	std::cout << std::this_thread::get_id() << " " << a << " " << b << " " << c << " " << d << std::endl;
	return 0;
}
