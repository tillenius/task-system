#include "TaskSystem.h"
#include <iostream>
#include <chrono>

int main() {

	TaskSystem ts;

	const int numBlocks = 10;

	Handle h;

	ts.submit(ts.shared, {}, {&h}, [](Context c) {
		std::cout << "First\n";
		SplitTask split(c);
		split.subtask(c.task_system->shared, [](Context) {
			std::cout << "One\n";
		});
		split.subtask(c.task_system->shared, [](Context) {
			std::cout << "Two\n";
		});
		split.subtask(c.task_system->shared, [](Context) {
			std::cout << "Three\n";
		});
	});
	ts.submit(ts.shared, {&h}, {}, [](Context c) {
		std::cout << "Last\n";
	});

	const int num_workers = 4;
	ts.run(num_workers);

	return 0;
}
