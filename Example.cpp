#include "TaskSystem.h"
#include <iostream>
#include <chrono>

int main() {

	TaskSystem ts;

	const int numBlocks = 10;

	std::vector<Handle> h(numBlocks * numBlocks);

	for (size_t j = 0; j < numBlocks; j++) {
		for (size_t k = 0; k < j; k++) {
			for (size_t i = j + 1; i < numBlocks; i++) {
				// A[i,j] = A[i,j] - A[i,k] * (A[j,k])^t
				ts.submit(ts.shared, {&h[i * numBlocks + k], &h[j * numBlocks + k]}, {&h[i * numBlocks + j]}, [i, j, k](Context) {
					std::cout << std::this_thread::get_id() << " gemm " << i << "," << j << "," << k << std::endl;
					std::this_thread::sleep_for(std::chrono::milliseconds(100));
				});
			}
		}
		for (size_t i = 0; i < j; i++) {
			// A[j,j] = A[j,j] - A[j,i] * (A[j,i])^t
			ts.submit(ts.shared, {&h[j * numBlocks + i] }, {&h[j * numBlocks + j]}, [i, j](Context) {
				std::cout << std::this_thread::get_id() << " syrk " << i << "," << j << std::endl;
				std::this_thread::sleep_for(std::chrono::milliseconds(100));
			});
		}

		// Cholesky Factorization of A[j,j]. Constrain to main thread, for demonstrating.
		ts.submit(ts.main, {}, {&h[j * numBlocks + j]}, [j](Context) {
			std::cout << std::this_thread::get_id() << " potrf " << j << std::endl;
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		});

		for (size_t i = j + 1; i < numBlocks; i++) {
			// A[i,j] <- A[i,j] = X * (A[j,j])^t
			ts.submit(ts.shared, {&h[j * numBlocks + j]}, {&h[i * numBlocks + j]}, [i, j](Context) {
				std::cout << std::this_thread::get_id() << " trsm " << i << "," << j << std::endl;
				std::this_thread::sleep_for(std::chrono::milliseconds(100));
			});
		}
	}

	const int num_workers = 4;
	ts.run(num_workers);

	return 0;
}
