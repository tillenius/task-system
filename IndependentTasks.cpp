#include <stdio.h>
#include <stdlib.h>
#define LOGGING
#include "Logging.h"
#include "d:/proj/tasksystem/tasksystem.h"

#include <Windows.h>
#include <iomanip>

#ifdef LOGGING
#define NUM_ITERATIONS 1
#else
#define NUM_ITERATIONS 5
#endif

LOG_entry LOG_data[LOG_MAX_ENTRIES];
std::atomic<size_t> LOG_ptr{ 0 };

LOG_TimeUnit LOG_start, LOG_mid, LOG_stop;

//////////// logging

char * TIMING_submit;
char * TIMING_execute;
char * TIMING_total;
int TIMING_submit_i;
int TIMING_execute_i;
int TIMING_total_i;
LOG_TimeUnit delay = 2000;

#define LOG_TIMER(LOG_NAME) \
    LOG_TimeUnit start = LOG_getTimeStart(); \
    LOG_TimeUnit stop = start + delay; \
    LOG_TimeUnit curr; \
    while ( (curr = LOG_rdtsc()) < stop); \
    LOG_add(LOG_NAME, start, curr)

void TIMING_init() {
	TIMING_submit = (char *)malloc(20 * 100);
	TIMING_execute = (char *)malloc(20 * 100);
	TIMING_total = (char *)malloc(20 * 100);
}

void TIMING_add(unsigned long long LOG_start, unsigned long long LOG_mid, unsigned long long LOG_stop) {
	TIMING_submit_i += sprintf(&TIMING_submit[TIMING_submit_i], "%llu ", LOG_mid - LOG_start);
	TIMING_execute_i += sprintf(&TIMING_execute[TIMING_execute_i], "%llu ", LOG_stop - LOG_mid);
	TIMING_total_i += sprintf(&TIMING_total[TIMING_total_i], "%llu ", LOG_stop - LOG_start);
}

void TIMING_end(int n) {
	printf("n= %d delay= %llu submit=[ %s] execute=[ %s] total=[ %s]\n",
		n, delay, TIMING_submit, TIMING_execute, TIMING_total);
	free(TIMING_submit);
	free(TIMING_execute);
	free(TIMING_total);
}

#ifdef LOGGING
#define NUM_ITERATIONS 1
#else
#define NUM_ITERATIONS 5
#endif

void test(int n) {

	LOG_start = LOG_getTimeStart();

	TaskSystem ts;

	for (int i = 0; i < n; ++i) {
		LOG_TimeUnit t0 = LOG_getTimeStart();
		ts.submit2(ts.shared, {}, {}, [](Context) { LOG_TIMER("task"); });
		LOG_TimeUnit t1 = LOG_getTimeStart();
		LOG_add("submit", t0, t1);
	}

	LOG_mid = LOG_getTimeStop();

	ts.run(3);

	LOG_stop = LOG_getTimeStop();
}

int main(int argc, char * argv[]) {

	int n = 100000;

	if (argc > 1)
		n = atoi(argv[1]);
	if (argc > 2)
		delay = atoi(argv[2]);

	TIMING_init();

	for (int i = 0; i < NUM_ITERATIONS; ++i) {
		test(n);
		TIMING_add(LOG_start, LOG_mid, LOG_stop);
		LOG_optional_break
	}

	TIMING_end(n);
	LOG_dump("d:\\proj\\perfviewer\\tasksystem.log");

	std::cout << "inc executed...: " << std::right << std::setw(10) << t_inc_executed.load() << "\n";
	std::cout << "queue added....: " << std::right << std::setw(10) << t_queue_added.load() << "\n";
	std::cout << "n_enqueue_start: " << std::right << std::setw(10) << n_enqueue_start << "\n";
	std::cout << "n_enqueue_retry: " << std::right << std::setw(10) << n_enqueue_retry << "\n";
	std::cout << "taskbuffer_stor: " << std::right << std::setw(10) << t_taskbuffer_store << "\n";
	std::cout << "queueidx_store.: " << std::right << std::setw(10) << t_queueidx_store << "\n";
	std::cout << "t_checkdone....: " << std::right << std::setw(10) << t_checkdone << "\n";
	std::cout << "t_fetch........: " << std::right << std::setw(10) << t_fetch << "\n";
	std::cout << "t_maincheckdone: " << std::right << std::setw(10) << t_maincheckdone << "\n";
	std::cout << "t_mainwait.....: " << std::right << std::setw(10) << t_mainwait << "\n";
	
	return 0;
}
