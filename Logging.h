#pragma once

#include <intrin.h>
typedef unsigned __int64  LOG_TimeUnit;
static inline LOG_TimeUnit LOG_getTimeStart() { return __rdtsc(); }
static inline LOG_TimeUnit LOG_rdtsc() { return __rdtsc(); }
static inline LOG_TimeUnit LOG_getTimeStop() { return __rdtsc(); }


#ifdef LOGGING

#include <atomic>
#include <Windows.h>

#define LOG_MAX_ENTRIES 2000000


static inline unsigned long LOG_getThread() {
	return GetCurrentThreadId();
}

typedef struct {
	LOG_TimeUnit start, length;
	unsigned long thread;
	char text[64 - 2 * sizeof(LOG_TimeUnit) - sizeof(unsigned long)];
} LOG_entry;


extern LOG_entry LOG_data[LOG_MAX_ENTRIES];
extern std::atomic<size_t> LOG_ptr;

static inline void LOG_add1(const char * text) {
	LOG_TimeUnit start = LOG_getTimeStart();
	LOG_TimeUnit stop = start;
	size_t i = LOG_ptr++;
	strcpy(LOG_data[i].text, text);
	LOG_data[i].start = start;
	LOG_data[i].length = stop - start;
	LOG_data[i].thread = LOG_getThread();
}

static inline void LOG_add(const char * text, LOG_TimeUnit start, LOG_TimeUnit stop) {
	size_t i = LOG_ptr++;
	strcpy(LOG_data[i].text, text);
	LOG_data[i].start = start;
	LOG_data[i].length = stop - start;
	LOG_data[i].thread = LOG_getThread();
}

static void LOG_dump(const char * filename) {
	size_t i;
	FILE * out = fopen(filename, "w");
	fprintf(out, "LOG 3\n");
	LOG_TimeUnit minimum = LOG_data[0].start;
	size_t num = LOG_ptr.load();
	for (i = 0; i < num; ++i) {
		if (LOG_data[i].start < minimum)
			minimum = LOG_data[i].start;
	}
	for (i = 0; i < num; ++i) {
		fprintf(out, "%lu: %llu %llu %s\n",
			LOG_data[i].thread,
			LOG_data[i].start - minimum,
			LOG_data[i].length,
			LOG_data[i].text);
	}
	fclose(out);
}

#define LOG_optional_break break;

#else
#define LOG_add1(a)
#define LOG_add(a,b,c)
#define LOG_dump(a)
#define LOG_optional_break
#endif
