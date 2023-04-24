#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <ctype.h>
#include <stdatomic.h>

#define ERR_MSG(code,msg) {						\
		fprintf(stderr, msg"\n");				\
		return (code);						\
	}								\

typedef struct thread_data {
	long int ncount;
	long int acount;
	long int ucount;
	long int fsize;
	long int workload;
	atomic_long* pos;
	char* fname;
	int buff_size;
	int status;
	int is_utf8;
}TData;

enum err_codes {SUCCESS, INPUT_INVAL, FILE_INACC, FAIL_ALLOC, NO_THREAD_ALIVE};

long int get_file_size(char *);
/*
 * returns size of the appointed file
 * operates by opening the file, moving the the end of it and reading
 * the position there
 */

void *parse_count(void *);
/*
 * function given to threads, returns a report code and stores results in
 * appointed structure
 * each thread first individually opens the file, then starts checking on
 * shared progress and assigning itself portions of the file untill no more is
 * left to parse.
 */

long int adjust_workload(long, int);
/*
 * returns a new value for workload
 * used to ensure workloads can be evenly divided into buffer-sized chunks
 * does this by creating two new possible workload values and returning
 * whichever deviates the least from the original value
 */

int main(int argc, char *argv[])
{
	long int fsize;
	int num_of_threads;
	long int workload;
	int buff_size;
	atomic_long pos = 0;
	long int ncount_final = 0;
	long int acount_final = 0;
	long int ucount_final = 0;
	int is_utf8 = 1;
	int threads_running = 0;

	if (strlen(argv[1]) >= 256 || argc == 1)
		ERR_MSG(-INPUT_INVAL, "File name too long or missing.");

	fsize = get_file_size(argv[1]);
	if (fsize < 0)
		ERR_MSG(fsize, "File inaccesible.");
	if (argc != 5)
		ERR_MSG(-INPUT_INVAL,"Parameters missing.");

	num_of_threads = strtol(argv[2], NULL, 0);
	workload = strtol(argv[3], NULL, 0);
	buff_size = strtol(argv[4], NULL, 0);

	if (workload > fsize)
		ERR_MSG(-INPUT_INVAL, "Workload exceeds file size.");
	if (buff_size > workload)
		ERR_MSG(-INPUT_INVAL, "Buffer exceeds given workload.");
	if (workload <= 0 || buff_size <= 0 || num_of_threads <= 0)
		ERR_MSG(-INPUT_INVAL, "Parameters must be positive.");

	if (workload % buff_size != 0) {
		workload = adjust_workload(workload, buff_size);
		printf("Buffer does not divide the workload evenly."
		       "\nNew workload: %ld\n", workload);
	}

	pthread_t *t_id = malloc(sizeof(pthread_t) * num_of_threads);
	if (!t_id)
		ERR_MSG(-FAIL_ALLOC, "Failure to allocate memory.");
	TData *tdata = malloc((sizeof(TData) * num_of_threads));
	if (!tdata)
		ERR_MSG(-FAIL_ALLOC, "Failure to allocate memory.");

	//creating and preparing each thread
	for(int i = 0; i < num_of_threads; i++) {
		tdata[i].workload = workload;
		tdata[i].fname = argv[1];
		tdata[i].fsize = fsize;
		tdata[i].pos = &pos;
		tdata[i].buff_size = buff_size;
		if (pthread_create(&t_id[i], NULL, parse_count,
		    				  (void *)&tdata[i]))	//?
			fprintf(stderr, "Failure to start a thread.\n");
		else
			threads_running++;

	}
	if (threads_running == 0)
		ERR_MSG(-NO_THREAD_ALIVE, "Failure to start any threads.")
	else if (threads_running != num_of_threads) {
		fprintf(stderr, "Unable to create requested number of threads."
		                "Threads created: %d/%d\n", threads_running,
							    num_of_threads);
	}

	//waiting for threads and gathering results
	for(int i = 0; i < num_of_threads; i++) {
		pthread_join(t_id[i], NULL);
		if (tdata[i].status) {
			threads_running--;
			fprintf(stderr, "Thread exited with error code: %d\n",
				tdata[i].status);
		}
		ncount_final += tdata[i].ncount;
		acount_final += tdata[i].acount;
		ucount_final += tdata[i].ucount;
		is_utf8 = is_utf8 & tdata[i].is_utf8;
	}
	if (!threads_running)
		ERR_MSG(-NO_THREAD_ALIVE, "All threads failed to complete.");

	char* answers[2] = {"isn't UTF8 compliant.", "is UTF8 compliant."};
	printf(
	"Total Size:                       %-20ld\n"
	"Newlines:                         %-20ld\n"
	"Alphanumeric characters:          %-20ld\n"
	"\n%s %s\n",
	fsize,
	ncount_final,
	acount_final,
	argv[1], answers[is_utf8]);
	if (is_utf8)
		printf("UTF8 characters:                  %-20ld\n", ucount_final);

	free(t_id);
	free(tdata);
	return SUCCESS;
}

long int get_file_size(char *fname) {
	FILE *handle = fopen(fname, "rb");
	if (!handle) {
		return -FILE_INACC;
	}
	fseek(handle, 0L, SEEK_END);
	long int fsize = ftell(handle);
	fclose(handle);
	return fsize;
}

void *parse_count(void *tdata) {
	long curr_pos;
	//creating copies of relevant variables to limit dereferencing
	long workload = ((TData*)tdata)->workload;
	long fsize = ((TData*)tdata)->fsize;
	int buff_size = ((TData*)tdata)->buff_size;
	long acount = 0;
	long ncount = 0;
	long ucount = 0;
	int is_utf8 = 1;

	char *fblock = malloc(sizeof(char) * buff_size);
	if (!fblock) {
		fprintf(stderr, "Thread unable to allocate buff memory.\n");
		((TData*)tdata)->status = -FAIL_ALLOC;
		pthread_exit(NULL);
	}

	FILE *handle = fopen(((TData*)tdata)->fname, "rb");
	if (!handle) {
		fprintf(stderr, "Thread unable to open file, terminating.\n");
		((TData*)tdata)->status = -FILE_INACC;
		pthread_exit(NULL);
	}
	//there is no threat of busy-waiting
	while(1) {
		/*
		 * thread obtains the current position in file and moves the
		 * position pointer ahead to "allocate" itself a portion of the
		 * file, avoiding overlap
		 */
		curr_pos = atomic_fetch_add(((TData*)tdata)->pos, workload);
		if (curr_pos >= fsize) break;
		fseek(handle, curr_pos, SEEK_SET);

		/*
		 * checking if eof is in proximity to avoid reading further
		 * if thread determines it is in the last section of the file
		 * (last workload) it will adjust the workload to fit in that
		 * section. this will still leave a piece of the file unread
		 */
		if ((fsize - curr_pos) < workload) workload = fsize % workload;

		for(int i = 0; i < workload / buff_size; i++) {
			fread(fblock, sizeof(*fblock), buff_size, handle);
			for (int j = 0; j < buff_size; j++) {
				if (fblock[j] == '\n') ncount++;
				if (isalnum(fblock[j])) acount++;
				if ((fblock[j] & 0xc0) != 0x80) ucount++;
				if ((fblock[j] & 0xf8) == 0xf8) is_utf8  = 0;
		}
		}
		/*
		 * here the last piece of the file is read. fread below and the
		 * loop proceeding it will only do anything if workload has been
		 * changed (which happens only in the last segment) and the file
		 * cannot be evenly divided into buffer-sized chunks (in which
		 * case the previous loop would read the entire file regardless)
		 */
		fread(fblock, sizeof(*fblock), workload % buff_size, handle);
		for (int i = 0; i < workload % buff_size; i++) {
			if (fblock[i] == '\n') ncount++;
			//if (isalnum(fblock[i])) acount++;
			if (
				(fblock[i] >= '0' && fblock[i] <= '9') ||
				(fblock[i] >= 'A' && fblock[i] <= 'Z') ||
				(fblock[i] >= 'a' && fblock[i] <= 'z')
			) acount++;
			if ((fblock[i] & 0xc0) != 0x80) ucount++;
			if ((fblock[i] & 0xf8) == 0xf8) is_utf8 = 0;
		}
	}
	((TData*)tdata)->ncount = ncount;
	((TData*)tdata)->acount = acount;
	((TData*)tdata)->ucount = ucount;
	((TData*)tdata)->is_utf8 = is_utf8;

	free(fblock);
	((TData*)tdata)->status = SUCCESS;
	return NULL;
}

long adjust_workload(long workload, int buff_size)
{
	long lower = (workload / buff_size) * buff_size;
	long upper = ((workload / buff_size) + 1 ) * buff_size;
	if (workload - lower > upper - workload)
		return upper;
	else
		return lower;
}
