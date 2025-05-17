#include <stdio.h>
#include <thread>

#include "CycleTimer.h"

typedef struct {
    float x0, x1;
    float y0, y1;
    unsigned int width;
    unsigned int height;
    int maxIterations;
    int* output;
    int threadId;
    int numThreads;
} WorkerArgs;


extern void mandelbrotSerial(
    float x0, float y0, float x1, float y1,
    int width, int height,
    int startRow, int numRows,
    int maxIterations,
    int output[]);

extern void mandelbrotSerialStep(
    float x0, float y0, float x1, float y1,
    int width, int height,
    int startRow, int step,
    int maxIterations,
    int output[]);

//
// workerThreadStart --
//
// Thread entrypoint.
void workerThreadStart(WorkerArgs * const args) {

    // TODO FOR CS149 STUDENTS: Implement the body of the worker
    // thread here. Each thread should make a call to mandelbrotSerial()
    // to compute a part of the output image.  For example, in a
    // program that uses two threads, thread 0 could compute the top
    // half of the image and thread 1 could compute the bottom half.

    // int totalRows = args->height;
    // int baseRows = totalRows / args->numThreads; 
    // int remainRows = totalRows % args->numThreads;

    // int startRow = args->threadId * baseRows + std::min(args->threadId, remainRows); 
    // int numRow = baseRows + (args->threadId < remainRows ? 1 : 0);

    // double startTime = CycleTimer::currentSeconds();
    // // printf("Thread %d processing rows [%d, %d)\n", 
    // //     args->threadId, startRow, startRow + numRow);
    // mandelbrotSerial(args->x0, args->y0, args->x1, args->y1, args->width, args->height,
    //                  startRow, numRow, args->maxIterations, args->output);
    // double endTime = CycleTimer::currentSeconds();

    // printf("[thread %d]:\t\t[%.3f] ms\n", args->threadId, (endTime-startTime)*1000);

    // 将每行按顺序分配给每个进程，以平均化部分行的任务量过大,即对于较大任务量的部分，每个线程均会处理其中的一行
    // 在平均任务的基础上，需考虑空间局部性，即均分为更大的块后再进行分配(其实不用考虑缓存问题，一行1200个元素够了)

    double startTime = CycleTimer::currentSeconds();
    // 由线程id决定从哪一行开始，间隔step由总线程数量决定，其余参数不变
    mandelbrotSerialStep(args->x0, args->y0, args->x1, args->y1,
                         args->width, args->height,
                         args->threadId, args->numThreads, args->maxIterations, args->output);
    double endTime = CycleTimer::currentSeconds();
    printf("[thread %d]:\t\t[%.3f] ms\n", args->threadId, (endTime-startTime)*1000);
}

//
// MandelbrotThread --
//
// Multi-threaded implementation of mandelbrot set image generation.
// Threads of execution are created by spawning std::threads.
void mandelbrotThread(
    int numThreads,
    float x0, float y0, float x1, float y1,
    int width, int height,
    int maxIterations, int output[])
{
    static constexpr int MAX_THREADS = 32;

    if (numThreads > MAX_THREADS)
    {
        fprintf(stderr, "Error: Max allowed threads is %d\n", MAX_THREADS);
        exit(1);
    }

    // Creates thread objects that do not yet represent a thread.
    std::thread workers[MAX_THREADS];
    WorkerArgs args[MAX_THREADS];

    for (int i=0; i<numThreads; i++) {
      
        // TODO FOR CS149 STUDENTS: You may or may not wish to modify
        // the per-thread arguments here.  The code below copies the
        // same arguments for each thread
        args[i].x0 = x0;
        args[i].y0 = y0;
        args[i].x1 = x1;
        args[i].y1 = y1;
        args[i].width = width;
        args[i].height = height;
        args[i].maxIterations = maxIterations;
        args[i].numThreads = numThreads;
        args[i].output = output;
      
        args[i].threadId = i;
    }

    // Spawn the worker threads.  Note that only numThreads-1 std::threads
    // are created and the main application thread is used as a worker
    // as well.
    for (int i=1; i<numThreads; i++) {
        workers[i] = std::thread(workerThreadStart, &args[i]);
    }
    
    workerThreadStart(&args[0]);

    // join worker threads
    for (int i=1; i<numThreads; i++) {
        workers[i].join();
    }
    printf("-------------------------------------\n");
}

