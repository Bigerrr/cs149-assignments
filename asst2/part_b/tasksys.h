#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <atomic>
#include <unordered_map>
#include <queue>
#include <thread>
#include <vector>
#include <memory>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

enum class TaskState {
    NotReady,
    Ready,
    Running,
    Finished
};

struct TaskBatchInfo {
    TaskID id_;
    IRunnable* runnable_;
    int num_total_tasks_;
    std::vector<TaskID> dependent_tasks_;
    std::atomic<int> remaining_deps_count_;
    TaskState task_state_{TaskState::NotReady};

    std::atomic<int> left_tasks_{-1};
    std::atomic<int> finished_tasks_{0};

    TaskBatchInfo() = default;
    TaskBatchInfo(TaskID id, IRunnable* runnable,
                  int num_total_tasks, int deps_count)
        : id_(id),
          runnable_(runnable),
          num_total_tasks_(num_total_tasks),
          remaining_deps_count_(deps_count),
          left_tasks_(num_total_tasks) {}
};


/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
        void threadRun();
        bool findAvailableTask(TaskBatchInfo*& task_info, int& task_id);
        void taskBatchFinished(TaskID task_id);

    private:
        TaskID next_task_id_{0};
        std::atomic<bool> done_{false};
        std::atomic<int> active_batches_{0};
        int num_threads_;
        std::unordered_map<TaskID, std::unique_ptr<TaskBatchInfo>> task_map_;
        std::queue<TaskBatchInfo*> ready_queue_;

        std::condition_variable cv_worker_;
        std::condition_variable cv_sync_;

        std::mutex queue_mtx_;
        std::mutex sync_mtx_;

        std::vector<std::thread> thread_pool_;
};

#endif
