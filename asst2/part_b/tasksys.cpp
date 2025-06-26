#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

void TaskSystemParallelThreadPoolSleeping::threadRun() {
    while(true) {
        // 从就绪队列获取任务，并备份任务信息
        // 旧版中许多变量现在为任务信息的成员变量
        TaskBatchInfo* task_info;
        int cur_task_id;

        if (findAvailableTask(task_info, cur_task_id)) {
            // printf("Task Batch id: %d, Small Task id: %d\n", task_info->id_, cur_task_id);
            task_info->runnable_->runTask(cur_task_id, task_info->num_total_tasks_);
            if (task_info->finished_tasks_.fetch_add(1) == task_info->num_total_tasks_ - 1) {
                taskBatchFinished(task_info->id_);
            }
        } else {
            // wait and sleep
            std::unique_lock<std::mutex> queue_lock(queue_mtx_);
            cv_worker_.wait(queue_lock, [this]() {
                return !ready_queue_.empty() || done_.load();
            });

            if (done_.load()) break;
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::taskBatchFinished(TaskID task_id) {
    auto& task_info = task_map_[task_id];
    task_info->task_state_ = TaskState::Finished;

    // 检查需要我的任务，并更新其剩余前置依赖数量，为0时则该任务可执行并入队
    for(auto& dep_task_id: task_info->dependent_tasks_) {
        auto& dep_task_info = *task_map_[dep_task_id];
        dep_task_info.remaining_deps_count_--;

        if (dep_task_info.remaining_deps_count_.load() <= 0) {
            dep_task_info.task_state_ = TaskState::Ready;
            std::lock_guard<std::mutex> queue_lock(queue_mtx_);
            ready_queue_.push(&dep_task_info);
            cv_worker_.notify_all();
        }
    }

    if (active_batches_.fetch_sub(1) == 1) { // 等价于active_bathes--和判0
        std::unique_lock<std::mutex> sync_lock(sync_mtx_); // 防止丢通知
        cv_sync_.notify_one();
    }
}

bool TaskSystemParallelThreadPoolSleeping::findAvailableTask(TaskBatchInfo*& task_info, int& task_id) {
    std::lock_guard<std::mutex> queue_lock(queue_mtx_);
    
    // 逐个检查队列中的任务并获取可执行的内部task_id，无剩余任务的任务批次从队列删除
    while(!ready_queue_.empty()) {
        task_info = ready_queue_.front();
        // check task
        if (task_info->left_tasks_ <= 0) {
            ready_queue_.pop();
            continue;
        } else {
            task_id = task_info->num_total_tasks_- task_info->left_tasks_.fetch_sub(1);
            return true;
        }
    }
    return false;
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    num_threads_ = num_threads;

    for(int i = 0; i < num_threads_; i++) {
        thread_pool_.emplace_back(&TaskSystemParallelThreadPoolSleeping::threadRun, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    // done and notifyAll
    // 类似于原run中的通知丢失，析构时，工作线程也可能丢失结束的通知
    {
        std::lock_guard<std::mutex> queue_lock(queue_mtx_);
        done_ = true;
        cv_worker_.notify_all();
    }

    for(auto &t: thread_pool_) {
        t.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // 用于串行任务测试
    std::vector<TaskID> no_deps;
    runAsyncWithDeps(runnable, num_total_tasks, no_deps);
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    active_batches_++;

    TaskID new_task_id = next_task_id_++;
    // 提前在map中构造，防止后续将临时对象加入队列
    auto result = task_map_.emplace(
        new_task_id,
        std::unique_ptr<TaskBatchInfo>(
            new TaskBatchInfo{
                new_task_id,
                runnable,
                num_total_tasks,
                static_cast<int>(deps.size())
            }
        )
    );
    auto it = result.first;
    TaskBatchInfo& new_task_info = *it->second;

    for (const TaskID dependency_id : deps) {
        auto& dependency_info = *task_map_[dependency_id];

        // 在加入依赖前获取是否完成的状态，防止多次对deps_count进行修改
        bool is_finished = (dependency_info.task_state_ == TaskState::Finished);
        dependency_info.dependent_tasks_.push_back(new_task_id);
        // 计入已完成的依赖任务，简化处理
        if (is_finished) {
            new_task_info.remaining_deps_count_--;
        }
    }

    // 无前置剩余，则已就绪
    if (new_task_info.remaining_deps_count_.load() <= 0) {
        new_task_info.task_state_ = TaskState::Ready;
        // 加入就绪队列
        std::lock_guard<std::mutex> queue_lock(queue_mtx_);
        ready_queue_.push(&new_task_info);
        // // 暂定在此便开始notify
        // cv_worker_.notify_all();
        // map非线程安全
    }

    return new_task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    cv_worker_.notify_all();
    std::unique_lock<std::mutex> sync_lock(sync_mtx_);
    cv_sync_.wait(sync_lock, [this]() {
        return active_batches_.load() == 0; // 活跃批次任务全部执行完毕
    });
    // 对于可能的多批次，active_batches_在这里已清零，无需手动进行
}
