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
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    std::vector<std::thread> threads;
    std::atomic<int> task_id(0); // current task id, must use value to inital

    // use for loop to create 'num_threads' threads.
    for (int i = 0; i < num_threads; i++) {
        // 用于线程中自动获取下个任务并执行
        // 原子操作取task_id模拟线程取任务执行
        auto func = [&, i]() {
            while(true) {
                int cur_task = task_id++;
                if (cur_task >= num_total_tasks) break;
                // printf("thread %d run task %d\n", i, cur_task);
                runnable->runTask(cur_task, num_total_tasks);
            }
        };

        threads.emplace_back(func); // auto convert to thread
    }

    for (auto &thread : threads) {
        thread.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    task_counter = 0;
    this->num_threads = num_threads;

    // std::vector<std::thread> threads_pool;
    // std::queue<std::function<void()>> task_queue;

    // 向线程池中添加线程，执行取任务执行函数
    // 其中取任务要在队列上加锁防止竞争

    for (int i = 0; i < num_threads; i++) {
        threads_pool.emplace_back([&]() { // use ref to access queue and lock
            // check
            // get queue mutex
            // no task than release mutex
            // keep check
            while(true) { // 对于竞争条件不要放在while循环变量中
                if (done.load()) break;
                std::unique_lock<std::mutex> lock(mtx);
                if (!task_queue.empty()) {
                    auto task = task_queue.front();
                    task_queue.pop();
                    lock.unlock(); // finish to use queue, let other threads to use
                    
                    // run the task
                    assert(task);
                    task();

                    task_counter++;
                }
            }
        });
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    // update pool status to done
    done = true;

    // join to wait all threads finish
    for (auto &t: threads_pool) {
        t.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    task_counter = 0;

    // 向线程池任务列表添加task
    for (int i = 0; i < num_total_tasks; i++) {
        std::lock_guard<std::mutex> lock(mtx);
        task_queue.emplace([=]() {
            runnable->runTask(i, num_total_tasks);
        });
    }

    // wait for finish
    while(true) {
        // atomic int
        if (task_counter == num_total_tasks) {
            break;
        }
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    // 初始化基本变量并添加线程
    num_threads_ = num_threads;

    for (int i = 0; i < num_threads; i++) {
        // 对于成员函数需如此传入函数指针和对应类型的对象
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

    // done and notifyAll?
    done = true;
    cv_worker_.notify_all();

    for(auto &t: thread_pool_) {
        t.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::threadRun() {
    // 向线程池中添加线程，执行取任务执行函数
    // 其中取任务要在队列上加锁防止竞争
    while(true) {
        // 同时管理两种情况，因为当所有bulk tasks完成后，线程重新进入等待任务
        // 但此时不会再有任务，只会有析构的设置done并结束线程
        // 如此在此处进行判断队列非空或是否done
        std::unique_lock<std::mutex> worker_lock(worker_mtx_);
        cv_worker_.wait(worker_lock, [this]() {
            return cur_task_.load() < total_num_tasks_ || done.load();
        });

        // automatic get lock again & not empty queue

        if (done.load()) break; // end this thread

        // get cur_task id
        int task_id = cur_task_.fetch_add(1);
        worker_lock.unlock(); // finish to use virtual queue, let other threads to use
        
        runnable_->runTask(task_id, total_num_tasks_);

        // 最后完成本批任务的线程通知run线程
        if (finished_tasks_.fetch_add(1) == total_num_tasks_ - 1) {
            cv_finished_.notify_all();
        }
    }
}


void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // 初始化本批tasks的state
    total_num_tasks_ = num_total_tasks;
    cur_task_ = 0;
    finished_tasks_ = 0;
    runnable_ = runnable;

    // printf("this batch has total %d tasks\n", num_total_tasks);

    // 改为虚拟队列(用cur_task取任务)
    // 成员变量初始化完毕即相当于任务全部放入

    cv_worker_.notify_all();

    // 检查此批任务是否全部完成
    // 需要最后一个线程通知
    std::unique_lock<std::mutex> finished_lock(finished_mtx_);
    cv_finished_.wait(finished_lock, [this]() {
        return finished_tasks_ == total_num_tasks_;
    });
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
