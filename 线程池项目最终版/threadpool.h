#ifndef THREADPOOL_H
#define THREADPOOL_H
#include<vector>
#include<queue>
#include<memory>
#include<atomic>
#include<mutex>
#include<condition_variable>
#include<functional>
#include<thread>
#include<iostream>
#include<unordered_map>
#include<future>
#include <chrono>
const int TASK_MAX_THRESHOLD = 2;
const int THREAD_MAX_THRESHOLD = 1024;
const int THREAD_MAX_IDLE_TIME = 6;

/*
example:
ThreadPool pool;
pool.start(6);
pool.submitTask(std::make_shared<MyTask>());
class MyTask :public Task{
public:
	void run() {

	}
};
*/


//线程池支持的模式
enum class PoolMode {
	MODE_FIXED,//固定数量的线程
	MODE_CACHED,//线程数量可动态增长
};

//线程类型

class Thread {
public:
	//线程函数对象类型
	using ThreadFunc = std::function<void(int)>;
	//构造
	Thread(ThreadFunc func)
		:func_(func)
		, threadId_(generatedId_++)
	{
	}
	//析构
	~Thread() = default;
	//启动线程
	void start() {
		//创建一个线程来执行一个线程函数
		std::thread t(func_, threadId_);//
		t.detach();//设置分离线程“这个线程以后我不管了，让它自己去后台运行吧。”
	}
	//获取线程id
	int getId()const {
		return threadId_;
	}
private:
	ThreadFunc func_;
	static int generatedId_;
	int threadId_;//保存线程id

};
int Thread::generatedId_ = 0;

//线程池类型
class ThreadPool {
public:
	//线程池构造
	ThreadPool()
		:initThreadSize_(0)
		, taskSize_(0)
		, idleThreadSize_(0)
		, curThreadSize_(0)
		, taskQueMaxThreshHold_(TASK_MAX_THRESHOLD)
		, threadSizeThreshHold_(THREAD_MAX_THRESHOLD)
		, poolMode_(PoolMode::MODE_FIXED)
		, isPoolRunning_(false)

	{}

	//线程池析构
	//线程池析构
	~ThreadPool()
	{
		isPoolRunning_ = false;
		//等待线程池里面的所有的线程返回  有两种状态：阻塞&正在执行任务中
		std::unique_lock<std::mutex> lock(taskQueMtx_);
		notEmpty_.notify_all();
		exitCond_.wait(lock, [&]()->bool {return threads_.size() == 0; });
	}

	//设置线程池工作模式
	void setMode(PoolMode mode) {
		if (checkRunningState())
			return;
		poolMode_ = mode;
	}

	//设置task任务队列阈值
	void setTaskQueMaxThreshHold(int threshhold) {
		if (checkRunningState())
			return;
		taskQueMaxThreshHold_ = threshhold;
	}

	//设置线程池cached模式下线程阈值
	void setThreadSizeThreshHold(int threshhold) {
		if (checkRunningState())
			return;
		if (poolMode_ == PoolMode::MODE_CACHED) {
			threadSizeThreshHold_ = threshhold;
		}
	}

	//给线程池提交任务
	//使用可变参模板编程，让submitTask可以接受任意任务函数和任意数量的参数
	template<typename Func,typename... Args>
	auto submitTask(Func&& func,Args&&... args)-> std::future<decltype(func(args...))>
	{
		using RType = decltype(func(args...));
		auto task = std::make_shared<std::packaged_task<RType()>>(
			std::bind(std::forward<Func>(func), std::forward<Args>(args)...)
			);
		std::future<RType> result = task->get_future();

		//获取锁
		std::unique_lock<std::mutex> lock(taskQueMtx_);
		//用户提交任务，最长不能阻塞1s，否则判断提交任务失败，返回
		if (!notFull_.wait_for(lock, std::chrono::seconds(1),
			[&]()->bool {return taskQue_.size() < (size_t)taskQueMaxThreshHold_; })) {
			//表示notFull_等待超过1s，条件依然没有满足
			std::cerr << "task queue is full, submit task fail" << std::endl;
			auto task = std::make_shared<std::packaged_task<RType()>>(
				[]()->RType {return RType(); });
			(*task)();
			return task->get_future();
		}

		//如果有空余，把任务放入任务队列
		//taskQue_.emplace(sp);
		taskQue_.emplace([task]() {(*task)(); });

		taskSize_++;
		//因为新放了任务，任务队列不空，在notEmpty_上进行通知,赶快分配线程执行任务
		notEmpty_.notify_all();

		//MODE_CACHED模式(小且快的任务)需要根据任务线程的数量，判读是否需要新增新的线程
		if (poolMode_ == PoolMode::MODE_CACHED
			&&taskSize_ > idleThreadSize_
			&&curThreadSize_ < threadSizeThreshHold_) {

			std::cout << "create new thread..." << std::endl;
			//创建新线程
			auto ptr = std::make_unique<Thread>(std::bind(&ThreadPool::threadFunc, this, std::placeholders::_1));
			int threadId = ptr->getId();
			threads_.emplace(threadId, std::move(ptr));

			threads_[threadId]->start();   // 启动
			curThreadSize_++;              // 计数
			idleThreadSize_++;             // 空闲线程+1
			//threads_.emplace_back(std::move(ptr));
		}

		//返回任务的Result对象
		return result;
	}


	//开启线程池
	void start(int initThreadSize = std::thread::hardware_concurrency()) {
		//设置线程池的运行状态
		isPoolRunning_ = true;


		//记录初始线程数量
		initThreadSize_ = initThreadSize;
		curThreadSize_ = initThreadSize;
		//创建线程对象
		for (int i = 0; i < initThreadSize_; i++)
		{
			//创建thread对象的时候，把线程函数给到thread线程对象
			auto ptr = std::make_unique<Thread>(std::bind(&ThreadPool::threadFunc, this, std::placeholders::_1));
			int threadId = ptr->getId();
			threads_.emplace(threadId, std::move(ptr));
			threads_[threadId]->start();

			idleThreadSize_++;
		}
	}

	ThreadPool(const ThreadPool&) = delete;
	ThreadPool& operator=(const ThreadPool&) = delete;
private:
	//定义线程函数
	void threadFunc(int threadid) {
		auto lastTime = std::chrono::high_resolution_clock().now();
		for (;;)
		{
			Task task;
			{
				//先获取锁
				std::unique_lock<std::mutex> lock(taskQueMtx_);
				std::cout << "tid:" << std::this_thread::get_id() << "尝试获取任务..." << std::endl;

				//MODE_CACHED模式下可能创建很多线程，空闲线程时间超过60s，把它回收掉
				//当前时间 - 上次线程执行的时间 > 60s
				//每秒返回一次   怎么区分：超时返回？还是任务执行返回
				while (taskQue_.size() == 0)
				{
					if (!isPoolRunning_)
					{
						threads_.erase(threadid);
						std::cout << "threadid:" << std::this_thread::get_id() << "exit" << std::endl;
						exitCond_.notify_all();
						return;
					}

					if (poolMode_ == PoolMode::MODE_CACHED)
					{
						if (std::cv_status::timeout == notEmpty_.wait_for(lock, std::chrono::seconds(1)))
						{
							auto now = std::chrono::high_resolution_clock().now();
							auto dur = std::chrono::duration_cast<std::chrono::seconds>(now - lastTime);
							if (dur.count() >= THREAD_MAX_IDLE_TIME && curThreadSize_ > initThreadSize_) {
								//开始回收当前线程
								threads_.erase(threadid);
								curThreadSize_--;
								idleThreadSize_--;
								std::cout << "threadid:" << std::this_thread::get_id() << "exit" << std::endl;
								return;
							}
						}
					}
					else
					{
						//等待notEmpty_条件
						notEmpty_.wait(lock);
					}

				}

				idleThreadSize_--;

				std::cout << "tid:" << std::this_thread::get_id() << "获取任务成功..." << std::endl;
				//从任务队列去一个任务出来
				task = taskQue_.front();
				taskQue_.pop();
				taskSize_--;

				//如果依然有剩余任务，继续通知其他的线程执行任务
				if (taskQue_.size() > 0) {
					notEmpty_.notify_all();
				}
				//取出一个任务，进行通知,通知可以继续生产任务
				notFull_.notify_all();
			}//就应该把锁释放掉

			//当前线程负责执行这个任务
			if (task != nullptr) {
				task();//执行function<void()>
			}
			idleThreadSize_++;
			lastTime = std::chrono::high_resolution_clock().now();//更新线程执行完任务的时间
		}
	}
	//检查pool的运行状态
	bool checkRunningState()const 
	{
		return isPoolRunning_;
	}

private:
	//std::vector<std::unique_ptr<Thread>> threads_;//线程列表
	std::unordered_map<int, std::unique_ptr<Thread>> threads_;//线程列表

	int initThreadSize_;//初始线程数量
	int threadSizeThreshHold_;//线程数量上线阈值
	std::atomic_int curThreadSize_;//记录当前线程的总数量
	std::atomic_int idleThreadSize_;//记录空闲线程的数量

	//Task任务=》函数对象
	using Task = std::function<void()>;
	std::queue<Task> taskQue_;//任务队列
	std::atomic_int taskSize_;//任务的数量
	int taskQueMaxThreshHold_;//任务队列数量上线阈值

	std::mutex taskQueMtx_;//保证任务队列的线程安全
	std::condition_variable notFull_;//任务队列不满
	std::condition_variable notEmpty_;//任务队列不空
	std::condition_variable exitCond_;//等待线程资源全部回收

	PoolMode poolMode_;//当前线程池的工作模式
	std::atomic_bool isPoolRunning_;//表示当前线程池的启动状态

};

#endif