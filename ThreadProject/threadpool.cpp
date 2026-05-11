#include "threadpool.h"



const int TASK_MAX_THRESHOLD = INT32_MAX;
const int THREAD_MAX_THRESHOLD = 1024;
const int THREAD_MAX_IDLE_TIME = 6;
//线程池构造
ThreadPool::ThreadPool()
	:initThreadSize_(0)
	, taskSize_(0)
	, idleThreadSize_(0)
	, curThreadSize_(0)
	, taskQueMaxThreshHold_(TASK_MAX_THRESHOLD)
	, threadSizeThreshHold_(THREAD_MAX_THRESHOLD)
	, poolMode_(PoolMode::MODE_FIXED)
	,isPoolRunning_(false)

{}

//线程池析构
ThreadPool::~ThreadPool()
{
	isPoolRunning_ = false;
	//等待线程池里面的所有的线程返回  有两种状态：阻塞&正在执行任务中
	std::unique_lock<std::mutex> lock(taskQueMtx_);
	notEmpty_.notify_all();
	exitCond_.wait(lock, [&]()->bool {return threads_.size() == 0; });
}

//设置线程池工作模式
void ThreadPool::setMode(PoolMode mode) {
	if (checkRunningState())
		return;
	poolMode_ = mode;
}

//设置task任务队列上限
void ThreadPool::setTaskQueMaxThreshHold(int threshhold) {
	if (checkRunningState())
		return;
	taskQueMaxThreshHold_ = threshhold;
}

//设置线程池cached模式下线程阈值
void ThreadPool::setThreadSizeThreshHold(int threshhold) {
	if (checkRunningState())
		return;
	if (poolMode_ == PoolMode::MODE_CACHED) {
		threadSizeThreshHold_ = threshhold;
	}
}

//给线程池提交任务
Result ThreadPool::submitTask(std::shared_ptr<Task> sp) {
	//获取锁
	std::unique_lock<std::mutex> lock(taskQueMtx_);
	//线程的通信 等待任务队列有空余
	//while (taskQue_.size()== taskQueMaxThreshHold_)
	//{
	//	notFull_.wait(lock);
	//}
	//用户提交任务，最长不能阻塞1s，否则判断提交任务失败，返回
	if (!notFull_.wait_for(lock, std::chrono::seconds(1),
		[&]()->bool {return taskQue_.size() < (size_t)taskQueMaxThreshHold_; })) {
		//表示notFull_等待超过1s，条件依然没有满足
		std::cerr << "task queue is full, submit task fail" << std::endl;
		return Result(sp,false);//
	}
	
	//如果有空余，把任务放入任务队列
	taskQue_.emplace(sp);
	taskSize_++;
	//因为新放了任务，任务队列不空，在notEmpty_上进行通知,赶快分配线程执行任务
	notEmpty_.notify_all();

	//MODE_CACHED模式(小且快的任务)需要根据任务线程的数量，判读是否需要新增新的线程
	if (poolMode_ == PoolMode::MODE_CACHED
		&&taskSize_ > idleThreadSize_
		&&curThreadSize_< threadSizeThreshHold_) {

		std::cout << "create new thread..."  << std::endl;
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
	return Result(sp);
}

//开启线程池
void ThreadPool::start(int initThreadSize) {
	//设置线程池的运行状态
	isPoolRunning_ = true;


	//记录初始线程数量
	initThreadSize_ = initThreadSize;
	curThreadSize_ = initThreadSize;
	//创建线程对象
	for (int i = 0; i < initThreadSize_; i++)
	{
		//创建thread对象的时候，把线程函数给到thread线程对象
		auto ptr = std::make_unique<Thread>(std::bind(&ThreadPool::threadFunc, this,std::placeholders::_1));
		int threadId = ptr->getId();
		threads_.emplace(threadId, std::move(ptr));
		threads_[threadId]->start();

		idleThreadSize_++;
		//threads_.emplace_back(std::move(ptr));

	}
	////启动所有线程
	//for (int i = 0; i < initThreadSize_; i++)
	//{
	//	threads_[i]->start();
	//	idleThreadSize_++;//记录初始空闲线程的数量
	//}
}
//定义线程函数   线程池的所有线程从任务队列里面消费任务
void ThreadPool::threadFunc(int threadid) {
	/*std::cout << "begin threadFunc tid:" << std::this_thread::get_id() << std::endl;
	std::cout << std::this_thread::get_id() << std::endl;
	std::cout << "end threadFubc" << std::this_thread::get_id() << std::endl;*/

	for(;;)
	{
		auto lastTime = std::chrono::high_resolution_clock().now();

		std::shared_ptr<Task> task;
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
				//线程池结束，回收线程资源
				/*if (!isPoolRunning_)
				{
					threads_.erase(threadid);
					std::cout << "threadid:" << std::this_thread::get_id() << "exit" << std::endl;
					exitCond_.notify_all();
					return;
				}*/

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
			//task->run();
			task->exec();
		}
		idleThreadSize_++;
		lastTime = std::chrono::high_resolution_clock().now();//更新线程执行完任务的时间
	}
}
bool ThreadPool::checkRunningState()const {
	return isPoolRunning_;
}

/////////////////////线程方法实现
int Thread::generatedId_ = 0;
Thread::Thread(ThreadFunc func) 
	:func_(func)
	,threadId_(generatedId_++)
{
}
//析构
Thread::~Thread() {

}
//启动线程
void Thread::start(){
	//创建一个线程来执行一个线程函数
	std::thread t(func_,threadId_);//
	t.detach();//设置分离线程“这个线程以后我不管了，让它自己去后台运行吧。”
}
//获取线程id
int Thread::getId()const {
	return threadId_;
}

///////////////////Task方法实现
Task::Task()
	:result_(nullptr)
{}

void Task::exec() {
	if (result_ != nullptr) {
		result_->setVal(run());//这里发生多态调用
	}

}

void Task::setResult(Result* res) {
	result_ = res;
}

////////////////////Result方法的实现
Result::Result(std::shared_ptr<Task> task, bool isValid)
	:isValid_(isValid)
	,task_(task)
{
	task_->setResult(this);
}

Any Result::get() {
	if (!isValid_) {
		return "";
	}
	sem_.wait();//task任务如果没有执行完，这里会阻塞用户的线程
	return std::move(any_);
}

void Result::setVal(Any any) {
	//存储task的返回值
	this->any_ = std::move(any);
	sem_.post();//已经获取的的任务的返回值，增加信号量资源
}