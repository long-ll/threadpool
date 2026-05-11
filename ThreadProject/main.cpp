#include<iostream>
#include<chrono>
#include<thread>
#include"threadpool.h"

using uLong = unsigned long long;

class MyTask :public Task {
public:
	MyTask(int begin, int end)
		:begin_(begin)
		,end_(end)
	{}
	//怎么设计run函数的返回值，可以表示任意的类型
	//Java Python  Object 是所有其他类类型的基类
	//C++17 Any 类型

	//- - 手搓any类型 - -
	Any run() {
		std::cout << "tid:" << std::this_thread::get_id() << "begin!" << std::endl;
		std::this_thread::sleep_for(std::chrono::seconds(5));
		uLong sum = 0;
		for (int i = begin_; i <= end_; i++)
		{
			sum += i;
		}
		std::cout << "tid:" << std::this_thread::get_id() << "end!" << std::endl;

		return sum;
	}
private:
	int begin_;
	int end_;
};

int main() {
	{
		ThreadPool pool;
		pool.setMode(PoolMode::MODE_CACHED);
		pool.start(1);

		Result res1 = pool.submitTask(std::make_shared<MyTask>(1, 100000000));
		Result res2 = pool.submitTask(std::make_shared<MyTask>(100000001, 200000000));
		Result res3 = pool.submitTask(std::make_shared<MyTask>(200000001, 300000000));
		//uLong sum1 = res1.get().cast_<uLong>();

		//std::cout << sum1 << std::endl;
		
	}

	std::cout << "main over!" << std::endl;


#if 0
	{
		ThreadPool pool;
		//用户自己设置线程的工作模式
		pool.setMode(PoolMode::MODE_CACHED);
		pool.start(4);

		//如何设计这里的Result机制呢
		Result res1 = pool.submitTask(std::make_shared<MyTask>(1, 100000000));
		Result res2 = pool.submitTask(std::make_shared<MyTask>(100000001, 200000000));
		Result res3 = pool.submitTask(std::make_shared<MyTask>(200000001, 300000000));
		pool.submitTask(std::make_shared<MyTask>(1, 100000000));
		pool.submitTask(std::make_shared<MyTask>(100000001, 200000000));
		pool.submitTask(std::make_shared<MyTask>(200000001, 300000000));

		uLong sum1 = res1.get().cast_<uLong>();//get返回了一个any类型，怎么转成具体的类型
		uLong sum2 = res2.get().cast_<uLong>();
		uLong sum3 = res3.get().cast_<uLong>();
		std::cout << (sum1 + sum2 + sum3) << std::endl;
	}
#endif
	
	//uLong sum = 0;
	//for (int i = 1; i <=300000000; i++)
	//{
	//	sum += i;
	//}
	//std::cout << sum << std::endl;

	//pool.submitTask(std::make_shared<MyTask>());
	//pool.submitTask(std::make_shared<MyTask>());
	//pool.submitTask(std::make_shared<MyTask>());

	//pool.submitTask(std::make_shared<MyTask>());
	//pool.submitTask(std::make_shared<MyTask>());
	//pool.submitTask(std::make_shared<MyTask>());
	//pool.submitTask(std::make_shared<MyTask>());
	//pool.submitTask(std::make_shared<MyTask>());

	getchar();


}