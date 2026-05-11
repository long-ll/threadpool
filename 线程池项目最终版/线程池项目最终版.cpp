
#include <iostream>
#include <future>
#include <vector>

#include"threadpool.h"

int sum1(int a, int b) 
{
	return a + b;
}

struct number {
	int x;
	int y;
};


number sum(number a, number b) {
	number c;
	c.x = a.x + b.x;
	c.y = a.y + b.y;
	return c;
}

std::vector<number> push_back_data(number a, std::vector<number> all) {
	all.push_back(a);
	return all;
}
int main()
{
	ThreadPool pool;

	//pool.setMode(PoolMode::MODE_CACHED);
	pool.start(2);

	number a;
	a.x = 1;
	a.y = 2;
	number b;
	b.x = 3;
	b.y = 4;

	std::vector<number> test;
	std::future<std::vector<number>> r = pool.submitTask(push_back_data ,a, test);
	std::vector<number> test1 = r.get();
	std::cout << test[0].x << " " << test[0].y << std::endl;


	std::future<number> re = pool.submitTask(sum, a, b);
	number c = re.get();
	std::cout << c.x << " " << c.y << std::endl;


	//std::future<int> res = pool.submitTask(sum1, 1, 2);

/*
	std::future<int> res1 = pool.submitTask([](int b, int end)->int {
		int sum = 0;
		for (int i = b; i <= end; i++)
			sum += i;
		std::this_thread::sleep_for(std::chrono::seconds(5));
		return sum;
	}, 1, 100);

	std::future<int> res78 = pool.submitTask([](int b, int end)->int {
		int sum = 0;
		for (int i = b; i <= end; i++)
			sum += i; 
		std::this_thread::sleep_for(std::chrono::seconds(5));
		return sum;
	}, 2, 100);

	std::future<int> res9 = pool.submitTask([](int b, int end)->int {
		int sum = 0;
		for (int i = b; i <= end; i++)
			sum += i; 
		std::this_thread::sleep_for(std::chrono::seconds(5));
		return sum;
	}, 3, 100);*/


	//std::cout << res1.get() << std::endl;
	//std::cout << res78.get() << std::endl;
	//std::cout << res9.get() << std::endl;



	//std::packaged_task<int(int, int)> task(sum1);

	//std::future<int> res = task.get_future();

	//std::thread t1(std::move(task),10, 20);
	//t1.detach();

	////std::thread t2(sum1,10, 20);

	//std::cout << res.get() << std::endl;



}

