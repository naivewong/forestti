#include "third_party/lfqueue.h"

#include <boost/lockfree/queue.hpp>
#include <unordered_map>

#include "base/Mutex.hpp"
#include "leveldb/third_party/thread_pool.h"
#include "mem/mem_postings.h"

class PostingsQueueWrapper {
 public:
  tsdb::mem::postings_list* postings_head_;
  tsdb::mem::postings_list* postings_tail_;
  tsdb::base::RWMutexLock mutex_;
  std::unordered_map<int, tsdb::mem::postings_list*> m_;

  tsdb::mem::postings_list* new_postings_list() {
    tsdb::mem::postings_list* list = new tsdb::mem::postings_list();
    list->version_ = 0;
    list->prev_ = nullptr;
    if (postings_head_) {
      postings_head_->prev_ = list;
      list->next_ = postings_head_;
    } else {
      list->next_ = nullptr;
      postings_tail_ = list;
    }
    postings_head_ = list;
    return list;
  }

  void move_postings_to_head(tsdb::mem::postings_list* list) {
    if (list == postings_head_) return;
    if (list == postings_tail_) postings_tail_ = postings_tail_->prev_;

    if (list->prev_) list->prev_->next_ = list->next_;
    if (list->next_) list->next_->prev_ = list->prev_;

    list->prev_ = nullptr;
    list->next_ = postings_head_;
    postings_head_->prev_ = list;
    postings_head_ = list;
  }

  void add(int i) {
    tsdb::base::RWLockGuard lock(mutex_, 1);
    tsdb::mem::postings_list* p = new_postings_list();
    m_[i] = p;
  }

  void get(int i) {
    tsdb::base::RWLockGuard lock(mutex_, 1);
    auto p = m_.find(i);
    if (p != m_.end()) move_postings_to_head(p->second);
  }

  void add_no_lock(int i) {
    tsdb::mem::postings_list* p = new_postings_list();
    m_[i] = p;
  }

  void get_no_lock(int i) {
    auto p = m_.find(i);
    if (p != m_.end()) move_postings_to_head(p->second);
  }
};

int num = 10000000;

void test1() {
  PostingsQueueWrapper q;
  Timer t;
  t.start();
  for (int i = 0; i < num; i++) {
    q.add(i);
  }
  for (int i = 0; i < num; i++) {
    q.get(i);
  }
  std::cout << "test1 " << t.since_start_nano() << std::endl;
  std::cout << q.m_.size() << std::endl;
}

void test1_2() {
  PostingsQueueWrapper q;
  Timer t;
  t.start();
  for (int i = 0; i < num; i++) {
    q.add_no_lock(i);
  }
  for (int i = 0; i < num; i++) {
    q.get_no_lock(i);
  }
  std::cout << "test1_2 " << t.since_start_nano() << std::endl;
  std::cout << q.m_.size() << std::endl;
}

void test2() {
  PostingsQueueWrapper q;
  ThreadPool pool(4);
  Timer t;
  t.start();
  for (int i = 0; i < 4; i++) {
    pool.enqueue(
        [](PostingsQueueWrapper* q, int start, int end) {
          for (int j = start; j < end; j++) q->add(j);
        },
        &q, num / 4 * i, num / 4 * (i + 1));
  }
  pool.wait_barrier();
  for (int i = 0; i < 4; i++) {
    pool.enqueue(
        [](PostingsQueueWrapper* q, int start, int end) {
          for (int j = start; j < end; j++) q->get(j);
        },
        &q, num / 4 * i, num / 4 * (i + 1));
  }
  pool.wait_barrier();
  std::cout << "test2 " << t.since_start_nano() << std::endl;
  std::cout << q.m_.size() << std::endl;
}

void test3() {
  PostingsQueueWrapper q;
  ThreadPool pool(4);
  queue_root* tasks;
  init_queue(&tasks);
  Timer t;
  t.start();
  for (int i = 0; i < 4; i++) {
    pool.enqueue(
        [](queue_root* q, int start, int end) {
          for (int j = start; j < end; j++)
            queue_add(q,
                      (void*)((uintptr_t)((uint64_t)(j) | 0x8000000000000000)));
        },
        tasks, num / 4 * i, num / 4 * (i + 1));
  }
  pool.wait_barrier();
  for (int i = 0; i < 4; i++) {
    pool.enqueue(
        [](queue_root* q, int start, int end) {
          for (int j = start; j < end; j++)
            queue_add(q, (void*)((uintptr_t)((uint64_t)(j))));
        },
        tasks, num / 4 * i, num / 4 * (i + 1));
  }
  pool.wait_barrier();
  std::cout << "test3 " << t.since_start_nano() << std::endl;

  // Get and process requests from queue.
  void* task;
  while (task = queue_get(tasks)) {
    uint64_t id = (uint64_t)((uintptr_t)(task));
    if (id & 0x8000000000000000)
      q.add(id & 0x7FFFFFFFFFFFFFFF);
    else
      q.get(id);
  }
  std::cout << "test3 " << t.since_start_nano() << std::endl;
  std::cout << q.m_.size() << std::endl;
}

void test4() {
  PostingsQueueWrapper q;
  ThreadPool pool(4);
  queue_root* tasks;
  init_queue(&tasks);
  Timer t;
  t.start();
  for (int i = 0; i < 4; i++) {
    pool.enqueue(
        [](queue_root* q, int start, int end) {
          for (int j = start; j < end; j++)
            queue_add(q,
                      (void*)((uintptr_t)((uint64_t)(j) | 0x8000000000000000)));
        },
        tasks, num / 4 * i, num / 4 * (i + 1));
  }
  pool.wait_barrier();
  for (int i = 0; i < 4; i++) {
    pool.enqueue(
        [](queue_root* q, int start, int end) {
          for (int j = start; j < end; j++)
            queue_add(q, (void*)((uintptr_t)((uint64_t)(j))));
        },
        tasks, num / 4 * i, num / 4 * (i + 1));
  }
  pool.wait_barrier();
  std::cout << "test4 " << t.since_start_nano() << std::endl;

  // Get and process requests from queue.
  for (int i = 0; i < 4; i++) {
    pool.enqueue(
        [](queue_root* q1, PostingsQueueWrapper* q2) {
          void* task;
          while (task = queue_get(q1)) {
            uint64_t id = (uint64_t)((uintptr_t)(task));
            if (id & 0x8000000000000000)
              q2->add(id & 0x7FFFFFFFFFFFFFFF);
            else
              q2->get(id);
          }
        },
        tasks, &q);
  }
  pool.wait_barrier();
  std::cout << "test4 " << t.since_start_nano() << std::endl;
  std::cout << q.m_.size() << std::endl;
}

void test5() {
  PostingsQueueWrapper q;
  ThreadPool pool(4);
  boost::lockfree::queue<uint64_t> tasks(1);
  Timer t;
  t.start();
  for (int i = 0; i < 4; i++) {
    pool.enqueue(
        [](boost::lockfree::queue<uint64_t>* q, int start, int end) {
          for (int j = start; j < end; j++)
            q->push((uint64_t)(j) | 0x8000000000000000);
        },
        &tasks, num / 4 * i, num / 4 * (i + 1));
  }
  pool.wait_barrier();
  for (int i = 0; i < 4; i++) {
    pool.enqueue(
        [](boost::lockfree::queue<uint64_t>* q, int start, int end) {
          for (int j = start; j < end; j++) q->push((uint64_t)(j));
        },
        &tasks, num / 4 * i, num / 4 * (i + 1));
  }
  pool.wait_barrier();
  std::cout << "test5 " << t.since_start_nano() << std::endl;

  // Get and process requests from queue.
  uint64_t id;
  while (tasks.pop(id)) {
    if (id & 0x8000000000000000)
      q.add(id & 0x7FFFFFFFFFFFFFFF);
    else
      q.get(id);
  }
  std::cout << "test5 " << t.since_start_nano() << std::endl;
  std::cout << q.m_.size() << std::endl;
}

void test6() {
  PostingsQueueWrapper q;
  ThreadPool pool(4);
  boost::lockfree::queue<uint64_t> tasks(1);
  Timer t;
  t.start();
  for (int i = 0; i < 4; i++) {
    pool.enqueue(
        [](boost::lockfree::queue<uint64_t>* q, int start, int end) {
          for (int j = start; j < end; j++)
            q->push((uint64_t)(j) | 0x8000000000000000);
        },
        &tasks, num / 4 * i, num / 4 * (i + 1));
  }
  pool.wait_barrier();
  for (int i = 0; i < 4; i++) {
    pool.enqueue(
        [](boost::lockfree::queue<uint64_t>* q, int start, int end) {
          for (int j = start; j < end; j++) q->push((uint64_t)(j));
        },
        &tasks, num / 4 * i, num / 4 * (i + 1));
  }
  pool.wait_barrier();
  std::cout << "test6 " << t.since_start_nano()
            << " lockfree:" << tasks.is_lock_free() << std::endl;

  // Get and process requests from queue.
  for (int i = 0; i < 4; i++) {
    pool.enqueue(
        [](boost::lockfree::queue<uint64_t>* q1, PostingsQueueWrapper* q2) {
          uint64_t id;
          while (q1->pop(id)) {
            if (id & 0x8000000000000000)
              q2->add(id & 0x7FFFFFFFFFFFFFFF);
            else
              q2->get(id);
          }
        },
        &tasks, &q);
  }
  pool.wait_barrier();
  std::cout << "test6 " << t.since_start_nano() << std::endl;
  std::cout << q.m_.size() << std::endl;
}

int main() {
  // test1();
  // test1_2();
  // test2();
  // test3();
  // test4();
  // test5();
  test6();
}