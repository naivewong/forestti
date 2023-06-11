#include <memory>
#include <stack>
#include <stdexcept>

template <class T, class D = std::default_delete<T>>
class SmartObjectPool {
 private:
  struct ReturnToPool_Deleter {
    explicit ReturnToPool_Deleter(std::weak_ptr<SmartObjectPool<T, D> *> pool)
        : pool_(pool) {}

    void operator()(T *ptr) {
      if (auto pool_ptr = pool_.lock())
        (*pool_ptr.get())->add(std::unique_ptr<T, D>{ptr});
      else
        D{}(ptr);
    }

   private:
    std::weak_ptr<SmartObjectPool<T, D> *> pool_;
  };

 public:
  using ptr_type = std::unique_ptr<T, ReturnToPool_Deleter>;

  SmartObjectPool() : this_ptr_(new SmartObjectPool<T, D> *(this)) {}
  virtual ~SmartObjectPool() {}

  void add(std::unique_ptr<T, D> t) { pool_.push(std::move(t)); }

  ptr_type acquire() {
    if (pool_.empty())
      throw std::out_of_range("Cannot acquire object from an empty pool.");

    ptr_type tmp(pool_.top().release(),
                 ReturnToPool_Deleter{
                     std::weak_ptr<SmartObjectPool<T, D> *>{this_ptr_}});
    pool_.pop();
    return std::move(tmp);
  }

  bool empty() const { return pool_.empty(); }

  size_t size() const { return pool_.size(); }

 private:
  std::shared_ptr<SmartObjectPool<T, D> *> this_ptr_;
  std::stack<std::unique_ptr<T, D>> pool_;
};

// int main(){
//     SmartObjectPool<int> pool;
//     pool.add(std::unique_ptr<int>(new int(42)));
//     assert(pool.size() == 1u);
//     {
//       auto obj = pool.acquire();
//       assert(pool.size() == 0u);
//       assert(*obj == 42);
//       *obj = 1337;
//     }
//     assert(pool.size() == 1u);
//     auto obj = pool.acquire();
//     assert(*obj = 1337);
// }