#pragma once

#include <map>
#include <deque>
#include <unordered_map>
#include <tuple>
#include <string>
#include <functional>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <mutex>
#include <chrono>
#include <atomic>
#include <algorithm>

template <typename Key, typename Value, typename Hash = std::hash<Key>>
class ConflatedQueue
{
public:
    using ItemType = pair<Key, Value>;

    void add(Key&& k, Value&& v)
    {
        auto it = m_mrvCache.find(k);
        if (it == m_mrvCache.end())
        {               
            tie(it, ignore) = m_mrvCache.emplace(std::move(k), std::move(v));
            m_queue.push_back(std::cref(it->first));
        }
        else
        {
            it->second = std::move<Value>(v);
        }        
    }  

    void add(const Key& k, const Value& v)
    {
        auto it = m_mrvCache.find(k);
        if (it == m_mrvCache.end())
        {
            tie(it, ignore) = m_mrvCache.emplace(k, v);
            m_queue.push_back(std::cref(it->first));
        }
        else
        {
            it->second = v;
        }
    }

    bool empty() 
    {
        return m_queue.empty(); 
    }
    
    ItemType extractLast()
    {
        auto it = m_mrvCache.find(m_queue.front());
        if (it == m_mrvCache.end())
        {
            throw runtime_error("empty Queue!");
        }

        auto r = make_pair(it->first, it->second);

        m_mrvCache.erase(it);
        m_queue.pop_front();

        return r;
    }

private:
    std::deque<reference_wrapper<const Key>> m_queue;
    std::unordered_map<Key, Value, Hash> m_mrvCache;
};

template <typename Queue>
class WorkQueue
{
public:
    using ItemType = typename Queue::ItemType;
    
    WorkQueue(Queue& q):m_queue(q),m_stop(false) {}

    template <typename Key, typename Value>
    void add(Key&& k, Value&& v)
    {     
        unique_lock<mutex> lock(m_guard);
        m_queue.add(forward<Key>(k), forward<Value>(v));

        lock.unlock();
        m_goodNews.notify_one();
    }
    
    void stop()
    {      
        m_stop = true;
        m_goodNews.notify_all();
    }

    bool extractLast(ItemType& item)
    {
        unique_lock<mutex> lock(m_guard);

        do
        {
            if (!m_queue.empty())
            {
                item = std::move(m_queue.extractLast());
                return true;
            }

            m_goodNews.wait(lock);        

        } 
        while (m_queue.empty() && !m_stop);
        return m_stop ? false : true;        
    }

    bool empty()
    {
        return m_queue.empty();
    }

private:
    Queue m_queue;
    mutex m_guard;
    condition_variable m_goodNews;
    bool m_stop;
};

struct Price
{
    double price;
    long long int size;       
    

    enum class Side : char
    {
        NA = 'N',
        BID = 'B',
        ASK = 'A'
    };

    Side side;

    Price() {}

    Price(double px, long long int sz, Side s):price(px), size(sz), side(s) {}

    Price(Price&& o)
    {
        price = o.price;
        size = o.size;
        side = o.side;

        cout << "Price(" << ")1 move constructor\n";
    }

    Price& operator= (Price&& o)
    {
        price = o.price;
        size = o.size;
        side = o.side;
        
        cout << "Price(" << ")move assign \n";
        return *this;
    }

    Price(const Price& o)
    {
        price = o.price;
        size = o.size;
        side = o.side;

        cout << "Price(" << ")copy constructor\n";
    }

    Price& operator= (const Price& o)
    {
        price = o.price;
        size = o.size;
        side = o.side;

        cout << "Price(" << ")assing\n";
    }

};
void testSPSC()
{
     atomic<size_t> recievedN(0);
     
    ConflatedQueue<string, Price> conflatedQueue;
     WorkQueue<ConflatedQueue<string, Price>> q(conflatedQueue);

     auto consume = [&q, &recievedN]() {
         try
         {
             while (1)
             {
                 ConflatedQueue<string, Price>::ItemType last;
                 if (!q.extractLast(last))
                     return;

                 recievedN++;
             }
         }
         catch (const std::exception& e)
         {
             return;
         }
     };

     vector<thread> pool;

     pool.push_back(thread(consume));
     pool.push_back(thread(consume));
     pool.push_back(thread(consume));

    thread publisher(
        [&q]() {
        constexpr size_t _N_ = 5;
        for (size_t i = 0; i < _N_; i++)
        {
            q.add(to_string(i % 5), 
                Price { static_cast<double>(i % 5), i % 400,
                       (i % 2 == 0 ? Price::Side::ASK 
                                   : Price::Side::BID) });
        }               
        
    });    
    


    if (publisher.joinable())
        publisher.join();
       
    this_thread::sleep_for(3s);
    q.stop();

    for_each(pool.begin(), pool.end(), [](auto& t) { if (t.joinable()) t.join(); });
    cout << "recieved " << recievedN << " updates";
}


class OrderId
{
public:
    OrderId(string&& id):_s(move(id))
    {
    }

    OrderId(OrderId&& o)
    {
        _s = std::move(o._s);
        cout << "OrderId(" << _s.c_str() << ")1 move constructor\n";
    }

    OrderId& operator= (OrderId&& o)
    {
        _s = std::move(o._s);
        cout << "OrderId(" << _s.c_str() << ")move assign \n";
    }

    OrderId(const OrderId& o)
    {
        _s = o._s;
        cout << "OrderId(" << _s.c_str() << ")copy constructor\n";
    }

    OrderId& operator= (const OrderId& o)
    {
        _s = o._s;
        cout << "OrderId(" << _s.c_str() << ")assing\n";
    }

    bool operator == (const OrderId& o) const noexcept
    {
        return _s == o._s;
    }

    struct Hash
    {
        std::size_t operator()(const OrderId& o) const noexcept
        {
            std::size_t h1 = std::hash<std::string>()(o._s);            
            return h1 << 1;
        }
    };

    std::string str() const
    {
        return _s;
    }

    std::string _s;
};

class Order
{
public:
    Order(string&& keyStation) :_s(move(keyStation))
    {
    }

    Order(Order&& o)
    {
        _s = std::move(o._s);
        cout << "Order(" << _s.c_str() << ")1 move constructor\n";
    }

    Order& operator= (Order&& o)
    {
        _s = std::move(o._s);
        cout << "Order(" << _s.c_str() << ")move assign \n";
        return *this;
    }

    Order(const Order& o)
    {
        _s = o._s;
        cout << "Order(" << _s.c_str() << ")copy constructor\n";
    }

    Order& operator= (const Order& o)
    {
        _s = o._s;
        cout << "Order(" << _s.c_str() << ")assing\n";
        return *this;
    }

    std::string str() const
    {
        return _s;
    }

    std::string _s;
};

void testConflationQueue()
{
    ConflatedQueue<OrderId, Order, OrderId::Hash> q;

    q.add(OrderId("1"), Order("reuters"));
    q.add(OrderId("2"), Order("ebs"));
    q.add(OrderId("3"), Order("currenex"));
    q.add(OrderId("3"), Order("currenex"));
    
    {
        OrderId i4("4");
        Order o4("currenex");
        q.add(i4, o4);
    }

    cout << "\n Let's start extracting\n!";

    while (!q.empty())
    {           
        [](const auto& item) {
            cout << endl << "i: " << item.first.str()
                << "| o: " << item.second.str()
                << endl;
        }
        (q.extractLast());
    }

    q.add(OrderId("1"), Order("reuters"));
    q.add(OrderId("2"), Order("ebs"));
    q.add(OrderId("3"), Order("currenex"));
    q.add(OrderId("3"), Order("currenex"));

    while (!q.empty())
    {
        [](const auto& item) {
            cout << endl << "i: " << item.first.str()
                << "| o: " << item.second.str()
                << endl;
        }
        (q.extractLast());
    }
        
}