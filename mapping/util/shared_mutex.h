/**
 * These classes implement a shared mutex.
 * A shared mutex can be held by either a single exclusive lock, or by multiple shared locks.
 * It is most commonly used for synchronization between reader/writer roles.
 *
 * Several implementations exist:
 * - boost::shared_lock, which requires -lboost_system as a runtime dependency
 * - std::shared_timed_mutex, added in c++14
 * - std::shared_mutex, added in c++17
 * As this project does not want to impose requirements beyond c++11, we have our own.
 *
 * This classes do not have a public API. You can expect:
 * - shared_lock is default constructible
 * - read/write_lock_guard each have a constructor taking a reference to a shared_lock
 * - all three classes have a destructor
 * Do not expect these to be copyable or movable, or to have any public methods.
 * We keep it simple, to ensure that any of the implementations mentioned above can later
 * be used as a drop-in replacement.
 */

#if 0
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>

using shared_mutex = boost::shared_mutex;
using shared_lock_guard = boost::shared_lock<boost::shared_mutex>;
using unique_lock_guard = boost::unique_lock<boost::shared_mutex>;

#endif

#include <mutex>

class shared_mutex {
	public:
		shared_mutex() : read_count(0) {}
		shared_mutex(shared_mutex &&) = delete; // no copies or moves
		~shared_mutex() {}
	private:
		void lock_shared() {
			std::lock_guard<std::mutex> g(w_prio);
			std::lock_guard<std::mutex> g2(mtx);
			read_count++;
			if (read_count == 1)
				w_lock.lock();
		}
		void unlock_shared() {
			std::lock_guard<std::mutex> g(mtx);
			read_count--;
			if (read_count == 0)
				w_lock.unlock();
		}
		void lock_unique() {
			std::lock_guard<std::mutex> g(w_prio);
			w_lock.lock();
		}
		void unlock_unique() {
			w_lock.unlock();
		}

		int read_count;
		std::mutex w_prio;
		std::mutex mtx;
		std::mutex w_lock;
	friend class shared_lock_guard;
	friend class unique_lock_guard;
};

class shared_lock_guard {
	public:
		shared_lock_guard(shared_mutex &m) : m(m) {
			this->m.lock_shared();
		}
		shared_lock_guard(shared_lock_guard &&) = delete; // no copies or moves
		~shared_lock_guard() {
			m.unlock_shared();
		}
	private:
		shared_mutex &m;
};

class unique_lock_guard {
	public:
		unique_lock_guard(shared_mutex &m) : m(m) {
			this->m.lock_unique();
		}
		unique_lock_guard(unique_lock_guard &&) = delete; // no copies or moves
		~unique_lock_guard() {
			m.unlock_unique();
		}
	private:
		shared_mutex &m;
};
