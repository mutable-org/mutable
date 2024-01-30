#pragma once


#include <condition_variable>
#include <mutable/util/macro.hpp>
#include <mutex>
#include <optional>
#include <utility>


namespace m {

namespace detail {

/**
 * NOTE: This class is not thread-safe.  It is the duty of the user to guard accesses to instances of this class.
 */
class reader_writer_mutex_internal
{
    /** The number of readers waiting to acquire a read lock. */
    uint16_t readers_waiting_{0};
    /** The number of writers waiting to acquire the singular write lock. */
    uint16_t writers_waiting_{0};
    /** The number of currently active readers, i.e. the number of readers that successfully claimed and are now holding
     * a read lock. */
    uint16_t readers_active_{0};
    /** As there can be at most one active writer, we track this information with a `bool`. */
    bool writer_active_{false};

    /** Whether a single reader wants to upgrade to a writer. */
    bool reader_wants_upgrade_{false};

    public:
    uint16_t readers_waiting() const { return readers_waiting_; }
    uint16_t readers_active() const { return readers_active_; }
    uint16_t writers_waiting() const { return writers_waiting_; }
    bool writer_active() const { return writer_active_; }
    bool reader_wants_upgrade() const { return reader_wants_upgrade_; }

    /** Inform others that we want to acquire a read lock. */
    void request_read_lock() { ++readers_waiting_; }
    /** Inform others that we want to acquire the write lock. */
    void request_write_lock() { ++writers_waiting_; }

    /**
     * Try to request an upgrade.  On success, inform others that we want to upgrade our held read lock to the write
     * lock.  When request succeeds, upgrading afterwards is guaranteed to terminate eventually.
     *
     * @return `true` on success, `false` otherwise
     */
    bool try_request_upgrade() {
        if (reader_wants_upgrade()) return false;
        reader_wants_upgrade_ = true;
        request_write_lock();
        return true;
    }

    /** The mutex is in a state where a reader can acquire a reader (shared) lock. */
    bool can_acquire_read_lock() const {
        return not reader_wants_upgrade() and not writer_active() and writers_waiting() == 0;
    }
    /** The mutex is in a state where a writer can acquire a write (exclusive) lock. */
    bool can_acquire_write_lock() const { return not writer_active() and readers_active() == 0; }
    /** The mutex is in a state where a reader can claim the write (exclusive) lock. */
    bool can_upgrade_lock() const {
        M_insist(reader_wants_upgrade(), "cannot upgrade without previously requesting the upgrade");
        M_insist(writers_waiting() > 0, "at least we must be waiting for the write lock");
        return not writer_active() and readers_active() == 1;  // are we the only active reader?
    }

    /** Acquires a previously requested read lock. */
    void acquire_read_lock() {
        M_insist(can_acquire_read_lock());
        M_insist(readers_waiting() > 0, "one or more readers did not properly request a read lock in advance");
        --readers_waiting_;
        ++readers_active_;
    }
    /* Acquires a previously requested write lock. */
    void acquire_write_lock() {
        M_insist(can_acquire_write_lock());
        M_insist(writers_waiting() > 0, "one or more writers did not properly request the write lock in advance");
        --writers_waiting_;
        writer_active_ = true;
    }

    /** Releases a previously acquired read lock. */
    void release_read_lock() {
        M_insist(readers_active() > 0);
        M_insist(not writer_active());
        --readers_active_;
    }

    /* Releases a previously acquired write lock. */
    void release_write_lock() {
        M_insist(writer_active());
        M_insist(readers_active() == 0);
        writer_active_ = false;
    }

    /** Upgrades an already held read lock to a write lock, after previously requesting to upgrade. */
    void upgrade_lock() {
        M_insist(can_upgrade_lock());
        reader_wants_upgrade_ = false;
        release_read_lock();
        acquire_write_lock();
        M_insist(readers_active() == 0, "we must have been the last active reader");
    }
};

}

/**
 * Implements a many-readers, single-writer locking concept.
 *
 * NOTE: This class is thread-safe.
 */
class reader_writer_mutex
{
    /**
     * The mutex to guard operations on members of this class.
     *
     * NOTE: This is actually not the mutex that the user of `reader_writer_mutex` holds.  The mutex held by a user of
     * `reader_writer_mutex` is virtual.
     */
    std::mutex mutex_;

    /** Represents the internal state of the reader-writer mutex. */
    detail::reader_writer_mutex_internal rw_;

    /**
     * Used to signal the respective class of clients that the resource has become available.
     *
     * NOTE: Clients are *not* waiting for `mutex_` to become available, but for the `reader_writer_mutex` to become
     * available.  These are two different things.
     */
    std::condition_variable cv_readers_, cv_writers_;

    public:
    void notify_readers() { cv_readers_.notify_all(); }
    void notify_writer() { cv_writers_.notify_one(); }

    /** Acquire a read lock.  This call blocks the calling thread until a read lock was acquired. */
    void lock_read() {
        std::unique_lock lock{mutex_};
        rw_.request_read_lock();
        cv_readers_.wait(lock, [this]() -> bool { return rw_.can_acquire_read_lock(); });
        M_insist(lock.owns_lock());
        rw_.acquire_read_lock();
    }

    /** Acquire the write lock.  This call blocks the calling thread until the write lock was acquired. */
    void lock_write() {
        std::unique_lock lock{mutex_};
        rw_.request_write_lock();
        cv_writers_.wait(lock, [this]() -> bool { return rw_.can_acquire_write_lock(); });
        M_insist(lock.owns_lock());
        rw_.acquire_write_lock();
    }

    /** Attempts to immediatly claim a read lock.  Returns `true` on success, `false` otherwise. */
    [[nodiscard]] bool try_lock_read() {
        std::unique_lock lock{mutex_};
        if (not rw_.can_acquire_read_lock())
            return false;
        rw_.request_read_lock();
        rw_.acquire_read_lock();
        return true;
    }

    /** Attempts to immediatly claim the exclusive write lock.  Returns `true` on success, `false` otherwise. */
    [[nodiscard]] bool try_lock_write() {
        std::unique_lock lock{mutex_};
        if (not rw_.can_acquire_write_lock())
            return false;
        rw_.request_write_lock();
        rw_.acquire_write_lock();
        return true;
    }

    /**
     * Tries to upgrade a read lock to the write lock.  This operation fails if another thread has already requested an
     * upgrade.
     *
     * WARNING: This operation is dangerous, as misuse can easily lead to a deadlock!
     *
     * If two threads both hold a read lock and then both want to upgrade, one thread will succeed and one will fail.
     * The succeeding thread will start to wait for the upgrade, i.e., until it is the single last active reader.  If
     * the other thread, that failed to upgrade, holds on to its read lock, the two threads deadlock.
     *
     * A fair solution to this problem is to have the reader that failed to upgrade give up its read lock.  This
     * behavior is implemented by `reader_writer_lock::upgrade()`.
     */
    [[nodiscard]] bool upgrade() {
        std::unique_lock lock{mutex_};
        if (not rw_.try_request_upgrade())
            return false;

        M_insist(rw_.reader_wants_upgrade());
        cv_writers_.wait(lock, [this]() -> bool { return rw_.can_upgrade_lock(); });
        M_insist(lock.owns_lock());
        rw_.upgrade_lock();
        return true;
    }

    void unlock_read() {
        std::unique_lock lock{mutex_};
        rw_.release_read_lock();
        M_insist(not rw_.reader_wants_upgrade() or rw_.writers_waiting() != 0,
                 "if a reader wants to upgrade, it must be waiting to claim the write lock");
        if (rw_.reader_wants_upgrade() and rw_.readers_active() == 1) {
            /* There is a single active reader remaining, that wants to upgrade to a writer.  Notify it. */
            lock.unlock();
            notify_writer();
        } else if (rw_.readers_active() == 0 and rw_.writers_waiting() != 0) {
            /* We were the last reader and there are writers waiting to acquire the lock.  Notify one of them. */
            lock.unlock();
            notify_writer();
        }
    }

    void unlock_write() {
        std::unique_lock lock{mutex_};
        M_insist(rw_.readers_active() == 0, "must not have active readers while holding the write lock");
        rw_.release_write_lock();
        if (rw_.writers_waiting() != 0) {
            /* There are other writers waiting to acquire the lock.  Notify one of them. */
            lock.unlock();
            notify_writer();
        } else if (rw_.writers_waiting() == 0 and rw_.readers_waiting() != 0) {
            // We were the last writer and there are other readers waiting to acquire the lock.  Notify all of them.
            lock.unlock();
            notify_readers();
        }
    }
};

/**
 * This is a helper class that helps managing reader and writer locks claimed from `reader_writer_mutex`.  Its main
 * purpose is to automatically unlock on destruction.
 */
struct reader_writer_lock
{
    private:
    std::reference_wrapper<reader_writer_mutex> rw_mutex_;
    enum {
        LOCK_NONE,      ///< holds no lock
        LOCK_READ,      ///< holds a read (shared) lock
        LOCK_WRITE,     ///< holds a write (exclusive) lock
    } state_{LOCK_NONE};

    public:
    explicit reader_writer_lock(reader_writer_mutex &rw_mutex) : rw_mutex_(rw_mutex) { }
    reader_writer_lock(const reader_writer_lock&) = delete;
    reader_writer_lock(reader_writer_lock &&other)
        : rw_mutex_(other.rw_mutex_)
        , state_(std::exchange(other.state_, LOCK_NONE))
    { }

    ~reader_writer_lock() {
        if (state_ == LOCK_NONE) return;
        unlock();
    }

    reader_writer_lock & operator=(reader_writer_lock &&other) = default;

    bool owns_read_lock() const { return state_ == LOCK_READ; }
    bool owns_write_lock() const { return state_ == LOCK_WRITE; }

    /** Acquire a read lock.  This call blocks the calling thread until a read lock was acquired. */
    void lock_read() {
        rw_mutex_.get().lock_read();
        state_ = LOCK_READ;
    }

    /** Acquire the write lock.  This call blocks the calling thread until the write lock was acquired. */
    void lock_write() {
        rw_mutex_.get().lock_write();
        state_ = LOCK_WRITE;
    }

    /**
     * Tries to upgrade a held read lock to the write lock.  If this call succeeds, it blocks until the write lock was
     * acquired.  If this call fails, immediatly unlocks the held read lock to prevent deadlocks and returns.
     *
     * NOTE: On failure, the held read lock is unlocked to prevent deadlocks.
     *
     * As a consequence, a reader that wants to upgrade and fails takes no precedence over other waiting readers or
     * writers.  Contrary, a reader that *succeeds* to upgrade takes precedence over other waiting readers.
     */
    [[nodiscard]] bool upgrade() {
        M_insist(state_ == LOCK_READ);
        if (rw_mutex_.get().upgrade()) {
            state_ = LOCK_WRITE;
            return true;
        } else {
            unlock();  // prevent deadlock by giving up read lock after failing to upgrade
            return false;
        }
    }

    void unlock() {
        M_insist(state_ != LOCK_NONE);
        switch (state_) {
            case LOCK_NONE:  M_unreachable("");
            case LOCK_READ:  rw_mutex_.get().unlock_read(); break;
            case LOCK_WRITE: rw_mutex_.get().unlock_write(); break;
        }
        state_ = LOCK_NONE;
    }
};

struct read_lock
{
    friend void swap(read_lock &first, read_lock &second) {
        using std::swap;
        swap(first.rw_mutex_,  second.rw_mutex_);
        swap(first.owns_lock_, second.owns_lock_);
    }

    protected:
    std::reference_wrapper<reader_writer_mutex> rw_mutex_;
    bool owns_lock_{false};

    public:
    explicit read_lock(reader_writer_mutex &rw_mutex) : rw_mutex_(rw_mutex) { lock(); }
    read_lock(reader_writer_mutex &rw_mutex, std::defer_lock_t) : rw_mutex_(rw_mutex) { }
    read_lock(reader_writer_mutex &rw_mutex, std::adopt_lock_t) : rw_mutex_(rw_mutex), owns_lock_{true} { }
    read_lock(const read_lock&) = delete;
    read_lock(read_lock &&other) : rw_mutex_(other.rw_mutex_) { swap(*this, other); }

    ~read_lock() { if (owns_lock()) unlock(); }

    read_lock & operator=(read_lock &&other) { swap(*this, other); return *this; }

    bool owns_lock() const { return owns_lock_; }

    void lock() { M_insist(not owns_lock()); rw_mutex_.get().lock_read(); owns_lock_ = true; }
    void unlock() { M_insist(owns_lock()); rw_mutex_.get().unlock_read(); owns_lock_ = false; }
};

struct write_lock
{
    friend void swap(write_lock &first, write_lock &second) {
        using std::swap;
        swap(first.rw_mutex_,  second.rw_mutex_);
        swap(first.owns_lock_, second.owns_lock_);
    }

    private:
    std::reference_wrapper<reader_writer_mutex> rw_mutex_;
    bool owns_lock_{false};

    public:
    explicit write_lock(reader_writer_mutex &rw_mutex) : rw_mutex_(rw_mutex) { lock(); }
    write_lock(reader_writer_mutex &rw_mutex, std::defer_lock_t) : rw_mutex_(rw_mutex) { }
    write_lock(reader_writer_mutex &rw_mutex, std::adopt_lock_t) : rw_mutex_(rw_mutex), owns_lock_{true} { }
    write_lock(const write_lock&) = delete;
    write_lock(write_lock &&other) : rw_mutex_(other.rw_mutex_) { swap(*this, other); }

    ~write_lock() { if (owns_lock()) unlock(); }

    write_lock & operator=(write_lock &&other) { swap(*this, other); return *this; }

    bool owns_lock() const { return owns_lock_; }

    void lock() { M_insist(not owns_lock()); rw_mutex_.get().lock_write(); owns_lock_ = true; }
    void unlock() { M_insist(owns_lock()); rw_mutex_.get().unlock_write(); owns_lock_ = false; }
};

struct upgrade_lock : protected read_lock
{
    explicit upgrade_lock(reader_writer_mutex &rw_mutex) : read_lock(rw_mutex) { M_insist(owns_read_lock()); }
    upgrade_lock(reader_writer_mutex &rw_mutex, std::defer_lock_t) : read_lock(rw_mutex, std::defer_lock) { }
    upgrade_lock(reader_writer_mutex &rw_mutex, std::adopt_lock_t) : read_lock(rw_mutex, std::adopt_lock) { }
    upgrade_lock(const upgrade_lock&) = delete;
    upgrade_lock(upgrade_lock &&other) = default;

    upgrade_lock & operator=(upgrade_lock&&) = default;

    bool owns_read_lock() const { return read_lock::owns_lock(); }

    using read_lock::lock;
    using read_lock::unlock;

    /**
     * Attempts to upgrade the read lock to a write lock.  Returns a `write_lock` on success.  On failure,
     * `std::nullopt` is returned and the held read lock is unlocked to prevent deadlocks.
     *
     * @see `reader_writer_lock::upgrade()`
     */
    [[nodiscard]] write_lock upgrade() {
        M_insist(owns_read_lock());
        if (rw_mutex_.get().upgrade()) {
            owns_lock_ = false;
            return write_lock{rw_mutex_, std::adopt_lock};  // we already own the write lock after upgrade
        }
        return write_lock{rw_mutex_, std::defer_lock};  // we could not upgrade to a write lock
    }
};

}
