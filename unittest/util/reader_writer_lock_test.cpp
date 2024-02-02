#include <catch2/catch.hpp>

#include <mutable/util/reader_writer_lock.hpp>
#include <thread>


using namespace m;
using namespace std::chrono;
using namespace std::chrono_literals;


TEST_CASE("reader_writer_mutex/simulation", "[core][util]")
{
    reader_writer_mutex mutex;

    /* R1 */ CHECK(mutex.try_lock_read());          // first reder success
    /* W1 */ CHECK_FALSE(mutex.try_lock_write());   // first writer fail(1)
    /* R2 */ CHECK(mutex.try_lock_read());          // second reader success
    /* W1 */ CHECK_FALSE(mutex.try_lock_write());   // first writer fail(2)
    /* R2 */ mutex.unlock_read();                   // second reader unlock
    /* W1 */ CHECK_FALSE(mutex.try_lock_write());   // first writer fail(3)
    /* R1 */ mutex.unlock_read();                   // first reader unlock
    /* W1 */ CHECK(mutex.try_lock_write());         // first writer success
    /* R3 */ CHECK_FALSE(mutex.try_lock_read());    // third reader fail(1)
    /* W2 */ CHECK_FALSE(mutex.try_lock_write());   // second writer fail(1)
    /* W1 */ mutex.unlock_write();                  // first reader unlock
    /* W2 */ CHECK(mutex.try_lock_write());         // second writer success
    /* R3 */ CHECK_FALSE(mutex.try_lock_read());    // third reader fail(1)
    /* W2 */ mutex.unlock_write();                  // first writer unlock
    /* R3 */ CHECK(mutex.try_lock_read());          // third reader success
    /* R3 */ mutex.unlock_read();                   // third reader unlock
}

TEST_CASE("reader_writer_mutex/reader_writer_lock/3_readers_2_writers", "[core][util]")
{
    reader_writer_mutex mutex;
    int shared_value{0}, r1{-1}, r2{-1}, r3{-1};

    auto write = [&mutex, &shared_value](int delta) {
        reader_writer_lock lock{mutex};
        lock.lock_write();
        M_insist(lock.owns_write_lock());
        shared_value += delta;
        std::this_thread::sleep_for(50ms);
    };

    auto read = [&mutex, &shared_value](int &dest) {
        reader_writer_lock lock{mutex};
        lock.lock_read();
        M_insist(lock.owns_read_lock());
        dest = shared_value;
        std::this_thread::sleep_for(50ms);
    };

    /* Spawn first reader, taking 50ms. */
    std::thread tr1{read, std::ref(r1)};
    std::this_thread::sleep_for(10ms);

    /* Spawn first writer, taking 50ms.  It is blocked by the first reader. */
    std::thread tw1{write, 1};
    std::this_thread::sleep_for(10ms);

    /* Spawn second reader.  The first reader should still be active while the first writer is still blocked.
     * Therefore, the newly spawned second reader should wait for the writer to finish. */
    std::thread tr2{read, std::ref(r2)};
    std::this_thread::sleep_for(10ms);

    /* Spawn second writer. */
    std::thread tw2{write, 2};
    std::this_thread::sleep_for(10ms);

    /* Spawn third reader. */
    std::thread tr3{read, std::ref(r3)};
    std::this_thread::sleep_for(10ms);

    tr1.join();
    tr2.join();
    tr3.join();
    tw1.join();
    tw2.join();

    CHECK(r1 == 0);
    CHECK(r2 == 3);  // second writer was prioritized over second reader
    CHECK(r3 == 3);
}

TEST_CASE("reader_writer_mutex/read_lock_write_lock/3_readers_2_writers", "[core][util]")
{
    reader_writer_mutex mutex;
    int shared_value{0}, r1{-1}, r2{-1}, r3{-1};

    auto write = [&mutex, &shared_value](int delta) {
        write_lock lock{mutex};
        M_insist(lock.owns_lock());
        shared_value += delta;
        std::this_thread::sleep_for(50ms);
    };

    auto read = [&mutex, &shared_value](int &dest) {
        read_lock lock{mutex};
        M_insist(lock.owns_lock());
        dest = shared_value;
        std::this_thread::sleep_for(50ms);
    };

    /* Spawn first reader, taking 50ms. */
    std::thread tr1{read, std::ref(r1)};
    std::this_thread::sleep_for(10ms);

    /* Spawn first writer, taking 50ms.  It is blocked by the first reader. */
    std::thread tw1{write, 1};
    std::this_thread::sleep_for(10ms);

    /* Spawn second reader.  The first reader should still be active while the first writer is still blocked.
     * Therefore, the newly spawned second reader should wait for the writer to finish. */
    std::thread tr2{read, std::ref(r2)};
    std::this_thread::sleep_for(10ms);

    /* Spawn second writer. */
    std::thread tw2{write, 2};
    std::this_thread::sleep_for(10ms);

    /* Spawn third reader. */
    std::thread tr3{read, std::ref(r3)};
    std::this_thread::sleep_for(10ms);

    tr1.join();
    tr2.join();
    tr3.join();
    tw1.join();
    tw2.join();

    CHECK(r1 == 0);
    CHECK(r2 == 3);  // second writer was prioritized over second reader
    CHECK(r3 == 3);
}

TEST_CASE("reader_writer_mutex/reader_writer_lock/4_readers_2_upgrades", "[core][util]")
{
    reader_writer_mutex mutex;
    int shared_value{0}, r1{-1}, r2{-1}, r3{-1}, r4{-1}, u1{-1}, u2{-1};

    auto read = [&mutex, &shared_value](int &dest) {
        reader_writer_lock lock{mutex};
        lock.lock_read();
        M_insist(lock.owns_read_lock());
        dest = shared_value;
        std::this_thread::sleep_for(50ms);
    };

    auto upgrade1 = [&mutex, &shared_value](int delta, int &dest) {
        reader_writer_lock lock{mutex};
        lock.lock_read();
        dest = shared_value;  // mimic read op
        std::this_thread::sleep_for(10ms);
        M_insist(lock.upgrade());  // immediately succeeds, as there are only regular readers
        M_insist(lock.owns_write_lock());
        shared_value += delta;
        std::this_thread::sleep_for(50ms);
    };

    auto upgrade2 = [&mutex, &shared_value](int delta, int &dest) {
        reader_writer_lock lock{mutex};
        do {
            lock.lock_read();
            dest = shared_value;  // mimic read op
            std::this_thread::sleep_for(10ms);
        } while (not lock.upgrade());
        M_insist(lock.owns_write_lock());
        shared_value += delta;
        std::this_thread::sleep_for(50ms);
    };

    /* Spawn first reader, taking 50ms. */
    std::thread tr1{read, std::ref(r1)};  // reads 0, completes at 50ms

    /* Spawn first upgrading reader.  Reads for 10ms, then is blocked for 20ms by first reader.  Then writes for 50ms. */
    std::this_thread::sleep_for(20ms);  // 20ms
    std::thread tu1{upgrade1, 1, std::ref(u1)};  // reads 0, upgrades to 1 at 50ms, completes at 100ms

    std::this_thread::sleep_for(20ms);  // 40ms
    std::thread tr2{read, std::ref(r2)};  // reads 1 at 100ms, completes at 150

    /* Spawn second writer. */
    std::this_thread::sleep_for(20ms);  // 60ms
    std::thread tu2{upgrade2, 2, std::ref(u2)};  // reads 1 at 190ms, upgrades to 3 at 200ms, completes at 250ms

    /* Spawn third reader. */
    std::this_thread::sleep_for(20ms);  // 80ms
    std::thread tr3{read, std::ref(r3)};  // reads 1 at 140ms, completes at 190ms

    std::this_thread::sleep_for(120ms);  // 200ms
    std::thread tr4{read, std::ref(r4)};  // reads 1 at 140ms, completes at 190ms

    tr1.join();
    tr2.join();
    tr3.join();
    tr4.join();
    tu1.join();
    tu2.join();

    CHECK(r1 == 0);
    CHECK(r2 == 1);
    CHECK(r3 == 1);
    CHECK(r4 == 3);
    CHECK(u1 == 0);
    CHECK(u2 == 1);
    CHECK(shared_value == 3);
}

TEST_CASE("reader_writer_mutex/upgrade_lock/4_readers_2_upgrades", "[core][util]")
{
    reader_writer_mutex mutex;
    int shared_value{0}, r1{-1}, r2{-1}, r3{-1}, r4{-1}, u1{-1}, u2{-1};

    auto read = [&mutex, &shared_value](int &dest) {
        read_lock lock{mutex};
        M_insist(lock.owns_lock());
        dest = shared_value;
        std::this_thread::sleep_for(50ms);
    };

    auto upgrade1 = [&mutex, &shared_value](int delta, int &dest) {
        upgrade_lock lock{mutex};
        M_insist(lock.owns_read_lock());
        dest = shared_value;  // mimic read op
        std::this_thread::sleep_for(10ms);
        write_lock wlock = lock.upgrade();
        M_insist(wlock.owns_lock());
        shared_value += delta;
        std::this_thread::sleep_for(50ms);
    };

    auto upgrade2 = [&mutex, &shared_value](int delta, int &dest) {
        upgrade_lock lock{mutex, std::defer_lock};
        M_insist(not lock.owns_read_lock());
        write_lock wlock{mutex, std::defer_lock};
        M_insist(not wlock.owns_lock());
        do {
            lock.lock();
            M_insist(lock.owns_read_lock());
            dest = shared_value;  // mimic read op
            std::this_thread::sleep_for(10ms);
        } while (wlock = lock.upgrade(), not wlock.owns_lock());
        M_insist(not lock.owns_read_lock());
        M_insist(wlock.owns_lock());
        shared_value += delta;
        std::this_thread::sleep_for(50ms);
    };

    /* Spawn first reader, taking 50ms. */
    std::thread tr1{read, std::ref(r1)};  // reads 0, completes at 50ms

    /* Spawn first upgrading reader.  Reads for 10ms, then is blocked for 20ms by first reader.  Then writes for 50ms. */
    std::this_thread::sleep_for(20ms);  // 20ms
    std::thread tu1{upgrade1, 1, std::ref(u1)};  // reads 0, upgrades to 1 at 50ms, completes at 100ms

    std::this_thread::sleep_for(20ms);  // 40ms
    std::thread tr2{read, std::ref(r2)};  // reads 1 at 100ms, completes at 150

    /* Spawn second writer. */
    std::this_thread::sleep_for(20ms);  // 60ms
    std::thread tu2{upgrade2, 2, std::ref(u2)};  // reads 1 at 190ms, upgrades to 3 at 200ms, completes at 250ms

    /* Spawn third reader. */
    std::this_thread::sleep_for(20ms);  // 80ms
    std::thread tr3{read, std::ref(r3)};  // reads 1 at 140ms, completes at 190ms

    std::this_thread::sleep_for(120ms);  // 200ms
    std::thread tr4{read, std::ref(r4)};  // reads 1 at 140ms, completes at 190ms

    tr1.join();
    tr2.join();
    tr3.join();
    tr4.join();
    tu1.join();
    tu2.join();

    CHECK(r1 == 0);
    CHECK(r2 == 1);
    CHECK(r3 == 1);
    CHECK(r4 == 3);
    CHECK(u1 == 0);
    CHECK(u2 == 1);
    CHECK(shared_value == 3);
}
