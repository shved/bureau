# Bureau 🗃️
Bureau is the key-value database based on LSM-tree-like algorithm. Not exactly the LSM but somewhat same. Instead of having layers of SSTs, I just have a sequential list of them. Which is to be changed later. The project has two goals. First, it helps me getting comfortable with Rust language. Second, I'm really curious about databases internals (currently going through an excellent set of lectures [here](https://youtube.com/playlist?list=PLSE8ODhjZXjaKScG3l0nuOiDTTqpfnWFf&si=kDk7n-zLPoWhAbBy)) and this project is just my lab puppet to put my fingers into it. A brief coverage of the most interesting implementation outcomes goes further. So, obviously this is not a production grade database. Given that, there is a list of what it does not do:
- does not support delete key
- does not support complex data types (e.g. collections)
- does not support any authentication facility
- does not support multiple databases or privileges
- it is built for Linux (and may be Unix) systems only
- no backward compatibility guarantees: protocol and files formats are to be broken at any moment

## Run
To play around there is a demo binary. In one terminal session run the database with
```
make dev.up
```
in another session call
```
cargo run --bin bureau-demo
```
amount of clients can be set with --clients=X option like that
```
cargo run --bin bureau-demo -- --clients=50
```
There are default of 30 clients. Reads/writes ratio is set to 0.7 in favour of writes, and the ratio of rewriting old keys is set to 0.3 which makes the compaction pretty heavy. You can play around with those numbers in the demo.rs code as well.

If you want to clean up the files writen (WAL and data), call
```
make dev.clean
```
Demo caries statistics a bunch of atomics (this way it looks cool). I've noticed sometimes amount of writes and reads isn't match, but it's because of the memory reordering issues I didn't cover. If a real error will be thrown in the process of test run you will see it in the server logs. It is all Acquire and Release and I did not try it out on an ARM machine so it could potentially be even worse. Anyway if you just curious depending on the time the test runs, this database on the given load pattern gives about 800-3000 SET requests per second and 500-1000 GET requests with the average write being 1-2ms but reads are growing as the database grows since there is yet no thread pool for disk access and the database is a flat data files array, no any extra indexing structure is used (yet).

## Case study
Here goes the list of the most peculiar and fun stuff I've met so far doing this project.

For a mem table which is a first part of LSM that sucks data first, I was first going to take an AVL-tree which is a perfect fit here. But then discovered Rust's std BTreeMap implementation which keys are also sorted. So for the sake of reducing dependencies surface I've choosen to stick with something that goes with the language itself. It is probably better in terms of performance as well since all the AVL crates I've found at the moment were looking like someones lab projects for algo learning purposes.

The database has two main long running threads. The idea behind that is simple. The database operates both in memory (when the new data just come in) and on disk. These two are very different and can be managed separately so I decided to split their work into independent threads and make them synchronize through communication channels. This separation of concerns looks pretty natural to me. In the code the thing that is operating in RAM is called Engine and the thread that works with disk is called Dispatcher. 

Such a way of separating the job between memory and disk allows easily release the main thread while the new SST is being written to the disk. As soon as a memtable gets full, Engine allocates new BTreeMap, sends a pointer to the full BTreeMap to the Dispatcher and release to handle next clients requests. So that working with disk never puts database on hold and clients can get quicker response. Especially for writing, which is good since LSM is good for write-heavy workloads by itself. 

Other things dispatcher does — it keeps track of persisted SST index, updates tables shrinked by compaction and reads values from disk. Synchronizing Dispatcher for all of this things make a guarantee that a value read will never happen before a new table is committed to disk (unless a time traveler tries to get the value that was not yet set by anyone).

If the load is intensive enough that the new table will be ready for going to disk before the previous one was written, the next mem table will just be handled as the previous one got to disk. To prevent RAM bloat I've made a pool for such tables, kind of limiting number, so that new requests to Dispatcher will be eventually suspended until the pool has new slot available. Without such a pool DB could eventually grab attention of the OOM and we do not want that. It is better let the clients wait a little longer in this case or abandon their requests getting timeout error. In the code those tables that are full, not accepting new values, and are going to be persisted can be referred as 'shadow tables'.

The way shadow table was originally implemented was pretty dumb. Every time the mem table got full, I've been making a clone of it (almost 64KB data structure) and 'sending' the whole clone to the Dispatcher channel. It's been extremely ugly looking piece of code so I've changed it for the better to work the way it is described above. The `7eab8d3d` commit is where that change was done.

For the SSTs that are written to disk I've chosen the names in the form of UUID v7 IDs. This way they are unique and can be sorted by the time of creation having just the file name. This way it does not rely on the file creation timestamp or anything else.

The SST encoding schema is pretty well discribed in the code comments. Every table has a bloom filter in the beginning, then the blocks list with the offsets and the raw data itself. Dispatcher reads SST sequentially so that if the key is not in the table, very few bytes and in most cases just one read will be done to know that.

To choose the right locations for default data, wal and log files locations I've had to look into this fancy document: https://www.pathname.com/fhs/pub/fhs-2.3.html#THEVARHIERARCHY

I was skeptical about binary search in the block, since the block raw data is in memory and the data set is pretty small (less then 4KB of keys and values). But I've made a benchmark to see if gives any win against sequential read of key-value vector. The blocks I used for the bench was very optimistic and had only less then 100 elements so I was sure my assumption that sequential read will go better was correct. Benchmark showed that binary search was actually significantly better. For the case when the key is close to the end of the block it give 8x win. And even if the key is in the first 10% of the block it still gives 2x win against sequential read. This was the result I was not expecting at all so I've had no other option and did use the binary search for it.

This program intensively works with disk. I wanted it to be well tested and at the same time I did not want to rely on disk in tests. Didn't want to use any kind of dummy directory or utilize the `/tmp` to test it. To make it work, I've made all the disk calls isolated in a separate layer. There is a special Storage trait for it in lib file. For this trait there are two implementations, one for actual disk and the other just puts everything into structures to keep it in RAM. This approach allows to switch storage implementations with ease and don't deal with any traces in filesystem after test runs.

In process of working decided to add support for database to set values in fire-and-forget style. Database is made with write heavy loads in mind so this option could be useful for some clients.

I started with very basic and dumb protocol where messages are simply utf-8 strings separated by new line indicator. It has a bunch of limitations so I decided to make a simple binary protocol instead. It won't make any hashsum checks since the request is not stored anywhere, request cant be split into pieces and performance is a priority for requests. Again, protocol meant to be as simple as possible.

Because of how tokio_util framed protocols work, there is an opportunity for client to actually occupy the connection forever if they send not enough bytes for a request. By the protocol design every message starts with the payload length so the server will just continue checking the socket for new bytes coming. There is a workaound for it with tokio timeout function for futures, but I don't like it from the program behaviour perspective. And I also don't think this should be considered a problem. Just a curious thing that TCP idle timout won't help here in a particular case when client has sent valid message length but never sent the payload.

Doing WAL I had to decide on the persistance strategy. I decided to go with the buffered writes. So WAL is adding records to buffer that is exactly disk page in size. For simplicity it just assumes the page is 4KB (even though this is not always true). To keep pages 4KB exactly there were two alternatives on the table. One to split the records whenever the buffer is full and second to add paddings of zeroes to buffer whenever the buffer is about to overflow. I decided to go with the second option even though it adds a bit of overhead to the data in size for paddings. First option would make it possible to have broken records in WAL and to recover from such a state it would be needed to have some kind of specieal 'recovery' mode. I decided to keep it simple for the end user.

Compaction is initially implemented very straightforward and not effective. In extent that I had to throttle iteration over tables by simply calling sleep for 200 ms to give reads and writes higher priority. Simple and not very informative measures shows that compaction only gives 1-2 ms of average response time which is fine for the first implementation. A better alternatives would be to have a pool of threads for disk interactions, may be sharding, or smarter selective locks that will only block get requests if they are about to use the table that is being in the process of compaction at the moment.

Even though the database is built on top of LSM and it is meant to be quick on writes and slow on reads, I've had an idea to implement an effective cache for LSM reads. Keep in mind it is originaly implemented slightly different then the origianl white paper LSM, it is more like a flat list of SStables. So the main idea was to make MFU (Most Frequently Used) cache but take into account not only the frequencies of the keys but also generation of the key's table. This is crucial in this case because every table generation means at least one additional disk trip to read and check bloom filter. So that if we have two high-demand keys but one live on the first table whereas the other thouthands tables in the past, it is obvious which value is better for cache. To make cache candidates scoring meet these requirements I choose to just multiply count of a key reads with the generation of the record in sstables index. So that an entry that was requested 100 times and lives on the 10th sstable will have score of 1000. To count keys requests I've implemented a trivial count-min sketch data structure. Least frequently used record in the cache is tracked in the separate attribute and helps to quickly choose candidate for eviction when the cache is already full. Comments in the cache.rs file cover more implementation details.

## TODOs
- [x] change dump Arc 'database' to LSM with channels
- [x] add simple server logs
- [x] write sstable after memtable is full
- [x] index to track sstables
- [x] forward 'get' to sstable index when no value in memtable
- [x] rework cloning of a memtable to send just initilized table; remove persistent shadow table since there is no need for it
- [x] add key value validations
- [x] make CI setup with clippy, tests, and other things
- [x] rework memtable insert to probe size first
- [x] make separate storage layer to abstract code that works with disk
- [x] more unit tests
- [x] handle shutdown properly
- [x] async test with few clients, test shutdown as well
- [x] make clean binary protocol instead of dummy line based protocol
- [x] wal
- [x] compaction
- [x] poor performance test
- [x] cache
- [x] better demo.rs with rates and histograms
- [ ] add more unit tests to dispatcher mod
- [ ] move paddings and paging concerns from WAL to its fs storage
- [ ] remake sstable index to VecDequeue
- [ ] make blocks fill paddings with zeroes to be exactly disk page in size
- [ ] wrap in-memory WAL implementation in Arc to cover more cases with unit tests
- [ ] make all const usize and cast them only when encoding data
- [ ] use different hash functions in count min sketch for better distribution (it is currently 1 function with 4 seeds)
- [ ] make similar cache of bloom filters for the most requested and old tables
- [ ] introduce leveled compaction to merge older tables into bigger chunks
- [ ] experiment with better disk access concurrency with shared stateful index and thread pool of gets and separate threads for index updates
- [ ] handle potential integer overflows where possible (cache scoring goes first)
- [ ] build config from env at the server start
- [ ] add workflow for testcov
- [ ] make bin crate alfa version
- [ ] experiment with memory arena for MemTable that is always living in RAM to reduce extra allocations
- [ ] make a statistics unit
  - [ ] keep track of reads from deep sstables to put frequent old values to cache for longer
  - [ ] keep track of average (or better median) key-value pair size to better predict the moment to flush memtable to disk (make it based on histogram)
- [ ] additional client commands: explicit connection termination, ping
- [ ] replace bloomfilter with self implemented to guarantee serialized bloom size wont change
- [ ] add key-prefix optimization to sst (keys are ordered so we could save space on the same prefix of several keys)
- [ ] handle allocation fails with try_reserve to gracefully shut down in case of error
