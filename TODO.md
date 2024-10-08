- [x] change dump Arc 'database' to LSM with channels
- [x] add simple server logs
- [x] write sstable after memtable is full
- [x] index to track sstables
- [x] forward 'get' to sstable index when no value in memtable
- [x] rework cloning of a memtable to send just initilized table; remove shadowtable since there is no need for it
- [x] add key value validations length (x > 0 && x < 256) and ascii
- [x] make CI setup with clippy, tests, and other things
- [ ] rework memtable insert to probe size first
- [ ] more unit tests
- [ ] high level test with tests/proptests.rs
- [ ] better logging to handle threads and put logs to file
- [ ] add workflow for testcov
- [ ] wal
- [ ] handle shutdown properly
- [ ] cache
- [ ] compaction
- [ ] make bin crate alfa version
- [ ] json response + schema
- [ ] make a statistics unit
  - [ ] keep track of reads from deep sstables to put frequent old values to cache for longer
  - [ ] keep track of average (or better median) key-value pair size to better predict the moment to flush memtable to disk (make it based on histogram)
- [ ] cache and cache strategies (prob on the sst blooms level - make a pool of hot blooms)
- [ ] add key-prefix optimization to sst (keys are ordered so we could save space on the same prefix of several keys)

