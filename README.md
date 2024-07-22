features: 
- does not support delete key
- does not support complex data types (e.g. collections)

notes:
- first idea was to take AVL-tree for a memtable, but compared to std-lib BTreeMap performance should not be significantly better, so just stick with btreemap from the std
- cache, memtable and shadow mem are always the same position. these are not present in index. index is only for sstables.
- is index entry a file name?
- paths to write to get from here: https://www.pathname.com/fhs/pub/fhs-2.3.html#THEVARHIERARCHY (/var)
- how to protect shadow table for the moment sstable is not yet written to disk
