features: 
- does not support delete key
- does not support complex data types (e.g. collections)

notes:
- first idea was to take AVL-tree for a memtable, but compared to std-lib BTreeMap performance should not be significantly better, so just stick with btreemap from the std
- cache, memtable and shadow mem are always the same position. these are not present in index. index is only for sstables.
- is index entry a file name?

