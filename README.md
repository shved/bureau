features: 
- does not support delete key
- does not support complex data types (e.g. collections)

notes:
- first idea was to take AVL-tree for a memtable, but compared to std-lib BTreeMap performance should not be significantly better, so just stick with btreemap from the std

