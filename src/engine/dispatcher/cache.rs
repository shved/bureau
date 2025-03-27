use ahash::AHasher;
use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

const CMS_BUCKETS: usize = 4;
const CMS_WIDTH: usize = 4096;

/// Primitive Count-Min Sketch implementation to space-efficient track most frequently requested keys.
/// Currently uses one hash function for all the buckets but different seed for each. Which makes it
/// less accurate because of poor distribution.
// TODO: Rework min sketch to use unique hash function for each bucket
// for better distribution quality. E.g. XXHash and MurMurHash3.
#[derive(Debug)]
struct FrequenciesMinSketch {
    counters: Vec<Vec<usize>>,
}

impl FrequenciesMinSketch {
    fn new() -> Self {
        Self {
            counters: (0..CMS_BUCKETS)
                .map(|_| (0..CMS_WIDTH).map(|_| 0).collect())
                .collect(),
        }
    }

    /// Increments counters and returns the current min for the key.
    fn increment(&mut self, key: &Bytes) -> usize {
        let mut min = usize::MAX;
        for (i, row) in self.counters.iter_mut().enumerate() {
            let index = hash_key(key, i as u64);
            let freq = row[index] + 1;
            row[index] = freq;
            if freq < min {
                min = freq;
            }
        }

        min
    }

    // fn count(&self, key: &Bytes) -> usize {
    //     self.counters
    //         .iter()
    //         .enumerate()
    //         .map(|(i, row)| {
    //             let index = hash_key(key, i as u64);
    //             row[index]
    //         })
    //         .min()
    //         .unwrap_or(0)
    // }
}

fn hash_key(key: &Bytes, seed: u64) -> usize {
    let mut hasher = AHasher::default();
    hasher.write_u64(seed);
    key.hash(&mut hasher);
    hasher.finish() as usize % CMS_WIDTH
}

#[derive(Debug)]
pub enum CheckResult {
    // Found key.
    Found(CacheValue),
    // Candidate and it's frequency estimation.
    Candidate(usize),
    // Value isn't a candidate for caching.
    Miss,
}

/// Cache score consists of two independent values. Frequency is approximation of a key demand
/// and generation is the position of a key's persistent table in the storage index.
#[derive(Debug, Default, Clone)]
pub struct Score {
    pub frequency: usize,
    pub generation: usize,
}

impl Score {
    fn new(frequency: usize, generation: usize) -> Self {
        Self {
            frequency,
            generation,
        }
    }
}

/// Cache value should carry its score together with its data
/// to be evaluated in place where it is needed.
#[derive(Debug, Default, Clone)]
pub struct CacheValue {
    pub data: Bytes,
    pub score: Score,
}

impl CacheValue {
    pub fn new(data: Bytes, frequency: usize, generation: usize) -> Self {
        Self {
            data,
            score: Score::new(frequency, generation),
        }
    }

    pub fn score(&self) -> usize {
        self.score.frequency * self.score.generation
    }

    fn advance(&mut self) {
        self.score.generation += 1
    }

    fn reset_generation(&mut self) {
        self.score.generation = 1;
    }
}

impl PartialEq for CacheValue {
    fn eq(&self, other: &Self) -> bool {
        self.score() == other.score()
    }
}

impl Eq for CacheValue {}

impl PartialOrd for CacheValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CacheValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score().cmp(&other.score())
    }
}

/// Reflects two different states of the LFU value weither it is set or not.
/// It is not exactly LFU, but a combination of frequency multiplied by generation
/// since it is very crutial here for amount of disk reads.
#[derive(Debug, Clone)]
enum Lfu {
    Blank,
    Set { key: Bytes, value: CacheValue },
}

impl Lfu {
    fn new(key: Bytes, value: CacheValue) -> Self {
        Self::Set { key, value }
    }

    fn blank() -> Self {
        Self::Blank
    }
}

/// Cache keeps track of both estimated frequencies and generations of values.
/// The value having higher generation means it is deeper in the set of sstables,
/// so that reading it will take more disk reads. That's why for disk reads it is
/// crucial to keep track of a value generation. From the frequently requested
/// values we want to cache the oldest ones. Generation helps to distinguish and
/// score values by it's position in the set of tables. Cache can be easily promoted
/// to be concurrent proof. Just change freq counters to be atomics and put RWLock
/// on the data map.
#[derive(Debug)]
pub struct Cache {
    map: HashMap<Bytes, CacheValue>,
    frequency: FrequenciesMinSketch,
    least_req: Lfu,
    cap: usize,
}

impl Cache {
    pub fn new(cap: usize) -> Self {
        Self {
            map: HashMap::with_capacity(cap),
            frequency: FrequenciesMinSketch::new(),
            least_req: Lfu::blank(),
            cap,
        }
    }

    /// Every key for GET request goes through this call. It increments frequencies
    /// and checks for a cache record.
    pub fn check(&mut self, key: &Bytes) -> CheckResult {
        // Update frequencies.
        let freq = self.frequency.increment(key);

        // Check if frequency is above threshold.
        // Adjust the frequency threshold to be cache size
        // so that empty cache will be quicker to fill.
        if freq >= self.size() {
            // Check the cache map.
            match self.map.get(key) {
                Some(value) => {
                    // Update least frequent key in the cache.
                    match &self.least_req {
                        Lfu::Set { value: lr_val, .. } => {
                            if value < lr_val {
                                self.least_req = Lfu::new(key.clone(), value.clone());
                            }
                        }
                        Lfu::Blank => {
                            // If LFU not set, let's set it to whatever we have here so that it can be adjusted later.
                            self.least_req = Lfu::new(key.clone(), value.clone());
                        }
                    }

                    return CheckResult::Found(value.clone());
                }
                None => return CheckResult::Candidate(freq),
            }
        }

        // Key is not demanded.
        CheckResult::Miss
    }

    /// Inserts the record into cache. If the cache is full, tries to evict some other record
    /// to free space for a new one. If eviction attempt did not work, it means there are more
    /// valuable records in the cache and the record won't be cached.
    pub fn try_insert(&mut self, key: Bytes, cache_value: CacheValue) {
        if self.is_full() {
            if self.evict(&cache_value) {
                self.map.insert(key, cache_value);
            }

            // Eviction failed, value won't be cached.
            return;
        }

        self.map.insert(key, cache_value);
    }

    fn size(&self) -> usize {
        self.map.len()
    }

    fn is_full(&self) -> bool {
        self.size() >= self.cap
    }

    /// Try to evict record from cache to free space for a new record.
    fn evict(&mut self, candidate_value: &CacheValue) -> bool {
        match &self.least_req {
            Lfu::Blank => {
                // TODO: It is also possible to just set least_req in this branch so it later be adjusted.
                return self.evict_iter(candidate_value);
            }
            Lfu::Set { key, .. } => match self.map.remove(key) {
                Some(_) => return true,
                None => {
                    if self.evict_iter(candidate_value) {
                        return true;
                    }
                }
            },
        };

        false
    }

    /// If no other option worked, try to evict the first record in the cache with
    /// lower score then the candidate's score.
    fn evict_iter(&mut self, candidate_value: &CacheValue) -> bool {
        if let Some(key) = self
            .map
            .iter()
            .find(|(_, v)| v < &candidate_value)
            .map(|(k, _)| k.clone())
        {
            if self.map.remove(&key).is_some() {
                return true;
            }
        }

        false
    }

    /// Just replaces an old value with the new one if it's in cache.
    pub fn update(&mut self, key: &Bytes, value: &Bytes) {
        if let Some(cache_value) = self.map.get_mut(key) {
            cache_value.data = value.clone();
            // New value also resets generation to 1.
            cache_value.reset_generation();
        }
    }

    /// Advances all the cache entries generations.
    pub fn advance(&mut self) {
        for (_, v) in self.map.iter_mut() {
            v.advance();
        }
    }
}
