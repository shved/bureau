use crate::engine::MemTable;
use ahash::AHasher;
use bytes::Bytes;
use std::cmp::{min, Ordering};
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

    fn update_frequency(&mut self, freq: usize) {
        self.score.frequency = freq;
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
/// since it is very crutial here for amount of disk reads. So the name stands for
/// Weighted Least Frequntly Used to articulate that it is not only frequency
/// that is important here.
#[derive(Debug, Clone)]
enum Wlfu {
    Blank,
    Set(Bytes),
}

impl Wlfu {
    fn new(key: Bytes) -> Self {
        Self::Set(key)
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
/// score values by it's position in the set of tables.
///
/// The implementation is rather sketchy, there could be all sorts of suboptimal
/// behaviour but this is generally find. In long run all the most demanded and old
/// records will settle here.
///
/// Cache can be easily promoted to be concurrent proof. Just change freq counters
/// to be atomics and put RWLock on the data map.
#[derive(Debug)]
pub struct Cache {
    map: HashMap<Bytes, CacheValue>,
    frequency: FrequenciesMinSketch,
    wlfu: Wlfu,
    cap: usize,
}

impl Cache {
    pub fn new(cap: usize) -> Self {
        Self {
            map: HashMap::with_capacity(cap),
            frequency: FrequenciesMinSketch::new(),
            wlfu: Wlfu::blank(),
            cap,
        }
    }

    /// Every key for GET request goes through this call. It increments frequencies
    /// and checks for a cache record.
    #[allow(clippy::manual_inspect)]
    pub fn check(&mut self, key: &Bytes) -> CheckResult {
        // Update CMS.
        let freq = self.frequency.increment(key);

        // Check the cache map.
        match self
            .map
            .get_mut(key)
            .map(|value| {
                // Update frequency of value.
                value.update_frequency(freq);
                value
            })
            .cloned()
        {
            Some(value) => {
                // Update least frequent key in the cache.
                self.update_wlru(key, &value);

                return CheckResult::Found(value);
            }
            None => {
                // Check if frequency is above threshold. Frequency threshold is to be
                // a current cache size (but not higher then 100) so that empty cache
                // will be quicker to fill.
                let threshold = min(100, self.size());
                if freq >= threshold {
                    return CheckResult::Candidate(freq);
                }
            }
        }

        // Key is not demanded.
        CheckResult::Miss
    }

    fn update_wlru(&mut self, key: &Bytes, value: &CacheValue) {
        match &self.wlfu {
            Wlfu::Set(wlru_key) => {
                if *key != wlru_key {
                    match self.map.get(wlru_key) {
                        Some(wlru_val) => {
                            if value < wlru_val {
                                self.wlfu = Wlfu::new(key.clone());
                            }
                        }
                        None => self.wlfu = Wlfu::new(key.clone()),
                    }
                }
            }
            Wlfu::Blank => {
                // If LFU not set, let's set it to whatever we have here so that it can be adjusted later.
                self.wlfu = Wlfu::new(key.clone());
            }
        }
    }

    /// Inserts the record into cache. If the cache is full, tries to evict some other record
    /// to free space for a new one. If eviction attempt did not work, it means there are more
    /// valuable records in the cache and the record won't be cached.
    pub fn try_insert(&mut self, key: Bytes, cache_value: CacheValue) {
        if self.is_full() {
            if self.evict(&cache_value) {
                self.map.insert(key, cache_value);
            }

            // Eiter it is inserted or not, we return here.
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
        match &self.wlfu {
            Wlfu::Blank => self.evict_iter(candidate_value),
            Wlfu::Set(key) => {
                if let Some(value) = self.map.get(key) {
                    if candidate_value > value {
                        match self.map.remove(key) {
                            Some(_) => {
                                self.wlfu = Wlfu::Blank;
                                return true;
                            }
                            None => {
                                self.wlfu = Wlfu::Blank;
                                return self.evict_iter(candidate_value);
                            }
                        }
                    }

                    return false;
                }

                false
            }
        }
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
            return self.map.remove(&key).is_some();
        }

        false
    }

    /// It iterates over the whole cache map, first advances generation
    /// and if the same key found in the fresh memtable, it gets updated.
    /// In case value data was update to the new value, its generation
    /// gets reset to 1.
    pub fn refresh(&mut self, data: &MemTable) {
        for (k, v) in self.map.iter_mut() {
            v.advance();
            if let Some(value) = data.get(k) {
                v.data = value.clone();
                v.reset_generation();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::SsTableSize;

    #[test]
    fn test_count_min_sketch() {
        let mut f = FrequenciesMinSketch::new();
        let key = Bytes::from("hello there");
        assert_eq!(f.increment(&key), 1);
        assert_eq!(f.increment(&key), 2);
    }

    #[test]
    fn test_cache_value() {
        let mut cv = CacheValue::new(Bytes::from("payload"), 15, 15);
        assert_eq!(cv.score(), 225);
        cv.advance();
        assert_eq!(cv.score.generation, 16);
        cv.reset_generation();
        assert_eq!(cv.score.generation, 1);
    }

    #[test]
    fn test_cache() {
        let mut c = Cache::new(3);
        let key_1 = Bytes::from("key1");
        let key_2 = Bytes::from("key2");
        let key_3 = Bytes::from("key3");
        assert_eq!(c.size(), 0);

        let check_result = c.check(&key_1);
        assert!(matches!(check_result, CheckResult::Candidate(_)));
        if let CheckResult::Candidate(freq) = check_result {
            assert_eq!(freq, 1);
        }
        let check_result = c.check(&key_1);
        assert!(matches!(check_result, CheckResult::Candidate(_)));
        if let CheckResult::Candidate(freq) = check_result {
            assert_eq!(freq, 2);
            c.try_insert(
                key_1.clone(),
                CacheValue::new(Bytes::from("value1"), freq, 1),
            );
        }
        let check_result = c.check(&key_1);
        assert!(matches!(check_result, CheckResult::Found(_)));
        assert_eq!(c.size(), 1);

        let check_result = c.check(&key_2);
        assert!(matches!(check_result, CheckResult::Candidate(_)));
        if let CheckResult::Candidate(freq) = check_result {
            assert_eq!(freq, 1);
        }
        let check_result = c.check(&key_2);
        assert!(matches!(check_result, CheckResult::Candidate(_)));
        if let CheckResult::Candidate(freq) = check_result {
            assert_eq!(freq, 2);
            c.try_insert(
                key_2.clone(),
                CacheValue::new(Bytes::from("value1"), freq, 1),
            );
        }
        let check_result = c.check(&key_2);
        assert!(matches!(check_result, CheckResult::Found(_)));
        assert_eq!(c.size(), 2);

        assert!(!c.is_full());

        let check_result = c.check(&key_3);
        assert!(matches!(check_result, CheckResult::Miss));
        let check_result = c.check(&key_3);
        assert!(matches!(check_result, CheckResult::Candidate(_)));
        if let CheckResult::Candidate(freq) = check_result {
            assert_eq!(freq, 2);
            c.try_insert(
                key_3.clone(),
                CacheValue::new(Bytes::from("value1"), freq, 1),
            );
        }
        let check_result = c.check(&key_3);
        assert!(matches!(check_result, CheckResult::Found(_)));
        assert_eq!(c.size(), 3);

        assert!(c.is_full());

        let mut m = MemTable::new(SsTableSize::Is(4096), None);
        c.refresh(&m);

        let key_4 = Bytes::from("key4");
        let check_result = c.check(&key_4);
        assert!(matches!(check_result, CheckResult::Miss));
        let cv = CacheValue::new(Bytes::from("value4"), 1, 1);
        assert!(!c.evict(&cv));
        c.try_insert(key_4.clone(), CacheValue::new(Bytes::from("v"), 1, 1));
        let check_result = c.check(&key_4);
        assert!(matches!(check_result, CheckResult::Miss));
        assert_eq!(c.size(), 3);

        let check_result = c.check(&key_1);
        // At this point key_1 is the most demanded in terms of cache score
        // but it remains WLFU at the same time. This is one of the flaws of cache
        // but as the next keys will hit cache WLFU will be adjusted
        // and there is no chance key_1 will be evicted by any record less valuable
        // then the key_1 so that's generally fine.
        assert!(matches!(&c.wlfu, Wlfu::Set(_)));
        if let Wlfu::Set(key) = &c.wlfu {
            assert_eq!(key, &key_1);
        }
        assert!(matches!(check_result, CheckResult::Found(_)));
        if let CheckResult::Found(cv) = check_result {
            assert_eq!(cv.score.generation, 2);
        }

        let new_value = Bytes::from("new_value");
        m.insert(key_1.clone(), new_value.clone(), None);
        c.refresh(&m);

        let check_result = c.check(&key_1);
        assert!(matches!(check_result, CheckResult::Found(_)));
        if let CheckResult::Found(cv) = check_result {
            assert_eq!(cv.data, &new_value);
            assert_eq!(cv.score.generation, 1);
            assert_eq!(cv.score.frequency, 5);
        }

        let _ = c.check(&key_1);
        let _ = c.check(&key_1);
        let _ = c.check(&key_1);
        let _ = c.check(&key_1);
        let _ = c.check(&key_1);
        let _ = c.check(&key_1);
        let _ = c.check(&key_1);
        let check_result = c.check(&key_1);
        assert!(matches!(check_result, CheckResult::Found(_)));
        // key1 score here should be 13.
        if let CheckResult::Found(cv) = check_result {
            assert_eq!(cv.score.generation, 1);
            assert_eq!(cv.score.frequency, 13);
        }

        // Check WLFU is still key_1.
        assert!(matches!(&c.wlfu, Wlfu::Set(_)));
        if let Wlfu::Set(key) = &c.wlfu {
            assert_eq!(key, &key_1);
        }

        let check_result = c.check(&key_2);
        assert!(matches!(check_result, CheckResult::Found(_)));
        // key2 score here should be 12 and thus it should go as a new WLFU.
        if let CheckResult::Found(cv) = check_result {
            assert_eq!(cv.score.generation, 3);
            assert_eq!(cv.score.frequency, 4);
        }

        // Now WLFU should be key_2.
        assert!(matches!(&c.wlfu, Wlfu::Set(_)));
        if let Wlfu::Set(key) = &c.wlfu {
            assert_eq!(key, &key_2);
        }
    }
}
