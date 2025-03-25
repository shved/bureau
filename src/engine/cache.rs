use ahash::AHasher;
use bytes::Bytes;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

const CMS_BUCKETS: usize = 4;
const CMS_WIDTH: usize = 4096;
const CACHE_THRESHOLD: usize = 100;

// TODO: Rework min sketch to use different hash functions for different buckets for better distribution.
// E.g. XXHash and MurMurHash3.
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
/// Found means key is cached, Lack means it is not cached but needed, Miss means it does not need to be cached.
pub enum CheckResult {
    Found(Bytes),
    Lack,
    Miss,
}

// TODO: Doc struct and functions.
#[derive(Debug)]
pub struct Cache {
    map: HashMap<Bytes, Bytes>,
    generations: HashMap<Bytes, usize>,
    frequency: FrequenciesMinSketch,
    cap: usize,
    least_freq: (Bytes, usize),
}

impl Cache {
    pub fn new(cap: usize) -> Self {
        Self {
            map: HashMap::with_capacity(cap),
            generations: HashMap::with_capacity(cap),
            frequency: FrequenciesMinSketch::new(),
            least_freq: (Bytes::default(), usize::MAX),
            cap,
        }
    }

    pub fn check(&mut self, key: &Bytes) -> CheckResult {
        // Update counters.
        let freq = self.frequency.increment(key);

        // Check if frequency if upper threshold.
        if freq >= CACHE_THRESHOLD {
            // Try obtain the value.
            match self.map.get(key) {
                Some(value) => {
                    // Make use of generation to keep oldest records in the cache.
                    let generation = match self.generations.get(key) {
                        Some(g) => *g,
                        None => 1,
                    };

                    // Update least frequent key in the cache.
                    if freq * generation < self.least_freq.1 {
                        self.least_freq.0 = key.clone();
                        self.least_freq.1 = freq * generation;
                    }
                    return CheckResult::Found(value.clone());
                }
                None => return CheckResult::Lack,
            }
        }

        CheckResult::Miss
    }

    /// Sets the value to cache. If the cache is full it somehow makes space for the new record.
    pub fn set(&mut self, key: Bytes, value: Bytes) {
        if self.map.len() > self.cap {
            // Free some space in the cache before insert new value.
            if self.evict().is_none() {
                // If evictions did not work, just remove the first key found.
                if let Some(first_key) = self.map.keys().next().cloned() {
                    self.map.remove(&first_key);
                }
            }
        }

        self.map.insert(key.clone(), value);
        self.generations.insert(key, 1);
    }

    /// Just replaces an old value with the new one. We trust caller here that
    /// the key is already set so the cache does not bloat.
    pub fn refresh(&mut self, key: Bytes, value: Bytes) {
        self.map.insert(key.clone(), value);
        self.generations.insert(key, 1);
    }

    pub fn increment_generations(&mut self) {
        for (_, v) in self.generations.iter_mut() {
            *v += 1;
        }
    }

    /// Removes the least_freq keys from the map.
    fn evict(&mut self) -> Option<Bytes> {
        if let Some(v) = self.map.remove(&self.least_freq.0) {
            return Some(v);
        };

        None
    }
}
