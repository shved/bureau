extern crate criterion;

use bytes::{BufMut, Bytes};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use random_string::generate_rng;
use std::fmt;
use std::ops::Range;

const RAW_DATA_ESTIMATE_SIZE: usize = 3900;
const CHARSET: &str =
    "1234567890_-/%#@!$^&*()+=~`\"'|[]{}abcdefghigklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

fn sequential_search(offsets: &[u16], data: &[u8], key: Bytes) -> Option<Bytes> {
    for offset in offsets {
        let get_key = parse_frame(data, *offset as usize);
        if get_key == key {
            return Some(Bytes::copy_from_slice(parse_frame(data, *offset as usize + 2 + get_key.len())));
        }
    }

    None
}

fn binary_search(offsets: &[u16], data: &[u8], key: Bytes) -> Option<Bytes> {
    let mut low = 0;
    let mut high = offsets.len() - 1;

    while low <= high {
        let mid = low + (high - low) / 2;

        let read_key = parse_frame(&data, offsets[mid] as usize);

        match read_key.cmp(&key) {
            std::cmp::Ordering::Less => low = mid + 1,
            std::cmp::Ordering::Greater => high = mid - 1,
            std::cmp::Ordering::Equal => {
                return Some(Bytes::copy_from_slice(parse_frame(data, offsets[mid] as usize + 2 + read_key.len())));
            }
        }
    }

    None
}

fn parse_frame(data: &[u8], offset: usize) -> &[u8] {
    let key_len: usize = (data[offset] as usize) << 8 | (data[offset + 1] as usize);
    &data[offset + 2..offset + 2 + key_len]
}

#[derive(Debug, Clone)]
struct BenchCase {
    raw_data: Vec<u8>,
    offsets: Vec<u16>,
    entry: Entry,
    position: Position,
}

#[derive(Debug, Clone)]
enum Position {
    Start,
    Mid,
    End,
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Position::Start => write!(f, "_at_the_start"),
            Position::Mid => write!(f, "_at_the_mid"),
            Position::End => write!(f, "_at_the_end"),
        }
    }
}

#[derive(Debug, Clone)]
struct Entry {
    key: Bytes,
    value: Bytes,
}

fn generate_block_data(len_rng: Range<usize>, position: Position) -> BenchCase {
    assert!(len_rng.start > 0, "Key should be longer then 0");
    assert!(len_rng.end < 256, "Key should be shorter then 256");

    let key_position = match position.clone() {
        Position::Start => 0.1,
        Position::Mid => 0.5,
        Position::End => 0.9,
    };

    let mut raw_data: Vec<u8> = Vec::new();
    let mut entries: Vec<Entry> = Vec::new();
    let mut offsets: Vec<u16> = Vec::new();
    let mut size = 0;

    while size < RAW_DATA_ESTIMATE_SIZE - 80 {
        let entry = Entry {
            key: Bytes::from(generate_rng(len_rng.clone(), &CHARSET)),
            value: Bytes::from(generate_rng(len_rng.clone(), &CHARSET)),
        };

        size += entry_size(&entry);
        entries.push(entry);
    }

    entries.sort_by(|e1, e2| e1.key.cmp(&e2.key));

    let idx = entries.len() as f64 * key_position;
    let search_entry = &entries[idx as usize].clone();

    for entry in entries {
        offsets.push(raw_data.len() as u16);
        raw_data.put_u16(entry.key.len() as u16);
        raw_data.put(entry.key);
        raw_data.put_u16(entry.value.len() as u16);
        raw_data.put(entry.value);
    }

    raw_data.extend((raw_data.len()..RAW_DATA_ESTIMATE_SIZE).map(|_| 0));

    BenchCase {
        raw_data,
        offsets,
        entry: search_entry.clone(),
        position,
    }
}

fn entry_size(e: &Entry) -> usize {
    2 + e.key.len() + 2 + e.value.len()
}

fn when_key_close_to_start(c: &mut Criterion) {
    let mut group = c.benchmark_group("key is in the beginning");

    group.warm_up_time(std::time::Duration::from_millis(250));

    let case = generate_block_data(6..60, Position::Start);

    group.bench_with_input(
        BenchmarkId::new("sequential_search", &case.position),
        &case,
        |b, case| {
            b.iter(|| {
                sequential_search(
                    case.offsets.as_ref(),
                    case.raw_data.as_ref(),
                    case.entry.key.clone(),
                )
            });
        },
    );

    group.bench_with_input(
        BenchmarkId::new("binary_search", &case.position),
        &case,
        |b, case| {
            b.iter(|| {
                binary_search(
                    case.offsets.as_ref(),
                    case.raw_data.as_ref(),
                    case.entry.key.clone(),
                );
            });
        },
    );
}

fn when_key_in_the_mid(c: &mut Criterion) {
    let mut group = c.benchmark_group("key is in middle of data");

    group.warm_up_time(std::time::Duration::from_millis(250));

    let case = generate_block_data(6..60, Position::Mid);

    group.bench_with_input(
        BenchmarkId::new("sequential_search", &case.position),
        &case,
        |b, case| {
            b.iter(|| {
                sequential_search(
                    case.offsets.as_ref(),
                    case.raw_data.as_ref(),
                    case.entry.key.clone(),
                );
            });
        },
    );

    group.bench_with_input(
        BenchmarkId::new("binary_search", &case.position),
        &case,
        |b, case| {
            b.iter(|| {
                binary_search(
                    case.offsets.as_ref(),
                    case.raw_data.as_ref(),
                    case.entry.key.clone(),
                );
            });
        },
    );
}

fn when_key_close_the_end(c: &mut Criterion) {
    let mut group = c.benchmark_group("key is close to the end of data");

    group.warm_up_time(std::time::Duration::from_millis(250));

    let case = generate_block_data(6..60, Position::End);

    group.bench_with_input(
        BenchmarkId::new("sequential_search", &case.position),
        &case,
        |b, case| {
            b.iter(|| {
                sequential_search(
                    case.offsets.as_ref(),
                    case.raw_data.as_ref(),
                    case.entry.key.clone(),
                );
            });
        },
    );

    group.bench_with_input(
        BenchmarkId::new("binary_search", &case.position),
        &case,
        |b, case| {
            b.iter(|| {
                binary_search(
                    case.offsets.as_ref(),
                    case.raw_data.as_ref(),
                    case.entry.key.clone(),
                );
            });
        },
    );
}

criterion_group!(
    search_in_block,
    when_key_close_to_start,
    when_key_in_the_mid,
    when_key_close_the_end,
);

criterion_main!(search_in_block);
