use zebra::database::{Database, Table, TableTransaction};
use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};

// Function that given a size, creates a table with keys 0 to size-1, each mapped to itself
fn setup_table(table: &mut Table<u32, u32>, size: u32) {
    let mut transaction = TableTransaction::new();
    for i in 0..size {
        transaction.set(i, i).unwrap();
    }
    let _ = table.execute(transaction);
}

// Function that given a table and its size, makes a desired proportion of writes and reads
// Assume that keys are 0..size-1  
fn write_to_table(table: &mut Table<u32, u32>, size: u32, write_proportion: f32) {
    let num_write = (size as f32 * write_proportion) as u32;
    let mut transaction = TableTransaction::new();
    for i in 0..num_write {
        transaction.set(i, i+1).unwrap();
    }
    for i in num_write..size {
        transaction.get(&i).unwrap();
    } 
    let _ = table.execute(transaction);

} 

// Fix number of operations, benchmark different proportions of write operations
fn write_proportion_benchmark(c: &mut Criterion) {
    // Setup table
    let mut database: Database<u32, u32> = Database::new();
    let size: u32 = 500000;
    let mut table = database.empty_table();
    setup_table(&mut table, size);
    // Benchmark
    let write_proportions = [0.0, 0.25, 0.5, 0.75, 1.0];
    let mut group = c.benchmark_group("Different write proportions");
    for wp in write_proportions {
        group.bench_with_input(BenchmarkId::new("write_proportion", wp), &wp, |b, &wp| {b.iter(|| write_to_table(&mut table, 500000, wp))});
    };
}

// Do half writes and half reads, benchmark different number of operations
fn no_operations_benchmark(c: &mut Criterion) {
    let mut database: Database<u32, u32> = Database::new();
    let mut table = database.empty_table();
    setup_table(&mut table, 500000);
    let no_ops = [50000, 125000, 250000, 500000];
    let mut group = c.benchmark_group("Different number of operations");
    for no_op in no_ops {
        group.bench_with_input(BenchmarkId::new("number_operations", no_op), &no_op, |b, &no_op| {b.iter(|| write_to_table(&mut table, no_op, 0.5))});
    }
}

criterion_group!(benches, no_operations_benchmark, write_proportion_benchmark);
criterion_main!(benches);