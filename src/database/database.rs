use crate::{
    common::store::Field,
    database::{
        store::{Cell, Store, Handle, DEPTH},
        interact::drop,
        Table, TableReceiver,
    },
};

use talk::sync::lenders::AtomicLender;
use oh_snap::Snap;
use serde::Deserialize;
use rayon::prelude::*;
use std::{sync::Arc, iter};

/// A datastrucure for memory-efficient storage and transfer of maps with a
/// large degree of similarity (% of key-pairs in common).
///
/// A database maintains a collection of [`Table`]s which in turn represent
/// a collection of key-value pairs. A [`Table`] can be read and modified by
/// creating and executing a [`Transaction`].
///
/// We optimize for the following use cases:
/// 1) Storing multiple maps with a lot of similarities (e.g. snapshots in a system)
/// 2) Transfering maps to databases with similar maps
/// 3) Applying large batches of operations (read, write, remove) to a single map
/// ([`Table`]). In particular, within a batch, we apply operations concurrently
/// and with minimal synchronization between threads.
///
/// The default hashing algorithm is currently Blake3, though this is
/// subject to change at any point in the future.
///
/// It is required that the keys implement `'static` and the [`Serialize`],
/// [`Send`] and [`Sync`] traits.
///
/// [`Field`]: crate::common::store::Field
/// [`Table`]: crate::database::Table
/// [`Transaction`]: crate::database::TableTransaction
/// [`Serialize`]: serde::Serialize
/// [`Send`]: Send
/// [`Sync`]: Sync
///
/// # Examples
///
/// ```rust
///
/// use zebra::database::{Database, Table, TableTransaction, TableResponse, Query};
///
/// fn main() {
///     // Type inference lets us omit an explicit type signature (which
///     // would be `Database<&str, integer>` in this example).
///     let mut database = Database::new();
///
///     // We create a new transaction. See [`Transaction`] for more details.
///     let mut modify = TableTransaction::new();
///     modify.set("Alice", 42).unwrap();
///
///     let mut table = database.empty_table();
///     let _ = table.execute(modify);
///
///     let mut read = TableTransaction::new();
///     let query_key = read.get(&"Alice").unwrap();
///     let response = table.execute(read);
///
///     assert_eq!(response.get(&query_key), Some(&42));
///
///     // Let's remove "Alice" and set "Bob".
///     let mut modify = TableTransaction::new();
///     modify.remove(&"Alice").unwrap();
///     modify.set(&"Bob", 23).unwrap();
///
///     // Ignore the response (modify only)
///     let _ = table.execute(modify);
///
///     let mut read = TableTransaction::new();
///     let query_key_alice = read.get(&"Alice").unwrap();
///     let query_key_bob = read.get(&"Bob").unwrap();
///     let response = table.execute(read);
///
///     assert_eq!(response.get(&query_key_alice), None);
///     assert_eq!(response.get(&query_key_bob), Some(&23));
/// }
/// ```

pub struct Database<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub(crate) store: Cell<Key, Value>,
}

impl<Key, Value> Database<Key, Value>
where
    Key: Field,
    Value: Field,
{
    /// Creates an empty `Database`.
    ///
    /// # Examples
    ///
    /// ```
    /// use zebra::database::Database;
    /// let mut database: Database<&str, i32> = Database::new();
    /// ```
    pub fn new() -> Self {
        Database {
            store: Cell::new(AtomicLender::new(Store::new())),
        }
    }

    /// Creates and assigns an empty [`Table`] to the `Database`.
    ///
    /// # Examples
    ///
    /// ```
    /// use zebra::database::Database;
    /// let mut database: Database<&str, i32> = Database::new();
    ///
    /// let table = database.empty_table();
    /// ```
    pub fn empty_table(&mut self) -> Table<Key, Value> {
        let mut store = self.store.take();
        let id = store.handle_counter;
        let table = Table::empty(self.store.clone(), store.handle_counter, Arc::new(()));
        let root = table.get_root();
        store.handle_map.insert(id, (root, table.2.clone()));

        let handle_transaction = store.handles_db.transaction();
        match handle_transaction.put(
            bincode::serialize(&id).unwrap(),
            bincode::serialize(&root).unwrap())
        {
            Err(e) => println!("{:?}", e),
            _ => ()
        }

        match handle_transaction.commit() {
            Err(e) => println!("{:?}", e),
            _ => ()
        }
        store.handle_counter += 1;
        self.store.restore(store);
        table
    }

    pub fn get_table(&self, id: u32) -> Result<Table<Key, Value>, String> {
        let store = self.store.take();
        let root = store.handle_map.get(&id);
        if root.is_some() {
            let root = &*root.unwrap();
            let handle = Handle::new(self.store.clone(), root.0);
            let table = Table::from_handle(handle, id, root.1.clone());
            self.store.restore(store);
            Ok(table)
        }
        else {
            self.store.restore(store);
            return Err("Don't recognise this id".to_string());
        }
    }

    pub fn clone_table(&self, id: u32) -> Result<Table<Key, Value>, String> {
        let mut store = self.store.take(); 
        let pair = store.handle_map.get(&id);
        match pair {
            Some(root) => {
                let (root, _) = *root;
                let new_id = store.handle_counter;
                let table = Table::from_handle(Handle::new(self.store.clone(), root), new_id, Arc::new(()));
                store.handle_map.insert(new_id, (root, table.2.clone()));
                store.handle_counter += 1;
                // Persistence stuff
                let mut map_changes = Snap::new(iter::repeat_with(|| Vec::new()).take(1 << DEPTH).collect());
                store.incref(root, &mut map_changes);
                map_changes.par_iter().enumerate().for_each(|(idx, vec)| {
                    let maps_transaction = store.maps_db[idx].transaction();
                    for (entry, label, delete) in vec {
                        if !(*delete) {
                            match maps_transaction.put(
                                bincode::serialize(&(entry.node.clone(), *label)).unwrap(),
                                bincode::serialize(&entry.references).unwrap())
                            {
                                Err(e) => println!("{:?}", e),
                                _ => ()
                            }
                        }
                        else {
                            match maps_transaction.delete(bincode::serialize(&(entry.node.clone(), *label)).unwrap()) {
                                Err(e) => println!("{:?}", e),
                                _ => ()
                            }
                        }
                    }
                    match maps_transaction.commit() {
                        Err(e) => println!("{:?}", e),
                        _ => ()
                    }     
                });

                let handle_transaction = store.handles_db.transaction();
                match handle_transaction.put(bincode::serialize(&new_id).unwrap(), bincode::serialize(&root).unwrap()) {
                    Err(e) => println!("{:?}", e),
                    _ => ()
                }
                match handle_transaction.commit() {
                    Err(e) => println!("{:?}", e),
                    _ => ()
                }

                self.store.restore(store);
                Ok(table)   
            }
            None => {
                self.store.restore(store);
                Err("Don't recognise this id".to_string())
            }
        }
    }

    pub fn delete_table(&self, id: u32) -> Result<(), String> {
        let mut store = self.store.take(); 
        match store.handle_map.clone().get(&id) {
            Some ((root, counter)) => {
                // Check for count 2, because cloning the map to read the contents increases each counter by 1
                if Arc::strong_count(counter) == 2 {
                    store.handle_map.remove(&id);
                    // Persistence stuff
                    let mut map_changes = Snap::new(iter::repeat_with(|| Vec::new()).take(1 << DEPTH).collect());
                    drop::drop(&mut store, *root, &mut map_changes);
                    map_changes.par_iter().enumerate().for_each(|(idx, vec)| {
                        let maps_transaction = store.maps_db[idx].transaction();
                        for (entry, label, delete) in vec {
                            if !(*delete) {
                                match maps_transaction.put(
                                    bincode::serialize(&(entry.node.clone(), *label)).unwrap(),
                                    bincode::serialize(&entry.references).unwrap())
                                {
                                    Err(e) => println!("{:?}", e),
                                    _ => ()
                                }
                            }
                            else {
                                match maps_transaction.delete(bincode::serialize(&(entry.node.clone(), *label)).unwrap()) {
                                    Err(e) => println!("{:?}", e),
                                    _ => ()
                                }
                            }
                        }
                        match maps_transaction.commit() {
                            Err(e) => println!("{:?}", e),
                            _ => ()
                        }     
                    });
    
                    let handle_transaction = store.handles_db.transaction();
                    match handle_transaction.delete(bincode::serialize(&id).unwrap()) {
                        Err(e) => println!("{:?}", e),
                        _ => ()
                    }
                    match handle_transaction.commit() {
                        Err(e) => println!("{:?}", e),
                        _ => ()
                    }
    
                    self.store.restore(store);
                    Ok(())
                }
                else {
                    self.store.restore(store);
                    Err("Pending references to this id".to_string())   
                }
            }
            
            None => {
                self.store.restore(store);
                Err("Don't recognise this id".to_string())   
            }
        }
    }

    /// Creates a [`TableReceiver`] assigned to this `Database`. The
    /// receiver is used to efficiently receive a [`Table`]
    /// from other databases and add them this one.
    ///
    /// See [`TableReceiver`] for more details on its operation.
    ///
    /// # Examples
    ///
    /// ```
    /// use zebra::database::Database;
    /// let mut database: Database<&str, i32> = Database::new();
    ///
    /// let mut receiver = database.receive();
    ///
    /// // Do things with receiver...
    ///
    /// ```
    pub fn receive(&self) -> TableReceiver<Key, Value> {
        TableReceiver::new(self.store.clone())
    }

    pub fn recover(&self) 
    where
        Key: Field + for<'a> Deserialize<'a>,
        Value: Field + for<'a> Deserialize<'a>, 
    {
        let mut store = self.store.take();
        store.recover_maps();
        store.recover_handles();
        self.store.restore(store);
    }
}

impl<Key, Value> Clone for Database<Key, Value>
where
    Key: Field,
    Value: Field,
{
    fn clone(&self) -> Self {
        Database {
            store: self.store.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::database::{store::Label, TableTransaction};
    use std::time::Instant;

    impl<Key, Value> Database<Key, Value>
    where
        Key: Field,
        Value: Field,
    {
        pub(crate) fn table_with_records<I>(&mut self, records: I) -> Table<Key, Value>
        where
            I: IntoIterator<Item = (Key, Value)>,
        {
            let mut table = self.empty_table();
            let mut transaction = TableTransaction::new();

            for (key, value) in records {
                transaction.set(key, value).unwrap();
            }

            table.execute(transaction);
            table
        }

        pub(crate) fn check<'a, I, J>(&self, tables: I, receivers: J)
        where
            I: IntoIterator<Item = &'a Table<Key, Value>>,
            J: IntoIterator<Item = &'a TableReceiver<Key, Value>>,
        {
            let tables: Vec<&'a Table<Key, Value>> = tables.into_iter().collect();

            let receivers: Vec<&'a TableReceiver<Key, Value>> = receivers.into_iter().collect();

            for table in &tables {
                table.check_tree();
            }

            let table_held = tables.iter().map(|table| table.root());

            let receiver_held = receivers.iter().map(|receiver| receiver.held()).flatten();

            let held: Vec<Label> = table_held.chain(receiver_held).collect();

            let mut store = self.store.take();
            store.check_leaks(held.clone());
            store.check_references(held.clone());
            self.store.restore(store);
        }

        pub(crate) fn write_to_table(&self, table: &mut Table<u32, u32>, no_ops: u32, write_proportion: f32, iterations: u32) {
            let num_write = (no_ops as f32 * write_proportion) as u32;
            for iter in 0..iterations {    
                let mut transaction = TableTransaction::new();
                for i in 0..num_write {
                    transaction.set(i, i + iter + 1).unwrap();
                }
                for i in num_write..no_ops {
                    transaction.get(&i).unwrap();
                } 
                let start = Instant::now();
                let _ = table.execute(transaction);
                let duration = start.elapsed();
                println!("Time elapsed is: {:?}", duration);
            }
        }
    }

    #[test]
    fn modify_basic() {
        let mut database: Database<u32, u32> = Database::new();

        let mut table = database.table_with_records((0..256).map(|i| (i, i)));

        let mut transaction = TableTransaction::new();
        for i in 128..256 {
            transaction.set(i, i + 1).unwrap();
        }
        let _ = table.execute(transaction);
        table.assert_records((0..256).map(|i| (i, if i < 128 { i } else { i + 1 })));

        database.check([&table], []);
    }

    #[test]
    fn clone_modify_original() {
        let mut database: Database<u32, u32> = Database::new();

        let mut table = database.table_with_records((0..256).map(|i| (i, i)));
        let table_id = table.1;
        let table_clone = database.clone_table(0).unwrap();
        let table_clone_id = table_clone.1;

        let mut transaction = TableTransaction::new();
        for i in 128..256 {
            transaction.set(i, i + 1).unwrap();
        }
        let _response = table.execute(transaction);
        table.assert_records((0..256).map(|i| (i, if i < 128 { i } else { i + 1 })));
        table_clone.assert_records((0..256).map(|i| (i, i)));
        database.check([&table, &table_clone], []);
        
        drop(table_clone);
        match database.delete_table(table_clone_id) {
            Err(e) => { println!("{}", e) }
            _ => {}
        }
        table.assert_records((0..256).map(|i| (i, if i < 128 { i } else { i + 1 })));
        database.check([&table], []); 
        
        drop(table);
        match database.delete_table(table_id) {
            Err(e) => { println!("{}", e) }
            _ => {}
        }
        database.check([], []); 
    }

    #[test]
    fn clone_modify_drop() {
        let mut database: Database<u32, u32> = Database::new();

        let table = database.table_with_records((0..256).map(|i| (i, i)));
        let table_id = table.1;
        let mut table_clone = database.clone_table(table_id).unwrap();
        let table_clone_id = table_clone.1;
        table_clone.assert_records((0..256).map(|i| (i, i)));

        let mut transaction = TableTransaction::new();
        for i in 128..256 {
            transaction.set(i, i + 1).unwrap();
        }
        let _response = table_clone.execute(transaction);
        table_clone.assert_records((0..256).map(|i| (i, if i < 128 { i } else { i + 1 })));
        table.assert_records((0..256).map(|i| (i, i)));
        database.check([&table, &table_clone], []);
        
        drop(table_clone);
        match database.delete_table(table_clone_id) {
            Err(e) => { println!("{}", e) }
            _ => {}
        }
        table.assert_records((0..256).map(|i| (i, i)));
        database.check([&table], []);
        
        drop(table);
        match database.delete_table(table_id) {
            Err(e) => { println!("{}", e) }
            _ => {}
        }
        database.check([], []); 
    }

    #[test]
    fn modify_recover() {
        let mut database: Database<u32, u32> = Database::new();

        let mut first_table = database.table_with_records((0..10000).map(|i| (i, i)));
        let mut second_table = database.table_with_records((0..10000).map(|i| (i, i)));

        let mut first_transaction = TableTransaction::new();
        for i in 5000..10000 {
            first_transaction.set(i, i + 1).unwrap();
        }
        let mut second_transaction = TableTransaction::new();
        for i in 5000..10000 {
            second_transaction.set(i, i + 2).unwrap();
        }
        let _ = first_table.execute(first_transaction);
        first_table.assert_records((0..10000).map(|i| (i, if i < 5000 { i } else { i + 1 })));
        let _ = second_table.execute(second_transaction);
        second_table.assert_records((0..10000).map(|i| (i, if i < 5000 { i } else { i + 2 })));

        database.recover();
        let recovered_first_table = database.get_table(first_table.1).unwrap();
        recovered_first_table.assert_records((0..10000).map(|i| (i, if i < 5000 { i } else { i + 1 })));
        let recovered_second_table = database.get_table(second_table.1).unwrap();
        recovered_second_table.assert_records((0..10000).map(|i| (i, if i < 5000 { i } else { i + 2 })));
    }

    #[test]
    fn benchmark_no_operations() {
        let mut database: Database<u32, u32> = Database::new();
        let size = 100000;
        let mut table = database.table_with_records((0..size).map(|i| (i, i)));   
        let no_ops = [1000, 10000, 100000];
        for no_op in no_ops.iter() {
            println!("Benchark {:} operations", no_op);
            database.write_to_table(&mut table, *no_op, 0.5, 5);
        }  
    } 

    #[test]
    fn benchmark_write_proportion() {
        let mut database: Database<u32, u32> = Database::new();
        let size = 100000;
        let mut table = database.table_with_records((0..size).map(|i| (i, i)));   
        let write_props = [0.0, 0.25, 0.5, 1.0];
        for wp in write_props.iter(){
            println!("Benchark {:} write proportion", wp);
            database.write_to_table(&mut table, size, *wp, 5);
        }  
    }
}
