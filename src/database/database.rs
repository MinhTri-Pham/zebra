use crate::{
    common::store::Field,
    database::{
        store::{Cell, Store, Handle},
        interact::drop,
        Table, TableReceiver,
    },
};

use talk::sync::lenders::AtomicLender;

use rocksdb::TransactionDB;
use std::time::SystemTime;
use std::sync::Arc;

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
    pub(crate) maps_db: Arc<TransactionDB>,
    pub(crate) handles_db: Arc<TransactionDB>, 
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
        let path = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros().to_string();
        let full_maps = "logs_maps/".to_owned() + &path;
        let full_handles = "logs_handles/".to_owned() + &path;
        let maps_db_pointer = Arc::new(TransactionDB::open_default(full_maps).unwrap());
        let handles_db_pointer = Arc::new(TransactionDB::open_default(full_handles).unwrap());
        Database {
            store: Cell::new(AtomicLender::new(Store::new(maps_db_pointer.clone(), handles_db_pointer.clone()))),
            maps_db: maps_db_pointer,
            handles_db: handles_db_pointer,
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
        let table = Table::empty(self.store.clone());
        let root = table.get_root();
        store.handle_map.insert(id, Arc::new(root));

        let handle_transaction = self.handles_db.transaction();
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
            let root = root.unwrap();
            let handle = Handle::new(self.store.clone(), *root.clone());
            let table = Ok(Table::from_handle(handle));
            self.store.restore(store);
            table
        }
        else {
            self.store.restore(store);
            return Err("Don't recognise this id".to_string());
        }
    }

    pub fn clone_table(&self, id: u32) -> Result<(u32, Table<Key, Value>), String> {
        let mut store = self.store.take(); 
        match store.handle_map.clone().get(&id) {
            Some(root) => {
                let handle = Handle::new(self.store.clone(), **root);
                let new_id = store.handle_counter;
                let result = Ok((new_id, Table::from_handle(handle)));
                store.handle_map.insert(new_id, root.clone());
                store.handle_counter += 1;
                
                // Persistence stuff
                let mut map_changes = Vec::new();
                store.incref(**root, &mut map_changes);
                let maps_transaction = self.maps_db.transaction();
                for (entry, delete) in map_changes {
                    if !delete {
                        match maps_transaction.put(bincode::serialize(&entry.node).unwrap(),bincode::serialize(&entry.references).unwrap())
                        {
                            Err(e) => println!("{:?}", e),
                            _ => ()
                        }
                    }
                    else {
                        match maps_transaction.delete(bincode::serialize(&entry.node).unwrap()) {
                            Err(e) => println!("{:?}", e),
                            _ => ()
                        }
                    }
                }
                match maps_transaction.commit() {
                    Err(e) => println!("{:?}", e),
                    _ => ()
                }

                let handle_transaction = self.handles_db.transaction();
                match handle_transaction.put(bincode::serialize(&new_id).unwrap(), bincode::serialize(root).unwrap()) {
                    Err(e) => println!("{:?}", e),
                    _ => ()
                }
                match handle_transaction.commit() {
                    Err(e) => println!("{:?}", e),
                    _ => ()
                }

                self.store.restore(store);
                result   
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
            Some (root) => {
                if Arc::strong_count(&root) == 1 {
                    store.handle_map.remove(&id);
                    // Persistence stuff
                    let mut map_changes = Vec::new();
                    drop::drop(&mut store, **root, &mut map_changes);
                    let maps_transaction = self.maps_db.transaction();
                    for (entry, delete) in map_changes {
                        if !delete {
                            match maps_transaction.put(bincode::serialize(&entry.node).unwrap(),bincode::serialize(&entry.references).unwrap())
                            {
                                Err(e) => println!("{:?}", e),
                                _ => ()
                            }
                        }
                        else {
                            match maps_transaction.delete(bincode::serialize(&entry.node).unwrap()) {
                                Err(e) => println!("{:?}", e),
                                _ => ()
                            }
                        }
                    }
                    match maps_transaction.commit() {
                        Err(e) => println!("{:?}", e),
                        _ => ()
                    }
    
                    let handle_transaction = self.handles_db.transaction();
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
                    return Err("Pending references to this id".to_string());       
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
}

impl<Key, Value> Clone for Database<Key, Value>
where
    Key: Field,
    Value: Field,
{
    fn clone(&self) -> Self {
        Database {
            store: self.store.clone(),
            maps_db: self.maps_db.clone(),
            handles_db: self.handles_db.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::database::{store::Label, TableTransaction};

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
        let table_clone = table.clone();

        let mut transaction = TableTransaction::new();
        for i in 128..256 {
            transaction.set(i, i + 1).unwrap();
        }
        let _response = table.execute(transaction);
        table.assert_records((0..256).map(|i| (i, if i < 128 { i } else { i + 1 })));
        table_clone.assert_records((0..256).map(|i| (i, i)));

        database.check([&table, &table_clone], []);
        drop(table_clone);

        table.assert_records((0..256).map(|i| (i, if i < 128 { i } else { i + 1 })));
        database.check([&table], []);
    }

    #[test]
    fn clone_modify_drop() {
        let mut database: Database<u32, u32> = Database::new();

        let table = database.table_with_records((0..256).map(|i| (i, i)));
        let mut table_clone = table.clone();

        let mut transaction = TableTransaction::new();
        for i in 128..256 {
            transaction.set(i, i + 1).unwrap();
        }
        let _response = table_clone.execute(transaction);
        table_clone.assert_records((0..256).map(|i| (i, if i < 128 { i } else { i + 1 })));
        table.assert_records((0..256).map(|i| (i, i)));

        database.check([&table, &table_clone], []);
        drop(table_clone);

        table.assert_records((0..256).map(|i| (i, i)));
        database.check([&table], []);
    }
}
