use crate::{
    common::{store::Field, tree::Path},
    database::{
        interact::{apply, diff, drop, export, Batch},
        store::{Cell, Label},
        store::Entry as MapEntry,
    },
    map::store::Node as MapNode,
};

use oh_snap::Snap;

use std::{
    collections::{hash_map::Entry, HashMap},
    hash::Hash as StdHash,
    ptr,
};

use std::vec::Vec;

use talk::crypto::primitives::hash::Hash;

pub(crate) struct Handle<Key: Field, Value: Field> {
    pub cell: Cell<Key, Value>,
    pub root: Label,
}

impl<Key, Value> Handle<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn empty(cell: Cell<Key, Value>) -> Self {
        Handle {
            cell,
            root: Label::Empty,
        }
    }

    pub fn new(cell: Cell<Key, Value>, root: Label) -> Self {
        Handle { cell, root }
    }

    pub fn commit(&self) -> Hash {
        self.root.hash().into()
    }

    pub fn apply(&mut self, batch: Batch<Key, Value>) -> (Batch<Key, Value>, Vec<(MapEntry<Key, Value>, bool)>) {
        let root = self.root;
        let store = self.cell.take();

        let (store, root, batch, map_changes) = apply::apply(store, root, batch);

        self.cell.restore(store);
        self.root = root;

        (batch, map_changes)
    }

    pub fn export(&mut self, paths: Snap<Path>) -> MapNode<Key, Value>
    where
        Key: Clone,
        Value: Clone,
    {
        let store = self.cell.take();
        let (store, root) = export::export(store, self.root, paths);
        self.cell.restore(store);

        root
    }

    pub fn diff(
        lho: &mut Handle<Key, Value>,
        rho: &mut Handle<Key, Value>,
    ) -> HashMap<Key, (Option<Value>, Option<Value>)>
    where
        Key: Clone + Eq + StdHash,
        Value: Clone + Eq,
    {
        if !ptr::eq(lho.cell.as_ref(), rho.cell.as_ref()) {
            panic!("called `Handle::diff` on two `Handle`s for different `Store`s (most likely, `Table::diff` / `Collection::diff` was called on two objects belonging to different `Database`s / `Family`-es)");
        }

        let store = lho.cell.take();

        let (store, lho_candidates, rho_candidates) = diff::diff(store, lho.root, rho.root);

        lho.cell.restore(store);

        let mut diff: HashMap<Key, (Option<Value>, Option<Value>)> = HashMap::new();

        for (key, value) in lho_candidates {
            let key = (**key.inner()).clone();
            let value = (**value.inner()).clone();

            diff.insert(key, (Some(value), None));
        }

        for (key, value) in rho_candidates {
            let key = (**key.inner()).clone();
            let value = (**value.inner()).clone();

            match diff.entry(key) {
                Entry::Occupied(mut entry) => {
                    if entry.get().0.as_ref().unwrap() == &value {
                        entry.remove_entry();
                    } else {
                        entry.get_mut().1 = Some(value);
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert((None, Some(value)));
                }
            }
        }

        diff
    }
}

impl<Key, Value> Clone for Handle<Key, Value>
where
    Key: Field,
    Value: Field,
{
    fn clone(&self) -> Self {
        let mut store = self.cell.take();
        let mut map_changes = Vec::new();
        store.incref(self.root, &mut map_changes);
        
        let maps_transaction = store.maps_db.transaction();
        for (entry, delete) in map_changes {
            if !delete {
                match maps_transaction.put(
                    bincode::serialize(&entry.node).unwrap(),
                    bincode::serialize(&entry.references).unwrap())
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

        let handles_transaction = store.handles_db.transaction();
        match handles_transaction.put(bincode::serialize(&store.handle_counter).unwrap(), bincode::serialize(&self.root).unwrap()) {
            Err(e) => println!("{:?}", e),
            _ => ()    
        }
        match handles_transaction.commit() {
            Err(e) => println!("{:?}", e),
            _ => ()
        }

        self.cell.restore(store);

        Handle {
            cell: self.cell.clone(),
            root: self.root,
        }
    }
}

impl<Key, Value> Drop for Handle<Key, Value>
where
    Key: Field,
    Value: Field,
{
    fn drop(&mut self) {
        let mut store = self.cell.take();
        let mut map_changes = Vec::new();
        drop::drop(&mut store, self.root, &mut map_changes);
        
        let maps_transaction = store.maps_db.transaction();
        for (entry, delete) in map_changes {
            if !delete {
                match maps_transaction.put(
                    bincode::serialize(&entry.node).unwrap(),
                    bincode::serialize(&entry.references).unwrap())
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

        let handles_transaction = store.handles_db.transaction();
        match handles_transaction.delete(bincode::serialize(&store.handle_counter).unwrap()) {
            Err(e) => println!("{:?}", e),
            _ => ()    
        }
        match handles_transaction.commit() {
            Err(e) => println!("{:?}", e),
            _ => ()
        }

        self.cell.restore(store);
    }
}
