use crate::{
    common::{data::Bytes, store::Field, tree::Prefix},
    database::store::{Entry, Label, MapId, Node, Split},
};

use oh_snap::Snap;

use std::{
    collections::{
        hash_map::{
            Entry as HashMapEntry,
            Entry::{Occupied, Vacant},
        },
        HashMap,
    },
    iter,
    sync::Arc,
    time::SystemTime,
};

use rocksdb::TransactionDB;
use bincode;
use serde::Deserialize;

pub(crate) type EntryMap<Key, Value> = HashMap<Bytes, Entry<Key, Value>>;
pub(crate) type EntryMapEntry<'a, Key, Value> = HashMapEntry<'a, Bytes, Entry<Key, Value>>;

pub(crate) const DEPTH: u8 = 8;

pub(crate) struct Store<Key: Field, Value: Field> {
    maps: Snap<EntryMap<Key, Value>>,
    scope: Prefix,
    pub(crate) maps_db: Arc<TransactionDB>,
    pub(crate) handles_db: Arc<TransactionDB>,
    pub(crate) handle_map: HashMap<u32, (Label, Arc<()>)>,
    pub(crate) handle_counter: u32,
}

impl<Key, Value> Store<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn new() -> Self {
        let path = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros().to_string();
        let full_maps = "logs/".to_owned() + &path + "/logs_maps";
        let full_handles = "logs/".to_owned() + &path + "/logs_handles";
        let maps_db_pointer = Arc::new(TransactionDB::open_default(full_maps).unwrap());
        let handles_db_pointer = Arc::new(TransactionDB::open_default(full_handles).unwrap());
        Store {
            maps: Snap::new(
                iter::repeat_with(|| EntryMap::new())
                    .take(1 << DEPTH)
                    .collect(),
            ),
            scope: Prefix::root(),
            maps_db: maps_db_pointer,
            handles_db: handles_db_pointer,
            handle_map: HashMap::new(),
            handle_counter: 0,
        }
    }

    #[cfg(test)]
    pub fn from_path(path: &str) -> Self {
        Store {
            maps: Snap::new(
                iter::repeat_with(|| EntryMap::new())
                    .take(1 << DEPTH)
                    .collect(),
            ),
            scope: Prefix::root(),
            maps_db: Arc::new(TransactionDB::open_default(path.to_owned() + "/maps/").unwrap()),
            handles_db: Arc::new(TransactionDB::open_default(path.to_owned() + "/handles/").unwrap()),
            handle_map: HashMap::new(),
            handle_counter: 0,
        }
    }

    pub fn merge(left: Self, right: Self) -> Self {
        Store {
            maps: Snap::merge(right.maps, left.maps),
            scope: left.scope.ancestor(1),
            maps_db: left.maps_db,
            handles_db: left.handles_db,
            handle_map: left.handle_map,
            handle_counter: left.handle_counter,
        }
    }

    pub fn split(self) -> Split<Key, Value> {
        if self.scope.depth() < DEPTH {
            let mid = 1 << (DEPTH - self.scope.depth() - 1);

            let (right_maps, left_maps) = self.maps.snap(mid); // `oh-snap` stores the lowest-index elements in `left`, while `zebra` stores them in `right`, hence the swap

            let left = Store {
                maps: left_maps,
                scope: self.scope.left(),
                maps_db: self.maps_db.clone(),
                handles_db: self.handles_db.clone(),
                handle_map: self.handle_map.clone(),
                handle_counter: self.handle_counter.clone(),
            };

            let right = Store {
                maps: right_maps,
                scope: self.scope.right(),
                maps_db: self.maps_db.clone(),
                handles_db: self.handles_db.clone(),
                handle_map: self.handle_map.clone(),
                handle_counter: self.handle_counter.clone(),
            };

            Split::Split(left, right)
        } else {
            Split::Unsplittable(self)
        }
    }

    #[cfg(test)]
    pub fn size(&self) -> usize {
        debug_assert!(self.maps.is_complete());
        self.maps.iter().map(|map| map.len()).sum()
    }

    pub fn entry(&mut self, label: Label) -> EntryMapEntry<Key, Value> {
        let map = label.map().id() - self.maps.range().start;
        let hash = label.hash();
        self.maps[map].entry(hash)
    }

    pub fn label(&self, node: &Node<Key, Value>) -> Label {
        let hash = node.hash();

        match node {
            Node::Empty => Label::Empty,
            Node::Internal(..) => {
                let map = MapId::internal(self.scope);
                Label::Internal(map, hash)
            }
            Node::Leaf(key, _) => {
                let map = MapId::leaf(&key.digest());
                Label::Leaf(map, hash)
            }
        }
    }

    pub fn populate(&mut self, label: Label, node: Node<Key, Value>, map_changes: &mut Vec<(Entry<Key, Value>, Label, bool)>) -> bool
    where
        Key: Field,
        Value: Field,
    {
        if !label.is_empty() {
            match self.entry(label) {
                Vacant(entry) => {
                    let entry_clone = Entry {
                        node: node.clone(),
                        references: 0,
                    };
                    entry.insert(Entry {
                        node,
                        references: 0,
                    });
                    map_changes.push((entry_clone, label, false));
                    true
                }
                Occupied(..) => false,
            }
        } else {
            false
        }
    }

    pub fn incref(&mut self, label: Label, map_changes: &mut Vec<(Entry<Key, Value>, Label, bool)>)
    where
        Key: Field,
        Value: Field,
    {
        if !label.is_empty() {
            match self.entry(label) {
                Occupied(mut entry) => {
                    let value = entry.get_mut();
                    value.references += 1;
                    let entry_clone = Entry {
                        node: value.node.clone(),
                        references: value.references,
                    };
                    map_changes.push((entry_clone, label, false));
                }
                Vacant(..) => panic!("called `incref` on non-existing node"),
            }
        }
    }

    pub fn decref(&mut self, label: Label, preserve: bool, map_changes: &mut Vec<(Entry<Key, Value>, Label, bool)>) -> Option<Node<Key, Value>>
    where
        Key: Field,
        Value: Field,
    {
        if !label.is_empty() {
            match self.entry(label) {
                Occupied(mut entry) => {
                    let value = entry.get_mut();
                    value.references -= 1;
                    let entry_clone = Entry {
                        node: value.node.clone(),
                        references: value.references,
                    };

                    if value.references == 0 && !preserve {
                        let (_, entry) = entry.remove_entry();
                        map_changes.push((entry_clone, label, true));
                        Some(entry.node)
                        
                    } else {
                        map_changes.push((entry_clone, label, false));
                        None
                    }
                }
                Vacant(..) => panic!("called `decref` on non-existing node"),
            }
        } else {
            None
        }
    }

    pub fn recover_maps(&mut self)
    where
        Key: Field +  for<'a> Deserialize<'a>,
        Value: Field +  for<'a> Deserialize<'a>, 
    {
        self.maps = Snap::new(
            iter::repeat_with(|| EntryMap::new())
                .take(1 << DEPTH)
                .collect());
        let mut iter = self.maps_db.raw_iterator();
        iter.seek_to_first();
        while iter.valid() {
            let (node, label): (Node<Key, Value>, Label) = bincode::deserialize(&(iter.key().unwrap())).unwrap();
            let references: usize = bincode::deserialize(&(iter.value().unwrap())).unwrap();
            let index = label.map().id() - self.maps.range().start;
            let hash = label.hash();
            self.maps[index].insert(hash, Entry {node, references});
            iter.next();
        }
    }

    pub fn recover_handles(&mut self)
    where
        Key: Field +  for<'a> Deserialize<'a>,
        Value: Field +  for<'a> Deserialize<'a>, 
    {
        self.handle_map = HashMap::new();
        let mut iter = self.handles_db.raw_iterator();
        iter.seek_to_first();
        while iter.valid() {
            let id: u32 = bincode::deserialize(&(iter.key().unwrap())).unwrap();
            let root: Label = bincode::deserialize(&(iter.value().unwrap())).unwrap();
            self.handle_map.insert(id, (root, Arc::new(())));
            iter.next();
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        common::tree::{Direction, Path},
        database::store::{Entry, Node, Wrap},
    };

    use std::{collections::HashSet, fmt::Debug, hash::Hash};

    impl<Key, Value> Store<Key, Value>
    where
        Key: Field,
        Value: Field,
    {
        pub fn raw_leaves<I>(leaves: I, path: &str) -> (Self, Vec<Label>)
        where
            I: IntoIterator<Item = (Key, Value)>,
        {
            let mut store = Store::from_path(path);

            let labels = leaves
                .into_iter()
                .map(|(key, value)| {
                    let key = wrap!(key);
                    let value = wrap!(value);

                    let node = Node::Leaf(key, value);
                    let label = store.label(&node);

                    let entry = Entry {
                        node,
                        references: 1,
                    };

                    match store.entry(label) {
                        EntryMapEntry::Vacant(entrymapentry) => {
                            entrymapentry.insert(entry);
                        }
                        _ => unreachable!(),
                    }

                    label
                })
                .collect();

            (store, labels)
        }

        pub fn fetch_node(&mut self, label: Label) -> Node<Key, Value> {
            match self.entry(label) {
                Occupied(entry) => entry.get().node.clone(),
                Vacant(..) => panic!("`fetch_node`: node not found"),
            }
        }

        pub fn fetch_internal(&mut self, label: Label) -> (Label, Label) {
            match self.fetch_node(label) {
                Node::Internal(left, right) => (left, right),
                _ => panic!("`fetch_internal`: node not `Internal`"),
            }
        }

        pub fn fetch_leaf(&mut self, label: Label) -> (Wrap<Key>, Wrap<Value>) {
            match self.fetch_node(label) {
                Node::Leaf(key, value) => (key, value),
                _ => panic!("`fetch_leaf`: node not `Leaf`"),
            }
        }

        pub fn fetch_label_at(&mut self, root: Label, location: Prefix) -> Label {
            let mut next = root;

            for direction in location {
                next = match (self.fetch_node(next), direction) {
                    (Node::Internal(next, _), Direction::Left)
                    | (Node::Internal(_, next), Direction::Right) => next,
                    _ => panic!("`label_at`: reached a dead end"),
                };
            }

            next
        }

        pub fn check_internal(&mut self, label: Label) {
            let (left, right) = self.fetch_internal(label);

            match (left, right) {
                (Label::Empty, Label::Empty)
                | (Label::Empty, Label::Leaf(..))
                | (Label::Leaf(..), Label::Empty) => {
                    panic!("`check_internal`: children violate compactness")
                }
                _ => {}
            }

            for child in [left, right] {
                if child != Label::Empty {
                    if let Vacant(..) = self.entry(child) {
                        panic!("`check_internal`: child not found");
                    }
                }
            }
        }

        pub fn check_leaf(&mut self, label: Label, location: Prefix) {
            let (key, _) = self.fetch_leaf(label);
            if !location.contains(&Path::from(key.digest())) {
                panic!("`check_leaf`: leaf outside of its key path")
            }
        }

        pub fn check_tree(&mut self, root: Label) {
            fn recursion<Key, Value>(store: &mut Store<Key, Value>, label: Label, location: Prefix)
            where
                Key: Field,
                Value: Field,
            {
                match label {
                    Label::Internal(..) => {
                        store.check_internal(label);

                        let (left, right) = store.fetch_internal(label);
                        recursion(store, left, location.left());
                        recursion(store, right, location.right());
                    }
                    Label::Leaf(..) => {
                        store.check_leaf(label, location);
                    }
                    Label::Empty => {}
                }
            }

            recursion(self, root, Prefix::root());
        }

        pub fn collect_tree(&mut self, root: Label) -> HashSet<Label> {
            let mut collector = HashSet::new();

            fn recursion<Key, Value>(
                store: &mut Store<Key, Value>,
                label: Label,
                collector: &mut HashSet<Label>,
            ) where
                Key: Field,
                Value: Field,
            {
                if !label.is_empty() {
                    collector.insert(label);
                }

                if let Label::Internal(..) = label {
                    let (left, right) = store.fetch_internal(label);
                    recursion(store, left, collector);
                    recursion(store, right, collector);
                }
            }

            recursion(self, root, &mut collector);
            collector
        }

        pub fn check_leaks<I>(&mut self, held: I)
        where
            I: IntoIterator<Item = Label>,
        {
            let mut labels = HashSet::new();

            for root in held {
                labels.extend(self.collect_tree(root));
            }

            if self.size() > labels.len() {
                panic!("`check_leaks`: unreachable entries detected");
            }
        }

        pub fn check_references<I>(&mut self, held: I)
        where
            I: IntoIterator<Item = Label>,
        {
            #[derive(Hash, PartialEq, Eq)]
            enum Reference {
                Internal(Label),
                External(usize),
            }

            fn recursion<Key, Value>(
                store: &mut Store<Key, Value>,
                label: Label,
                references: &mut HashMap<Label, HashSet<Reference>>,
            ) where
                Key: Field,
                Value: Field,
            {
                if let Label::Internal(..) = label {
                    let (left, right) = store.fetch_internal(label);

                    for child in [left, right] {
                        references
                            .entry(child)
                            .or_insert(HashSet::new())
                            .insert(Reference::Internal(label));

                        recursion(store, child, references);
                    }
                }
            }

            let mut references: HashMap<Label, HashSet<Reference>> = HashMap::new();

            for (id, held) in held.into_iter().enumerate() {
                references
                    .entry(held)
                    .or_insert(HashSet::new())
                    .insert(Reference::External(id));

                recursion(self, held, &mut references);
            }

            for (label, references) in references {
                if !label.is_empty() {
                    match self.entry(label) {
                        Occupied(entry) => {
                            assert_eq!(entry.get().references, references.len());
                        }
                        Vacant(..) => unreachable!(),
                    }
                }
            }
        }

        pub fn collect_records(&mut self, root: Label) -> HashMap<Key, Value>
        where
            Key: Clone + Eq + Hash,
            Value: Clone,
        {
            fn recursion<Key, Value>(
                store: &mut Store<Key, Value>,
                label: Label,
                collector: &mut HashMap<Key, Value>,
            ) where
                Key: Field + Clone + Eq + Hash,
                Value: Field + Clone,
            {
                match label {
                    Label::Internal(..) => {
                        let (left, right) = store.fetch_internal(label);
                        recursion(store, left, collector);
                        recursion(store, right, collector);
                    }
                    Label::Leaf(..) => {
                        let (key, value) = store.fetch_leaf(label);
                        collector.insert((**key.inner()).clone(), (**value.inner()).clone());
                    }
                    Label::Empty => {}
                }
            }

            let mut collector = HashMap::new();
            recursion(self, root, &mut collector);
            collector
        }

        pub fn assert_records<I>(&mut self, root: Label, reference: I)
        where
            Key: Debug + Clone + Eq + Hash,
            Value: Debug + Clone + Eq + Hash,
            I: IntoIterator<Item = (Key, Value)>,
        {
            let actual: HashSet<(Key, Value)> = self.collect_records(root).into_iter().collect();

            let reference: HashSet<(Key, Value)> = reference.into_iter().collect();

            let differences: HashSet<(Key, Value)> = reference
                .symmetric_difference(&actual)
                .map(|r| r.clone())
                .collect();

            assert_eq!(differences, HashSet::new());
        }
    }

    #[test]
    fn split() {
        let (mut store, labels) = Store::raw_leaves([(0u32, 1u32)], "logs/store/split");

        let path = Path::from(wrap!(0u32).digest());
        let label = labels[0];

        for splits in 0..DEPTH {
            store = match store.split() {
                Split::Split(left, right) => {
                    if path[splits] == Direction::Left {
                        left
                    } else {
                        right
                    }
                }
                Split::Unsplittable(_) => unreachable!(),
            };

            match store.entry(label) {
                EntryMapEntry::Occupied(..) => {}
                _ => {
                    unreachable!();
                }
            }
        }

        for _ in DEPTH..=255 {
            store = match store.split() {
                Split::Split(_, _) => unreachable!(),
                Split::Unsplittable(store) => store,
            };

            match store.entry(label) {
                EntryMapEntry::Occupied(..) => {}
                _ => {
                    unreachable!();
                }
            }
        }
    }

    #[test]
    fn merge() {
        let leaves = (0..=8).map(|i| (i, i));
        let (store, labels) = Store::raw_leaves(leaves, "logs/store/merge");

        let (l, r) = match store.split() {
            Split::Split(l, r) => (l, r),
            Split::Unsplittable(..) => unreachable!(),
        };

        let (ll, lr) = match l.split() {
            Split::Split(l, r) => (l, r),
            Split::Unsplittable(..) => unreachable!(),
        };

        let (rl, rr) = match r.split() {
            Split::Split(l, r) => (l, r),
            Split::Unsplittable(..) => unreachable!(),
        };

        let l = Store::merge(ll, lr);
        let r = Store::merge(rl, rr);

        let mut store = Store::merge(l, r);

        for (index, label) in labels.into_iter().enumerate() {
            match store.entry(label) {
                EntryMapEntry::Occupied(entry) => match &entry.get().node {
                    Node::Leaf(key, value) => {
                        assert_eq!(*key, wrap!(index));
                        assert_eq!(*value, wrap!(index));
                    }
                    _ => unreachable!(),
                },
                _ => {
                    unreachable!();
                }
            }
        }
    }

    #[test]
    fn size() {
        let store = Store::<u32, u32>::from_path("logs/store/size_1");
        assert_eq!(store.size(), 0);

        let leaves = (0..=8).map(|i| (i, i));
        let (store, _) = Store::raw_leaves(leaves, "logs/store/size_2");

        assert_eq!(store.size(), 9);
    }
}
