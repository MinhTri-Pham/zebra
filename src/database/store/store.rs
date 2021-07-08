use crate::database::{
    data::Bytes,
    store::{Entry, Field, Label, MapId, Node, Split},
    tree::Prefix,
};

use oh_snap::Snap;

use std::collections::hash_map::Entry as HashMapEntry;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::iter;

pub(crate) type EntryMap<Key, Value> = HashMap<Bytes, Entry<Key, Value>>;
pub(crate) type EntryMapEntry<'a, Key, Value> =
    HashMapEntry<'a, Bytes, Entry<Key, Value>>;

pub(crate) const DEPTH: u8 = 8;

pub(crate) struct Store<Key: Field, Value: Field> {
    maps: Snap<EntryMap<Key, Value>>,
    scope: Prefix,
}

impl<Key, Value> Store<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn new() -> Self {
        Store {
            maps: Snap::new(
                iter::repeat_with(|| EntryMap::new())
                    .take(1 << DEPTH)
                    .collect(),
            ),
            scope: Prefix::root(),
        }
    }

    pub fn merge(left: Self, right: Self) -> Self {
        Store {
            maps: Snap::merge(right.maps, left.maps),
            scope: left.scope.ancestor(1),
        }
    }

    pub fn split(self) -> Split<Key, Value> {
        if self.scope.depth() < DEPTH {
            let mid = 1 << (DEPTH - self.scope.depth() - 1);

            let (right_maps, left_maps) = self.maps.snap(mid); // `oh-snap` stores the lowest-index elements in `left`, while `zebra` stores them in `right`, hence the swap

            let left = Store {
                maps: left_maps,
                scope: self.scope.left(),
            };

            let right = Store {
                maps: right_maps,
                scope: self.scope.right(),
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
        let hash = *label.hash();
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

    pub fn populate(&mut self, label: Label, node: Node<Key, Value>) -> bool
    where
        Key: Field,
        Value: Field,
    {
        if !label.is_empty() {
            match self.entry(label) {
                Vacant(entry) => {
                    entry.insert(Entry {
                        node,
                        references: 0,
                    });

                    true
                }
                Occupied(..) => false,
            }
        } else {
            false
        }
    }

    pub fn incref(&mut self, label: Label)
    where
        Key: Field,
        Value: Field,
    {
        if !label.is_empty() {
            match self.entry(label) {
                Occupied(mut entry) => {
                    entry.get_mut().references += 1;
                }
                Vacant(..) => panic!("called `incref` on non-existing node"),
            }
        }
    }

    pub fn decref(
        &mut self,
        label: Label,
        preserve: bool,
    ) -> Option<Node<Key, Value>>
    where
        Key: Field,
        Value: Field,
    {
        if !label.is_empty() {
            match self.entry(label) {
                Occupied(mut entry) => {
                    let value = entry.get_mut();
                    value.references -= 1;

                    if value.references == 0 && !preserve {
                        let (_, entry) = entry.remove_entry();
                        Some(entry.node)
                    } else {
                        None
                    }
                }
                Vacant(..) => panic!("called `decref` on non-existing node"),
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::database::{
        store::{Entry, Node, Wrap},
        tree::{Direction, Path},
    };

    use std::collections::HashSet;
    use std::hash::Hash;

    impl<Key, Value> Store<Key, Value>
    where
        Key: Field,
        Value: Field,
    {
        pub fn raw_leaves<I>(leaves: I) -> (Self, Vec<Label>)
        where
            I: IntoIterator<Item = (Key, Value)>,
        {
            let mut store = Store::new();

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
            if !location.contains(&Path::from(*key.digest())) {
                panic!("`check_leaf`: leaf outside of its key path")
            }
        }

        pub fn check_tree(&mut self, root: Label, location: Prefix) {
            match root {
                Label::Internal(..) => {
                    self.check_internal(root);

                    let (left, right) = self.fetch_internal(root);
                    self.check_tree(left, location.left());
                    self.check_tree(right, location.right());
                }
                Label::Leaf(..) => {
                    self.check_leaf(root, location);
                }
                Label::Empty => {}
            }
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

                match label {
                    Label::Internal(..) => {
                        let (left, right) = store.fetch_internal(label);
                        recursion(store, left, collector);
                        recursion(store, right, collector);
                    }
                    _ => {}
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
                        collector.insert(
                            (**key.inner()).clone(),
                            (**value.inner()).clone(),
                        );
                    }
                    Label::Empty => {}
                }
            }

            let mut collector = HashMap::new();
            recursion(self, root, &mut collector);
            collector
        }
    }

    #[test]
    fn split() {
        let (mut store, labels) = Store::raw_leaves([(0u32, 1u32)]);

        let path = Path::from(*wrap!(0u32).digest());
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
        let (store, labels) = Store::raw_leaves(leaves);

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
        let store = Store::<u32, u32>::new();
        assert_eq!(store.size(), 0);

        let leaves = (0..=8).map(|i| (i, i));
        let (store, _) = Store::raw_leaves(leaves);

        assert_eq!(store.size(), 9);
    }
}
