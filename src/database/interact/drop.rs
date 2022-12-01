use crate::{
    common::store::Field,
    database::store::{Label, Node, Store, Entry},
};

use oh_snap::Snap;

pub(crate) fn drop<Key, Value>(store: &mut Store<Key, Value>, label: Label, map_changes: &mut Snap<Vec<(Entry<Key, Value>, Label, bool)>>)
where
    Key: Field,
    Value: Field,
{
    match store.decref(label, false, map_changes) {
        Some(Node::Internal(left, right)) => {
            drop(store, left, map_changes);
            drop(store, right, map_changes);
        }
        _ => (),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::database::interact::{apply, Batch};

    use rand::{seq::IteratorRandom, Rng};

    #[test]
    fn single() {
        let store = Store::<u32, u32>::from_path("logs/drop/single");

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (mut store, root, _, mut map_changes) = apply::apply(store, Label::Empty, batch);
        store.check_leaks([root]);

        drop(&mut store, root, &mut map_changes);
        store.check_leaks([]);
    }

    #[test]
    fn double_independent() {
        let store = Store::<u32, u32>::from_path("logs/drop/double_independent");

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (mut store, first_root, _, mut first_map_changes) = apply::apply(store, Label::Empty, batch);
        store.check_leaks([first_root]);

        let batch = Batch::new((128..256).map(|i| set!(i, i)).collect());
        let (mut store, second_root, _, mut second_map_changes) = apply::apply(store, Label::Empty, batch);
        store.check_leaks([first_root, second_root]);

        drop(&mut store, first_root, &mut first_map_changes);
        store.check_leaks([second_root]);

        drop(&mut store, second_root, &mut second_map_changes);
        store.check_leaks([]);
    }

    #[test]
    fn double_same() {
        let store = Store::<u32, u32>::from_path("logs/drop/double_same");

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (mut store, first_root, _, mut first_map_changes) = apply::apply(store, Label::Empty, batch);
        store.check_leaks([first_root]);

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (mut store, second_root, _, mut second_map_changes) = apply::apply(store, Label::Empty, batch);
        store.check_leaks([first_root, second_root]);

        drop(&mut store, first_root, &mut first_map_changes);
        store.check_leaks([second_root]);

        drop(&mut store, second_root, &mut second_map_changes);
        store.check_leaks([]);
    }

    #[test]
    fn double_overlap() {
        let store = Store::<u32, u32>::from_path("logs/drop/double_overlap");

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (mut store, first_root, _, mut first_map_changes) = apply::apply(store, Label::Empty, batch);
        store.check_leaks([first_root]);

        let batch = Batch::new((64..192).map(|i| set!(i, i)).collect());
        let (mut store, second_root, _, mut second_map_changes) = apply::apply(store, Label::Empty, batch);
        store.check_leaks([first_root, second_root]);

        drop(&mut store, first_root, &mut first_map_changes);
        store.check_leaks([second_root]);

        drop(&mut store, second_root, &mut second_map_changes);
        store.check_leaks([]);
    }

    #[test]
    fn stress() {
        let mut rng = rand::thread_rng();
        let mut roots: Vec<Label> = Vec::new();

        let mut store = Store::<u32, u32>::from_path("logs/drop/stress");
        let mut map_changes_vec = Vec::new();

        for _ in 0..32 {
            if rng.gen::<bool>() {
                let keys = (0..1024).choose_multiple(&mut rng, 128);
                let batch = Batch::new(keys.iter().map(|&i| set!(i, i)).collect());

                let result = apply::apply(store, Label::Empty, batch);
                store = result.0;
                roots.push(result.1);
                map_changes_vec.push(result.3);
            } else {
                if let Some(index) = (0..roots.len()).choose(&mut rng) {
                    drop(&mut store, roots[index], &mut map_changes_vec[index]);
                    roots.remove(index);
                }
            }

            store.check_leaks(roots.clone());
        }
    }
}
