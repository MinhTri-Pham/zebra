use crate::{
    common::{data::Bytes, store::Field},
    database::{
        errors::{MalformedAnswer, SyncError},
        interact::drop,
        store::{Cell, Label, MapId, Node, Store},
        sync::{locate, Severity},
        tree::Prefix,
        Answer, Question, Table,
    },
};

use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::{HashMap, HashSet};

const DEFAULT_WINDOW: usize = 128;

pub struct Receiver<Key: Field, Value: Field> {
    cell: Cell<Key, Value>,
    root: Option<Label>,
    held: HashSet<Label>,
    frontier: HashMap<Bytes, Context>,
    acquired: HashMap<Label, Node<Key, Value>>,
    pub settings: Settings,
}

pub struct Settings {
    pub window: usize,
}

pub enum Status<Key: Field, Value: Field> {
    Complete(Table<Key, Value>),
    Incomplete(Receiver<Key, Value>, Question),
}

struct Context {
    location: Prefix,
    remote_label: Label,
}

impl<Key, Value> Receiver<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub(crate) fn new(cell: Cell<Key, Value>) -> Self {
        Receiver {
            cell,
            root: None,
            held: HashSet::new(),
            frontier: HashMap::new(),
            acquired: HashMap::new(),
            settings: Settings {
                window: DEFAULT_WINDOW,
            },
        }
    }

    pub fn learn(
        mut self,
        answer: Answer<Key, Value>,
    ) -> Result<Status<Key, Value>, SyncError> {
        let mut store = self.cell.take();
        let mut severity = Severity::ok();

        for node in answer.0 {
            severity = match self.update(&mut store, node) {
                Ok(()) => Severity::ok(),
                Err(offence) => severity + offence,
            };

            if severity.is_malicious() {
                break;
            }
        }

        if severity.is_benign() {
            if self.frontier.is_empty() {
                // Receive complete, flush if necessary
                match self.root {
                    Some(root) => {
                        // At least one node was received: flush
                        self.flush(&mut store, root);
                        self.cell.restore(store);

                        Ok(Status::Complete(Table::new(
                            self.cell.clone(),
                            root,
                        )))
                    }
                    None => {
                        // No node received: the new table's `root` should be `Empty`
                        self.cell.restore(store);
                        Ok(Status::Complete(Table::new(
                            self.cell.clone(),
                            Label::Empty,
                        )))
                    }
                }
            } else {
                // Receive incomplete, carry on with new `Question`
                self.cell.restore(store);
                let question = self.ask();

                Ok(Status::Incomplete(self, question))
            }
        } else {
            self.cell.restore(store);
            MalformedAnswer.fail()
        }
    }

    fn update(
        &mut self,
        store: &mut Store<Key, Value>,
        node: Node<Key, Value>,
    ) -> Result<(), Severity> {
        let hash = node.hash();

        let location = if self.root.is_some() {
            // Check if `hash` is in `frontier`. If so, retrieve `location`.
            Ok(self.frontier.get(&hash).ok_or(Severity::benign())?.location)
        } else {
            // This is the first `node` fed in `update`. By convention, `node` is the root.
            Ok(Prefix::root())
        }?;

        // Check if `node` preserves topology invariants:
        // - If `node` is `Internal`, its children must preserve compactness.
        // - If `node` is `Leaf`, it must lie along its `key` path.
        // If so, compute `node`'s `label`.
        let label = match node {
            Node::Internal(left, right) => match (left, right) {
                (Label::Empty, Label::Empty)
                | (Label::Empty, Label::Leaf(..))
                | (Label::Leaf(..), Label::Empty) => Err(Severity::malicious()),
                _ => Ok(Label::Internal(MapId::internal(location), hash)),
            },
            Node::Leaf(ref key, _) => {
                if location.contains(&(*key.digest()).into()) {
                    Ok(Label::Leaf(MapId::leaf(key.digest()), hash))
                } else {
                    Err(Severity::malicious())
                }
            }
            Node::Empty => Err(Severity::malicious()),
        }?;

        // Fill `root` if necessary.

        if self.root.is_none() {
            self.root = Some(label);
        }

        // Check if `label` is already in `store`.
        let hold = match store.entry(label) {
            Occupied(..) => true,
            Vacant(..) => false,
        };

        if hold {
            // If `node` is `Internal`, its position in `store` must match `location`.
            if let Node::Internal(..) = node {
                if locate::locate(store, label) == location {
                    Ok(())
                } else {
                    Err(Severity::malicious())
                }
            } else {
                Ok(())
            }?;

            store.incref(label);
            self.held.insert(label);
        } else {
            if let Node::Internal(ref left, ref right) = node {
                self.sight(left, location.left());
                self.sight(right, location.right());
            }

            self.acquired.insert(label, node);
        }

        self.frontier.remove(&hash);
        Ok(())
    }

    fn sight(&mut self, label: &Label, location: Prefix) {
        if !label.is_empty() {
            self.frontier.insert(
                *label.hash(),
                Context {
                    location,
                    remote_label: *label,
                },
            );
        }
    }

    fn ask(&self) -> Question {
        Question(
            self.frontier
                .iter()
                .map(|(_, context)| context.remote_label)
                .take(self.settings.window)
                .collect(),
        )
    }

    fn flush(&mut self, store: &mut Store<Key, Value>, label: Label) {
        if !label.is_empty() {
            let stored = match store.entry(label) {
                Occupied(..) => true,
                Vacant(..) => false,
            };

            let recursion = if stored {
                None
            } else {
                let node = self.acquired.get(&label).unwrap();
                store.populate(label, node.clone());

                match node {
                    Node::Internal(left, right) => Some((*left, *right)),
                    _ => None,
                }
            };

            if self.held.contains(&label) {
                self.held.remove(&label);
            } else {
                store.incref(label);
            }

            if let Some((left, right)) = recursion {
                self.flush(store, left);
                self.flush(store, right);
            }
        }
    }
}

impl<Key, Value> Drop for Receiver<Key, Value>
where
    Key: Field,
    Value: Field,
{
    fn drop(&mut self) {
        let mut store = self.cell.take();

        for label in self.held.iter() {
            drop::drop(&mut store, *label);
        }

        self.cell.restore(store);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::database::{sync::ANSWER_DEPTH, Database, Sender};

    use std::array::IntoIter;

    impl<Key, Value> Receiver<Key, Value>
    where
        Key: Field,
        Value: Field,
    {
        pub(crate) fn held(&self) -> Vec<Label> {
            self.held.iter().map(|label| *label).collect()
        }
    }

    fn run<'a, Key, Value, I, const N: usize>(
        database: &Database<Key, Value>,
        tables: I,
        transfers: [(&mut Sender<Key, Value>, Receiver<Key, Value>); N],
    ) -> ([Table<Key, Value>; N], usize)
    where
        Key: Field,
        Value: Field,
        I: IntoIterator<Item = &'a Table<Key, Value>>,
    {
        enum Transfer<'a, Key, Value>
        where
            Key: Field,
            Value: Field,
        {
            Complete(Table<Key, Value>),
            Incomplete(
                &'a mut Sender<Key, Value>,
                Receiver<Key, Value>,
                Answer<Key, Value>,
            ),
        }

        let mut transfers: [Transfer<Key, Value>; N] = array_init::from_iter(
            IntoIter::new(transfers).map(|(sender, receiver)| {
                let hello = sender.hello();
                Transfer::Incomplete(sender, receiver, hello)
            }),
        )
        .unwrap();

        let tables: Vec<&Table<Key, Value>> = tables.into_iter().collect();

        let mut steps: usize = 0;

        loop {
            steps += 1;

            transfers = array_init::from_iter(IntoIter::new(transfers).map(
                |transfer| match transfer {
                    Transfer::Incomplete(sender, receiver, answer) => {
                        let status = receiver.learn(answer).unwrap();

                        match status {
                            Status::Complete(table) => {
                                Transfer::Complete(table)
                            }
                            Status::Incomplete(receiver, question) => {
                                let answer = sender.answer(&question).unwrap();
                                Transfer::Incomplete(sender, receiver, answer)
                            }
                        }
                    }
                    complete => complete,
                },
            ))
            .unwrap();

            let receivers =
                transfers.iter().filter_map(|transfer| match transfer {
                    Transfer::Complete(..) => None,
                    Transfer::Incomplete(_, receiver, _) => Some(receiver),
                });

            let received =
                transfers.iter().filter_map(|transfer| match transfer {
                    Transfer::Complete(table) => Some(table),
                    Transfer::Incomplete(..) => None,
                });

            let tables = tables.clone().into_iter().chain(received);

            database.check(tables, receivers);

            if transfers.iter().all(|transfer| {
                if let Transfer::Complete(..) = transfer {
                    true
                } else {
                    false
                }
            }) {
                break;
            }
        }

        let received: [Table<Key, Value>; N] =
            array_init::from_iter(IntoIter::new(transfers).map(|transfer| {
                match transfer {
                    Transfer::Complete(table) => table,
                    Transfer::Incomplete(..) => unreachable!(),
                }
            }))
            .unwrap();

        (received, steps)
    }

    #[tokio::test]
    async fn empty() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.empty_table();
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([received], steps) = run(&bob, [], [(&mut sender, receiver)]);

        assert_eq!(steps, 1);
        received.assert_records([]);
    }

    #[tokio::test]
    async fn single() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records([(0, 1)]).await;
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([received], steps) = run(&bob, [], [(&mut sender, receiver)]);

        assert_eq!(steps, 1);
        received.assert_records([(0, 1)]);
    }

    #[tokio::test]
    async fn tree() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..8).map(|i| (i, i))).await;
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([received], steps) = run(&bob, [], [(&mut sender, receiver)]);

        assert_eq!(steps, 3);
        received.assert_records((0..8).map(|i| (i, i)));
    }

    #[tokio::test]
    async fn multiple() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i))).await;
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([received], _) = run(&bob, [], [(&mut sender, receiver)]);

        received.assert_records((0..256).map(|i| (i, i)));
    }

    #[tokio::test]
    async fn single_then_single() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records([(0, 1)]).await;
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([first], steps) = run(&bob, [], [(&mut sender, receiver)]);

        assert_eq!(steps, 1);
        first.assert_records([(0, 1)]);

        let original = alice.table_with_records([(2, 3)]).await;
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([second], steps) = run(&bob, [&first], [(&mut sender, receiver)]);

        assert_eq!(steps, 1);
        second.assert_records([(2, 3)]);
    }

    #[tokio::test]
    async fn single_then_same() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records([(0, 1)]).await;
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([first], steps) = run(&bob, [], [(&mut sender, receiver)]);

        assert_eq!(steps, 1);
        first.assert_records([(0, 1)]);

        let receiver = bob.receive();
        let ([second], steps) = run(&bob, [&first], [(&mut sender, receiver)]);

        assert_eq!(steps, 1);
        second.assert_records([(0, 1)]);
    }

    #[tokio::test]
    async fn tree_then_same() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..8).map(|i| (i, i))).await;
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([first], steps) = run(&bob, [], [(&mut sender, receiver)]);

        assert_eq!(steps, 3);
        first.assert_records((0..8).map(|i| (i, i)));

        let receiver = bob.receive();
        let ([second], steps) = run(&bob, [&first], [(&mut sender, receiver)]);

        assert_eq!(steps, 1);
        second.assert_records((0..8).map(|i| (i, i)));
    }

    #[tokio::test]
    async fn multiple_then_multiple() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i))).await;
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([first], _) = run(&bob, [], [(&mut sender, receiver)]);

        first.assert_records((0..256).map(|i| (i, i)));

        let original =
            alice.table_with_records((256..512).map(|i| (i, i))).await;
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([second], _) = run(&bob, [&first], [(&mut sender, receiver)]);

        second.assert_records((256..512).map(|i| (i, i)));
    }

    #[tokio::test]
    async fn multiple_then_same() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i))).await;
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([first], _) = run(&bob, [], [(&mut sender, receiver)]);

        first.assert_records((0..256).map(|i| (i, i)));

        let receiver = bob.receive();
        let ([second], steps) = run(&bob, [&first], [(&mut sender, receiver)]);

        assert_eq!(steps, 1);
        second.assert_records((0..256).map(|i| (i, i)));
    }

    #[tokio::test]
    async fn multiple_then_subset() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i))).await;
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([first], first_steps) = run(&bob, [], [(&mut sender, receiver)]);

        first.assert_records((0..256).map(|i| (i, i)));

        let original = alice.table_with_records((0..128).map(|i| (i, i))).await;
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([second], second_steps) =
            run(&bob, [&first], [(&mut sender, receiver)]);

        second.assert_records((0..128).map(|i| (i, i)));
        assert!(second_steps < first_steps);
    }

    #[tokio::test]
    async fn multiple_then_superset() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i))).await;
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([first], _) = run(&bob, [], [(&mut sender, receiver)]);

        first.assert_records((0..256).map(|i| (i, i)));

        let original = alice.table_with_records((0..512).map(|i| (i, i))).await;
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([second], _) = run(&bob, [&first], [(&mut sender, receiver)]);

        second.assert_records((0..512).map(|i| (i, i)));
    }

    #[tokio::test]
    async fn multiple_then_overlap() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i))).await;
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([first], _) = run(&bob, [], [(&mut sender, receiver)]);

        first.assert_records((0..256).map(|i| (i, i)));

        let original =
            alice.table_with_records((128..384).map(|i| (i, i))).await;
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([second], _) = run(&bob, [&first], [(&mut sender, receiver)]);

        second.assert_records((128..384).map(|i| (i, i)));
    }

    #[tokio::test]
    async fn multiple_then_multiple_then_overlap() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i))).await;
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([first], _) = run(&bob, [], [(&mut sender, receiver)]);

        first.assert_records((0..256).map(|i| (i, i)));

        let original =
            alice.table_with_records((256..512).map(|i| (i, i))).await;
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([second], _) = run(&bob, [&first], [(&mut sender, receiver)]);

        second.assert_records((256..512).map(|i| (i, i)));

        let original =
            alice.table_with_records((128..384).map(|i| (i, i))).await;
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([third], _) =
            run(&bob, [&first, &second], [(&mut sender, receiver)]);

        third.assert_records((128..384).map(|i| (i, i)));
    }

    #[tokio::test]
    async fn multiple_interleave_multiple() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let first_original =
            alice.table_with_records((0..256).map(|i| (i, i))).await;
        let mut first_sender = first_original.send();

        let second_original =
            alice.table_with_records((256..512).map(|i| (i, i))).await;
        let mut second_sender = second_original.send();

        let first_receiver = bob.receive();
        let second_receiver = bob.receive();

        let ([first, second], _) = run(
            &bob,
            [],
            [
                (&mut first_sender, first_receiver),
                (&mut second_sender, second_receiver),
            ],
        );

        first.assert_records((0..256).map(|i| (i, i)));
        second.assert_records((256..512).map(|i| (i, i)));
    }

    #[tokio::test]
    async fn multiple_interleave_same() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let first_original =
            alice.table_with_records((0..256).map(|i| (i, i))).await;
        let mut first_sender = first_original.send();

        let second_original =
            alice.table_with_records((0..256).map(|i| (i, i))).await;
        let mut second_sender = second_original.send();

        let first_receiver = bob.receive();
        let second_receiver = bob.receive();

        let ([first, second], _) = run(
            &bob,
            [],
            [
                (&mut first_sender, first_receiver),
                (&mut second_sender, second_receiver),
            ],
        );

        first.assert_records((0..256).map(|i| (i, i)));
        second.assert_records((0..256).map(|i| (i, i)));
    }

    #[tokio::test]
    async fn multiple_interleave_overlap() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let first_original =
            alice.table_with_records((0..256).map(|i| (i, i))).await;
        let mut first_sender = first_original.send();

        let second_original =
            alice.table_with_records((128..384).map(|i| (i, i))).await;
        let mut second_sender = second_original.send();

        let first_receiver = bob.receive();
        let second_receiver = bob.receive();

        let ([first, second], _) = run(
            &bob,
            [],
            [
                (&mut first_sender, first_receiver),
                (&mut second_sender, second_receiver),
            ],
        );

        first.assert_records((0..256).map(|i| (i, i)));
        second.assert_records((128..384).map(|i| (i, i)));
    }

    #[tokio::test]
    async fn multiple_then_double_overlap() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i))).await;
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([received], _) = run(&bob, [], [(&mut sender, receiver)]);

        received.assert_records((0..256).map(|i| (i, i)));

        let first_original =
            alice.table_with_records((128..384).map(|i| (i, i))).await;
        let mut first_sender = first_original.send();

        let second_original =
            alice.table_with_records((128..384).map(|i| (i, i))).await;
        let mut second_sender = second_original.send();

        let first_receiver = bob.receive();
        let second_receiver = bob.receive();

        let ([first, second], _) = run(
            &bob,
            [&received],
            [
                (&mut first_sender, first_receiver),
                (&mut second_sender, second_receiver),
            ],
        );

        first.assert_records((128..384).map(|i| (i, i)));
        second.assert_records((128..384).map(|i| (i, i)));
    }

    #[tokio::test]
    async fn multiple_then_overlap_drop_received_midway() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i))).await;
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([received], _) = run(&bob, [], [(&mut sender, receiver)]);

        received.assert_records((0..256).map(|i| (i, i)));

        let first_original =
            alice.table_with_records((128..384).map(|i| (i, i))).await;
        let mut first_sender = first_original.send();

        let mut first_receiver = bob.receive();
        let mut answer = first_sender.hello();

        for _ in 0..2 {
            let status = first_receiver.learn(answer).unwrap();

            match status {
                Status::Complete(_) => {
                    panic!("Should take longer than 2 steps");
                }
                Status::Incomplete(receiver, question) => {
                    answer = sender.answer(&question).unwrap();
                    first_receiver = receiver;
                }
            };
        }

        drop(received);

        let first = loop {
            let status = first_receiver.learn(answer).unwrap();

            match status {
                Status::Complete(table) => {
                    break table;
                }
                Status::Incomplete(receiver, question) => {
                    answer = sender.answer(&question).unwrap();
                    first_receiver = receiver;
                }
            };
        };

        bob.check([&first], []);
        first.assert_records((128..384).map(|i| (i, i)));
    }

    #[tokio::test]
    async fn multiple_acceptable_benign() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i))).await;
        let mut sender = original.send();

        let mut receiver = bob.receive();

        let mut answer = sender.hello();

        let max_benign = (1 << (ANSWER_DEPTH + 1)) - 2;

        answer = Answer(
            (0..max_benign + 1)
                .map(|_| answer.0[0].clone())
                .collect::<Vec<Node<_, _>>>(),
        );

        let first = loop {
            let status = receiver.learn(answer).unwrap();

            match status {
                Status::Complete(table) => {
                    break table;
                }
                Status::Incomplete(receiver_t, question) => {
                    answer = sender.answer(&question).unwrap();
                    receiver = receiver_t;
                }
            };
        };

        bob.check([&first], []);
        first.assert_records((0..256).map(|i| (i, i)));
    }

    #[tokio::test]
    async fn multiple_unacceptable_benign() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i))).await;
        let mut sender = original.send();

        let receiver = bob.receive();

        let mut answer = sender.hello();

        let max_benign = (1 << (ANSWER_DEPTH + 1)) - 2;

        answer = Answer(
            (0..max_benign + 2)
                .map(|_| answer.0[0].clone())
                .collect::<Vec<Node<_, _>>>(),
        );

        match receiver.learn(answer) {
            Err(SyncError::MalformedAnswer) => (),
            Err(x) => {
                panic!("Expected `SyncError::MalformedAnswer` but got {:?}", x)
            }
            _ => panic!("Receiver accepts too many benign faults from sender"),
        }
    }
}
