use crate::database::{
    data::Bytes,
    errors::{MalformedAnswer, SyncError},
    interact::drop,
    store::{Cell, Field, Label, MapId, Node, Store},
    sync::{locate, Severity},
    tree::Prefix,
    Answer, Question, Table,
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
        let mut severity = Severity::Benign(0);

        for node in answer.0 {
            severity = match self.update(&mut store, node) {
                Ok(()) => Severity::Benign(0),
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
        let hash = node.hash().into();

        let location = if self.root.is_some() {
            // Check if `hash` is in `frontier`. If so, retrieve `location`.
            Ok(self
                .frontier
                .get(&hash)
                .ok_or(Severity::Benign(1))?
                .location)
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
                | (Label::Leaf(..), Label::Empty) => Err(Severity::Malicious),
                _ => Ok(Label::Internal(MapId::internal(location), hash)),
            },
            Node::Leaf(ref key, _) => {
                if location.contains(&(*key.digest()).into()) {
                    Ok(Label::Leaf(MapId::leaf(key.digest()), hash))
                } else {
                    Err(Severity::Malicious)
                }
            }
            Node::Empty => Err(Severity::Malicious),
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
                    Err(Severity::Malicious)
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

    use crate::database::{Database, Sender};

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
}
