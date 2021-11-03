use crate::{
    common::{store::Field, Commitment},
    database::{
        CollectionResponse, CollectionSender, CollectionTransaction, Table,
    },
};

use std::collections::HashSet;
use std::hash::Hash;

pub struct Collection<Item: Field>(pub(crate) Table<Item, ()>);

impl<Item> Collection<Item>
where
    Item: Field,
{
    pub fn commit(&self) -> Commitment {
        self.0.commit()
    }

    pub async fn execute(
        &mut self,
        transaction: CollectionTransaction<Item>,
    ) -> CollectionResponse<Item> {
        CollectionResponse(self.0.execute(transaction.0).await)
    }

    pub fn send(self) -> CollectionSender<Item> {
        CollectionSender(self.0.send())
    }

    pub async fn diff(
        lho: &mut Collection<Item>,
        rho: &mut Collection<Item>,
    ) -> (HashSet<Item>, HashSet<Item>)
    where
        Item: Clone + Eq + Hash,
    {
        let mut lho_minus_rho = HashSet::new();
        let mut rho_minus_lho = HashSet::new();

        for (key, (in_lho, _)) in Table::diff(&mut lho.0, &mut rho.0).await {
            if in_lho.is_some() {
                lho_minus_rho.insert(key);
            } else {
                rho_minus_lho.insert(key);
            }
        }

        (lho_minus_rho, rho_minus_lho)
    }
}

impl<Item> Clone for Collection<Item>
where
    Item: Field,
{
    fn clone(&self) -> Self {
        Collection(self.0.clone())
    }
}
