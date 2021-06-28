use drop::crypto::hash::HashError as DropHashError;

use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum QueryError {
    #[snafu(display("failed to hash field: {}", source))]
    HashError { source: DropHashError },
    #[snafu(display("key collision within transaction"))]
    KeyCollision {},
}