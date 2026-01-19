use sqlx::{Pool, Sqlite};
use std::sync::RwLock;

static PERSISTENCE_DB: RwLock<Option<Pool<Sqlite>>> = RwLock::new(None);

pub fn use_database(db: Pool<Sqlite>) {
    let mut lock = PERSISTENCE_DB
        .write()
        .expect("Failed to acquire write lock on DB");
    *lock = Some(db);
}

pub fn using_db() -> bool {
    let lock = PERSISTENCE_DB
        .read()
        .expect("Failed to acquire read lock on DB");
    lock.is_some()
}

pub fn get_db() -> Option<Pool<Sqlite>> {
    let lock = PERSISTENCE_DB
        .read()
        .expect("Failed to acquire read lock on DB");

    lock.clone()
}
