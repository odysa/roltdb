use std::{rc::Weak, sync::Arc};

#[derive(Debug)]
pub struct DB(pub Arc<IDB>);
#[derive(Debug)]
pub struct WeakDB(pub Weak<IDB>);

#[derive(Debug)]
pub struct IDB {}
