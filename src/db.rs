use std::{rc::Weak, sync::Arc};

pub struct DB(pub Arc<IDB>);
pub struct WeakDB(pub Weak<IDB>);

pub struct IDB {}
