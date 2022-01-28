mod bucket;
mod cursor;
mod data;
mod db;
mod error;
mod free_list;
mod meta;
mod node;
mod page;
mod transaction;
mod utils;
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
