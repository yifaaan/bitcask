mod batch;
mod data;
mod db;
mod errors;
mod fio;
mod index;
mod iterator;
mod merge;
mod options;
mod util;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
