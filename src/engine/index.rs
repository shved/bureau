#[derive(Debug)]
pub struct Index {
    entries: Vec<String>,
}

impl Index {
    // TODO: Pass path to db files here.
    pub fn new() -> Index {
        Index { entries: vec![] }
    }
}
