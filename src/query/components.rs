use crate::types::{data::PlankData, fields::PlankField};
use std::collections::HashMap;

pub enum QueryKey {
    // Attr(String),
    RowGroup,
    Column(String),
}

pub enum Cmp {
    Eq(QueryKey, PlankData),
    Greater(QueryKey, PlankData),
    GreaterEq(QueryKey, PlankData),
    Less(QueryKey, PlankData),
    LessEq(QueryKey, PlankData),
    Not(Box<Cmp>),
    And(Vec<Cmp>),
    Or(Vec<Cmp>),
}

pub struct PlankQuery {
    pub select: Option<Vec<QueryKey>>,
    pub filter: Option<Cmp>,
    pub limit: Option<u32>,
}

impl Default for PlankQuery {
    fn default() -> Self {
        PlankQuery {
            select: None,
            filter: None,
            limit: None,
        }
    }
}

impl PlankQuery {
    pub fn new() -> PlankQuery {
        Self::default()
    }

    pub fn select(mut self, cols: Vec<QueryKey>) -> Self {
        self.select = Some(cols);
        self
    }

    pub fn filter(mut self, cmp: Cmp) -> Self {
        self.filter = Some(cmp);
        self
    }

    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }
}

/*
* PlankQuery{
*   select: [Column("age")]),
*   where: [
*       Eq(Column("age"), Int64(10)
*   ]
* }
*/
