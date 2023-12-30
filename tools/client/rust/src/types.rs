use serde::Deserialize;
use time::Date;

pub type TdbInt = i64;
pub type TdbString = String;
pub type TdbVector<T> = Vec<T>;
pub type TdbFloat = f64;
pub type TdbDate = Date;
pub type TdbBool = bool;
pub type TdbBytes = Vec<u8>;

#[derive(Deserialize, Debug)]
pub struct TdbResponse<D> {
    pub status: u32,
    pub message: String,
    pub data: Option<D>,
    __tdb_client_req_id__: u64,
}

#[derive(Deserialize, Debug)]
pub struct TdbResponseMany<D> {
    pub status: u32,
    pub message: String,
    pub data: Option<Vec<D>>,
    __tdb_client_req_id__: u64,
}
