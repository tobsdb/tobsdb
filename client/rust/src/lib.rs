use futures_util::{SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, map::Map};
use std::fs;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::protocol::{frame::coding::CloseCode, CloseFrame};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use url::Url;

pub struct TdbAuthParms<'a> {
    username: &'a str,
    password: &'a str,
}

impl<'b> TdbAuthParms<'b> {
    pub fn new(username: &'b str, password: &'b str) -> TdbAuthParms<'b> {
        TdbAuthParms { username, password }
    }
}

// TODO: use custom schema types
#[derive(Deserialize)]
pub struct TdbResponse {
    pub status: u32,
    pub message: String,
    pub data: Option<Map<String, serde_json::Value>>,
    __tdb_client_req_id__: u64,
}

#[derive(Deserialize)]
pub struct TdbResponseMany {
    pub status: u32,
    pub message: String,
    pub data: Option<Vec<Map<String, serde_json::Value>>>,
    __tdb_client_req_id__: u64,
}

pub struct Tobsdb {
    url: Url,
    conn: Option<WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>>,
}

impl Tobsdb {
    pub fn new(
        url: &str,
        db_name: &str,
        schema_path: &str,
        auth_parms: Option<TdbAuthParms>,
    ) -> Tobsdb {
        // TODO: handle error
        let schema_data = fs::read_to_string(schema_path).unwrap();
        let mut url_params = vec![("db", db_name), ("schema", schema_data.as_str())];
        if let Some(auth_parms) = auth_parms {
            url_params.push(("username", auth_parms.username));
            url_params.push(("password", auth_parms.password));
        }
        let url = Url::parse_with_params(url, url_params).unwrap();
        Tobsdb { url, conn: None }
    }

    // TODO: allow user pass in schema as string
    pub async fn connect(&mut self) -> Result<(), TdbError> {
        if self.conn.is_some() {
            println!("Already connected to {:?}", self.url.origin());
            return Ok(());
        }
        println!("Connecting to {:?}", self.url.origin());
        self.conn = match tokio_tungstenite::connect_async(self.url.clone()).await {
            Ok((conn, res)) => {
                if let Some(tdb_error) = res.headers().get("tdb-error") {
                    return Err(TdbError::ConnFailed(format!("{:?}", tdb_error)));
                }
                Some(conn)
            }
            Err(e) => return Err(TdbError::ConnFailed(format!("{}", e))),
        };
        Ok(())
    }

    pub async fn disconnect(&mut self) {
        if let Some(mut conn) = self.conn.take() {
            conn.close(Some(CloseFrame {
                code: CloseCode::Normal,
                reason: "Disconnect".into(),
            }))
            .await
            .unwrap();
        }
    }

    async fn require_conn(&mut self) -> Result<(), TdbError> {
        self.connect().await?;
        if self.conn.is_none() {
            return Err(TdbError::Disconnected);
        }
        Ok(())
    }

    async fn query<D, T>(
        &mut self,
        action: &str,
        table: &str,
        data: Option<D>,
        q_where: Option<&Map<String, serde_json::Value>>,
    ) -> Result<T, TdbError>
    where
        D: Serialize,
        T: DeserializeOwned,
    {
        self.require_conn().await?;

        let query_data = json!({
            "action": action,
            "table": table,
            "data": data,
            "where": q_where
        });

        let conn = self.conn.as_mut().unwrap();

        if let Err(e) = conn.send(Message::from(query_data.to_string())).await {
            return Err(TdbError::QueryFailed(format!("{}", e)));
        };

        if let Some(Ok(res)) = conn.next().await {
            let res_str = res.into_text().unwrap();
            let res: T = serde_json::from_str(&res_str).unwrap();
            return Ok(res);
        }

        Err(TdbError::NoResponse)
    }

    pub async fn create(
        &mut self,
        table: &str,
        data: &Map<String, serde_json::Value>,
    ) -> Result<TdbResponse, TdbError> {
        self.query::<&Map<String, serde_json::Value>, TdbResponse>(
            "create",
            table,
            Some(data),
            None,
        )
        .await
    }

    pub async fn create_many(
        &mut self,
        table: &str,
        data: Vec<&Map<String, serde_json::Value>>,
    ) -> Result<TdbResponseMany, TdbError> {
        self.query::<Vec<&Map<String, serde_json::Value>>, TdbResponseMany>(
            "createMany",
            table,
            Some(data),
            None,
        )
        .await
    }

    pub async fn find_unqiue(
        &mut self,
        table: &str,
        where_constraint: &Map<String, serde_json::Value>,
    ) -> Result<TdbResponse, TdbError> {
        self.query::<_, TdbResponse>("findUnique", table, None::<()>, Some(where_constraint))
            .await
    }

    pub async fn find_many(
        &mut self,
        table: &str,
        where_constraint: &Map<String, serde_json::Value>,
    ) -> Result<TdbResponseMany, TdbError> {
        self.query::<_, TdbResponseMany>("findMany", table, None::<()>, Some(where_constraint))
            .await
    }

    pub async fn update_unqiue(
        &mut self,
        table: &str,
        where_constraint: &Map<String, serde_json::Value>,
    ) -> Result<TdbResponse, TdbError> {
        self.query::<_, TdbResponse>("updateUnique", table, None::<()>, Some(where_constraint))
            .await
    }

    pub async fn update_many(
        &mut self,
        table: &str,
        where_constraint: &Map<String, serde_json::Value>,
    ) -> Result<TdbResponseMany, TdbError> {
        self.query::<_, TdbResponseMany>("updateMany", table, None::<()>, Some(where_constraint))
            .await
    }

    pub async fn delete_unqiue(
        &mut self,
        table: &str,
        where_constraint: &Map<String, serde_json::Value>,
    ) -> Result<TdbResponse, TdbError> {
        self.query::<_, TdbResponse>("deleteUnique", table, None::<()>, Some(where_constraint))
            .await
    }

    pub async fn delete_many(
        &mut self,
        table: &str,
        where_constraint: &Map<String, serde_json::Value>,
    ) -> Result<TdbResponseMany, TdbError> {
        self.query::<_, TdbResponseMany>("deleteMany", table, None::<()>, Some(where_constraint))
            .await
    }
}

#[derive(Debug, Clone)]
pub enum TdbError {
    ConnFailed(String),
    QueryFailed(String),
    Disconnected,
    NoResponse,
}

impl std::fmt::Display for TdbError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TdbError::ConnFailed(reason) => write!(f, "Connection Failed: {}", reason),
            TdbError::QueryFailed(reason) => write!(f, "Query Failed: {}", reason),
            TdbError::Disconnected => write!(f, "Websocket disconnected"),
            TdbError::NoResponse => write!(f, "No response received"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_tdb_connect() {
        let mut tdb = Tobsdb::new(
            "ws://localhost:7085",
            "rust_client_ts",
            "./test_schema.tdb",
            Some(TdbAuthParms::new("user", "pass")),
        );
        assert!(tdb.conn.is_none());

        tdb.connect().await.unwrap();
        assert!(tdb.conn.is_some());

        let mut input: Map<String, serde_json::Value> = Map::new();
        input.insert("b".to_string(), "hello world".into());
        tdb.create("a", &input).await.unwrap();

        tdb.disconnect().await;
    }
}
