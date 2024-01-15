pub mod errors;
pub mod types;

use errors::TdbError;
use futures_util::{SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
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

pub struct Tobsdb {
    url: Url,
    conn: Option<WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>>,
}

impl Tobsdb {
    /// Create a new TobsDB client
    ///
    /// # Arguments
    ///
    /// * `url` - Websocket URL of the TobsDB server
    /// * `db_name` - Name of the database to use on the server
    /// * `schema_path` - Path to the TobsDB schema file
    /// * `auth_parms` - Optional authentication parameters
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
    /// Connect to a TobsDB server
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

    // TODO: handle responses asynchronously
    async fn query<D, W, T>(
        &mut self,
        action: &str,
        table: &str,
        data: Option<D>,
        q_where: Option<W>,
    ) -> Result<T, TdbError>
    where
        D: Serialize,
        W: Serialize,
        T: DeserializeOwned,
    {
        self.connect().await?;
        if self.conn.is_none() {
            return Err(TdbError::Disconnected);
        }

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
            let res: T = serde_json::from_str(res_str.as_str()).unwrap();
            return Ok(res);
        }

        Err(TdbError::NoResponse)
    }

    /// Create a new row in `table`
    ///
    /// # Arguments
    ///
    /// * `table` - Name of the table
    /// * `data` - Row data
    ///
    /// # Examples
    /// ```rust
    /// use tobsdb::Tobsdb;
    /// use tobsdb::types;
    ///
    /// struct User {
    ///     pub name: types::TdbString,
    ///     pub age: types::TdbInt,
    /// }
    ///
    /// let res = tdb.create("user", &User { name: types::TdbString::from("John"), age: 42 }).await;
    /// assert!(res.status == 201);
    /// assert!(res.data.is_some());
    ///
    /// let user = res.data.unwrap();
    /// assert!(user.name == "John");
    /// assert!(user.age == 42);
    /// ```
    pub async fn create<T>(
        &mut self,
        table: &str,
        data: &T,
    ) -> Result<types::TdbResponse<T>, TdbError>
    where
        T: DeserializeOwned + Serialize,
    {
        self.query::<&T, _, types::TdbResponse<T>>("create", table, Some(data), None::<()>)
            .await
    }

    /// Create new rows in `table`
    ///
    /// # Arguments
    ///
    /// * `table` - Name of the table
    /// * `data` - Vector of row data
    ///
    /// # Examples
    /// ```rust
    /// use tobsdb::Tobsdb;
    /// use tobsdb::types;
    ///
    /// struct User {
    ///     pub name: types::TdbString,
    ///     pub age: types::TdbInt,
    /// }
    ///
    /// let res = tdb.create(
    ///     "user",
    ///     vec![
    ///         &User { name: types::TdbString::from("John"), age: 42 },
    ///         &User { name: types::TdbString::from("Stacy"), age: 33 }
    ///     ],
    /// ).await;
    /// assert!(res.status == 201);
    /// assert!(res.data.is_some());
    ///
    /// let users = res.data.unwrap();
    /// assert!(users.len() == 2);
    /// ```
    pub async fn create_many<T>(
        &mut self,
        table: &str,
        data: Vec<&T>,
    ) -> Result<types::TdbResponseMany<T>, TdbError>
    where
        T: DeserializeOwned + Serialize,
    {
        self.query::<Vec<&T>, _, types::TdbResponseMany<T>>(
            "createMany",
            table,
            Some(data),
            None::<()>,
        )
        .await
    }

    /// Find row in `table`
    ///
    /// # Arguments
    ///
    /// * `table` - Name of the table
    /// * `where` - Row data constraints
    ///
    /// # Examples
    /// ```rust
    /// use tobsdb::Tobsdb;
    /// use tobsdb::types;
    ///
    /// struct User {
    ///     pub name: types::TdbString,
    ///     pub age: types::TdbInt,
    /// }
    ///
    /// let res = tdb.find_unqiue(
    ///     "user",
    ///     &User { name: types::TdbString::from("John") },
    /// ).await;
    /// assert!(res.status == 200);
    /// assert!(res.data.is_some());
    ///
    /// let user = res.data.unwrap();
    /// assert!(users.name == "John");
    /// assert!(users.age == 42);
    /// ```
    pub async fn find_unqiue<T>(
        &mut self,
        table: &str,
        where_constraint: &T,
    ) -> Result<types::TdbResponse<T>, TdbError>
    where
        T: DeserializeOwned + Serialize,
    {
        self.query::<_, &T, types::TdbResponse<T>>(
            "findUnique",
            table,
            None::<()>,
            Some(where_constraint),
        )
        .await
    }

    /// Find rows in `table`
    ///
    /// # Arguments
    ///
    /// * `table` - Name of the table
    /// * `where` - Row data constraints
    ///
    /// # Examples
    /// ```rust
    /// use tobsdb::Tobsdb;
    /// use tobsdb::types;
    ///
    /// struct User {
    ///     pub name: types::TdbString,
    ///     pub age: types::TdbInt,
    /// }
    ///
    /// let res = tdb.find_unqiue(
    ///     "user",
    ///     &User { name: types::TdbString::from("John") },
    /// ).await;
    /// assert!(res.status == 200);
    /// assert!(res.data.is_some());
    ///
    /// let users = res.data.unwrap();
    /// assert!(users.len() == 1);
    /// ```
    // TODO: probably leave `where_constraint` as map and have user pass in expected return type
    pub async fn find_many<T>(
        &mut self,
        table: &str,
        where_constraint: &T,
    ) -> Result<types::TdbResponseMany<T>, TdbError>
    where
        T: DeserializeOwned + Serialize,
    {
        self.query::<_, &T, types::TdbResponseMany<T>>(
            "findMany",
            table,
            None::<()>,
            Some(where_constraint),
        )
        .await
    }

    /// Update a row in `table`
    ///
    /// # Arguments
    ///
    /// * `table` - Name of the table
    /// * `where` - Row data constraints
    /// * `data` - Row data to update
    ///
    /// # Examples
    /// ```rust
    /// use tobsdb::Tobsdb;
    /// use tobsdb::types;
    ///
    /// struct User {
    ///     pub name: types::TdbString,
    ///     pub age: types::TdbInt,
    /// }
    ///
    /// let res = tdb.update_unqiue(
    ///     "user",
    ///     &User { name: types::TdbString::from("John") },
    ///     &User { name: types::TdbString::from("James") },
    /// ).await;
    /// assert!(res.status == 200);
    /// assert!(res.data.is_some());
    ///
    /// let user = res.data.unwrap();
    /// assert!(users.name == "James");
    /// ```
    pub async fn update_unqiue<D, W>(
        &mut self,
        table: &str,
        data: &D,
        where_constraint: &W,
    ) -> Result<types::TdbResponse<D>, TdbError>
    where
        D: DeserializeOwned + Serialize,
        W: DeserializeOwned + Serialize,
    {
        self.query::<&D, &W, types::TdbResponse<D>>(
            "updateUnique",
            table,
            Some(data),
            Some(where_constraint),
        )
        .await
    }

    pub async fn update_many<D, W>(
        &mut self,
        table: &str,
        data: &D,
        where_constraint: &W,
    ) -> Result<types::TdbResponseMany<D>, TdbError>
    where
        D: DeserializeOwned + Serialize,
        W: DeserializeOwned + Serialize,
    {
        self.query::<&D, &W, types::TdbResponseMany<D>>(
            "updateMany",
            table,
            Some(data),
            Some(where_constraint),
        )
        .await
    }

    pub async fn delete_unqiue<T>(
        &mut self,
        table: &str,
        where_constraint: &T,
    ) -> Result<types::TdbResponse<T>, TdbError>
    where
        T: DeserializeOwned + Serialize,
    {
        self.query::<_, &T, types::TdbResponse<T>>(
            "deleteUnique",
            table,
            None::<()>,
            Some(where_constraint),
        )
        .await
    }

    pub async fn delete_many<T>(
        &mut self,
        table: &str,
        where_constraint: &T,
    ) -> Result<types::TdbResponseMany<T>, TdbError>
    where
        T: DeserializeOwned + Serialize,
    {
        self.query::<_, &T, types::TdbResponseMany<T>>(
            "deleteMany",
            table,
            None::<()>,
            Some(where_constraint),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TdbString;
    use serde::Deserialize;

    #[derive(Deserialize, Serialize)]
    struct TableA {
        pub b: TdbString,
    }

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

        let res = tdb
            .create(
                "a",
                &TableA {
                    b: TdbString::from("hello world"),
                },
            )
            .await
            .unwrap();
        assert!(res.data.is_some());
        assert!(res.data.unwrap().b == "hello world");
        assert!(res.message == "Created new row in table a");
        assert!(res.status == 201);

        tdb.disconnect().await;
    }
}
