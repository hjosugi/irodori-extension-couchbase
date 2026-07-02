use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use reqwest::{Client, RequestBuilder};
use serde_json::{json, Map, Value};
use tokio::runtime::Runtime;

use crate::abi::{self, IrodoriConnectorBuffer};
use crate::{ABI_VERSION, CONFIG_JSON, DRIVER_LINKED, ENGINE, MANIFEST_JSON};

static CONNECTIONS: OnceLock<Mutex<HashMap<String, CouchbaseConnection>>> = OnceLock::new();
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

#[derive(Clone)]
struct CouchbaseConnection {
    client: Client,
    config: CouchbaseConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CouchbaseConfig {
    base_url: String,
    username: Option<String>,
    password: Option<String>,
    bearer_token: Option<String>,
    redaction_values: Vec<String>,
}

type QueryRows = Vec<Vec<Value>>;
type QueryOutput = (Vec<String>, QueryRows, bool);

fn connections() -> &'static Mutex<HashMap<String, CouchbaseConnection>> {
    CONNECTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn runtime() -> Result<&'static Runtime, String> {
    if let Some(runtime) = RUNTIME.get() {
        return Ok(runtime);
    }
    let runtime = Runtime::new().map_err(|err| format!("create tokio runtime failed: {err}"))?;
    let _ = RUNTIME.set(runtime);
    RUNTIME
        .get()
        .ok_or_else(|| "create tokio runtime failed.".to_string())
}

pub fn call_json(request: IrodoriConnectorBuffer) -> IrodoriConnectorBuffer {
    let request = match abi::parse_request(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let method = match abi::request_method(request.as_ref()) {
        Ok(method) => method,
        Err(response) => return response,
    };

    match method {
        "health" | "ping" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        ])),
        "describe" | "capabilities" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
            (
                "manifest".to_string(),
                serde_json::from_str(MANIFEST_JSON).unwrap_or(Value::Null),
            ),
            (
                "config".to_string(),
                serde_json::from_str(CONFIG_JSON).unwrap_or(Value::Null),
            ),
        ])),
        "manifest" => abi::owned_buffer(MANIFEST_JSON.to_string()),
        "config" => abi::owned_buffer(CONFIG_JSON.to_string()),
        "connect" => connect(request.as_ref().expect("connect has request")),
        "query" => query(request.as_ref().expect("query has request")),
        "metadata" => metadata(request.as_ref().expect("metadata has request")),
        "close" => close(request.as_ref().expect("close has request")),
        other => abi::error(
            "connector.unknownMethod",
            format!("unknown connector method: {other}"),
        ),
    }
}

fn connect(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let config = match CouchbaseConfig::from_request(request) {
        Ok(config) => config,
        Err(err) => return abi::error("connector.invalidRequest", err),
    };
    let connection = CouchbaseConnection {
        client: Client::new(),
        config,
    };
    let version = match runtime().and_then(|runtime| runtime.block_on(load_version(&connection))) {
        Ok(version) => version,
        Err(err) => return abi::error("connector.connectFailed", connection.config.redact(&err)),
    };
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let response = Map::from_iter([
        ("engine".to_string(), Value::String(ENGINE.to_string())),
        (
            "connectionId".to_string(),
            Value::String(connection_id.clone()),
        ),
        ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        (
            "endpoint".to_string(),
            Value::String(connection.config.base_url.clone()),
        ),
        ("serverVersion".to_string(), Value::String(version)),
    ]);
    guard.insert(connection_id, connection);
    abi::ok(response)
}

fn query(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let Some(sql) = abi::string_field(request, "sql")
        .or_else(|| abi::string_field(request, "query"))
        .or_else(|| abi::string_field(request, "statement"))
    else {
        return abi::error(
            "connector.invalidRequest",
            "query requires a string sql, query, or statement field.",
        );
    };
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime()
        .and_then(|runtime| runtime.block_on(run_query(&connection, sql, abi::max_rows(request))))
    {
        Ok((columns, rows, truncated)) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            (
                "columns".to_string(),
                Value::Array(columns.into_iter().map(Value::String).collect()),
            ),
            (
                "rows".to_string(),
                Value::Array(rows.into_iter().map(Value::Array).collect()),
            ),
            ("truncated".to_string(), Value::Bool(truncated)),
        ])),
        Err(err) => abi::error("connector.queryFailed", connection.config.redact(&err)),
    }
}

fn metadata(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime().and_then(|runtime| runtime.block_on(load_metadata(&connection))) {
        Ok(metadata) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            ("metadata".to_string(), metadata),
        ])),
        Err(err) => abi::error("connector.metadataFailed", connection.config.redact(&err)),
    }
}

fn close(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let existed = guard.remove(&connection_id).is_some();
    abi::ok(Map::from_iter([
        ("connectionId".to_string(), Value::String(connection_id)),
        ("closed".to_string(), Value::Bool(existed)),
    ]))
}

impl CouchbaseConnection {
    fn auth(&self, builder: RequestBuilder) -> RequestBuilder {
        if let Some(token) = self.config.bearer_token.as_deref() {
            builder.bearer_auth(token)
        } else if let Some(username) = self.config.username.as_deref() {
            builder.basic_auth(username, self.config.password.as_deref())
        } else {
            builder
        }
    }
}

impl CouchbaseConfig {
    fn from_request(request: &Value) -> Result<Self, String> {
        let base_url = option_string(request, &["connectionString", "url", "dsn"])
            .unwrap_or_else(|| build_url(request));
        let username = option_string(request, &["user", "username"]);
        let password = option_string(request, &["password"]);
        let bearer_token = option_string(request, &["token", "bearerToken", "accessToken"]);
        let mut redaction_values = Vec::new();
        push_sensitive(&mut redaction_values, password.as_deref());
        push_sensitive(&mut redaction_values, bearer_token.as_deref());
        collect_url_auth(&base_url, &mut redaction_values);
        Ok(Self {
            base_url: normalize_url(&base_url),
            username,
            password,
            bearer_token,
            redaction_values,
        })
    }

    fn redact(&self, message: &str) -> String {
        self.redaction_values.iter().fold(
            message.replace(&self.base_url, "<couchbase-url>"),
            |message, secret| {
                if secret.is_empty() {
                    message
                } else {
                    message.replace(secret, "****")
                }
            },
        )
    }
}

async fn load_version(connection: &CouchbaseConnection) -> Result<String, String> {
    let (_, rows, _) = run_query(connection, "SELECT VERSION() AS version", 1).await?;
    Ok(rows
        .first()
        .and_then(|row| row.first())
        .and_then(Value::as_str)
        .map(|version| format!("Couchbase {version}"))
        .unwrap_or_else(|| "Couchbase".to_string()))
}

async fn run_query(
    connection: &CouchbaseConnection,
    sql: &str,
    cap: usize,
) -> Result<QueryOutput, String> {
    let value = query_service(connection, sql).await?;
    let results = value
        .get("results")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let truncated = results.len() > cap;
    Ok(rows_to_output(
        results.into_iter().take(cap).collect(),
        truncated,
    ))
}

async fn load_metadata(connection: &CouchbaseConnection) -> Result<Value, String> {
    let sql = "SELECT RAW { \
        \"bucket\": bucket_id, \
        \"scope\": scope_id, \
        \"name\": name, \
        \"path\": path, \
        \"type\": `type` \
      } FROM system:keyspaces ORDER BY bucket_id, scope_id, name";
    let value = query_service(connection, sql).await?;
    let rows = value
        .get("results")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut objects = Vec::new();
    for row in rows {
        let bucket = row
            .get("bucket")
            .and_then(Value::as_str)
            .unwrap_or("default");
        let scope = row
            .get("scope")
            .and_then(Value::as_str)
            .unwrap_or("_default");
        let name = row.get("name").and_then(Value::as_str).unwrap_or("");
        if name.is_empty() {
            continue;
        }
        objects.push(json!({
            "schema": format!("{bucket}.{scope}"),
            "name": name,
            "kind": row.get("type").and_then(Value::as_str).unwrap_or("keyspace"),
            "columns": [{
                "name": "document",
                "dataType": "json",
                "nullable": true,
                "ordinal": 1
            }],
            "indexes": [],
            "primaryKey": [],
            "foreignKeys": []
        }));
    }
    Ok(json!({ "schemas": [{ "name": "default", "objects": objects }] }))
}

async fn query_service(connection: &CouchbaseConnection, sql: &str) -> Result<Value, String> {
    let response = connection
        .auth(
            connection
                .client
                .post(format!("{}/query/service", connection.config.base_url)),
        )
        .form(&[("statement", sql)])
        .send()
        .await
        .map_err(|err| format!("Couchbase query request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("Couchbase response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!("Couchbase returned HTTP {status}: {text}"));
    }
    let value = serde_json::from_str::<Value>(&text)
        .map_err(|err| format!("Couchbase JSON response parse failed: {err}: {text}"))?;
    if let Some(errors) = value.get("errors").and_then(Value::as_array) {
        if let Some(error) = errors.first() {
            return Err(error
                .get("msg")
                .or_else(|| error.get("message"))
                .and_then(Value::as_str)
                .unwrap_or("Couchbase query failed.")
                .to_string());
        }
    }
    Ok(value)
}

fn rows_to_output(rows_json: Vec<Value>, truncated: bool) -> QueryOutput {
    let mut columns = Vec::new();
    for row in &rows_json {
        if let Some(object) = row.as_object() {
            for key in object.keys() {
                if !columns.iter().any(|column| column == key) {
                    columns.push(key.clone());
                }
            }
        }
    }
    let rows = rows_json
        .iter()
        .map(|row| {
            if let Some(object) = row.as_object() {
                columns
                    .iter()
                    .map(|column| object.get(column).cloned().unwrap_or(Value::Null))
                    .collect()
            } else {
                vec![row.clone()]
            }
        })
        .collect::<Vec<_>>();
    if columns.is_empty() && !rows_json.is_empty() {
        (vec!["value".to_string()], rows, truncated)
    } else {
        (columns, rows, truncated)
    }
}

fn connection(connection_id: &str) -> Result<CouchbaseConnection, IrodoriConnectorBuffer> {
    let guard = connections().lock().map_err(|_| {
        abi::error(
            "connector.statePoisoned",
            "Connector connection state is poisoned.",
        )
    })?;
    guard.get(connection_id).cloned().ok_or_else(|| {
        abi::error(
            "connector.connectionNotFound",
            format!("no open connection: {connection_id}"),
        )
    })
}

fn build_url(request: &Value) -> String {
    let host = option_string(request, &["host", "endpoint"]).unwrap_or_else(|| "127.0.0.1".into());
    let port = option_string(request, &["queryPort", "port"]).unwrap_or_else(|| "8093".into());
    let scheme = if bool_option(request, &["tls", "ssl"]).unwrap_or(false) {
        "https"
    } else {
        "http"
    };
    format!("{scheme}://{host}:{port}")
}

fn normalize_url(value: &str) -> String {
    let value = value.trim().trim_end_matches('/');
    if let Some(rest) = value.strip_prefix("couchbase://") {
        format!("http://{}:8093", rest.trim_end_matches('/'))
    } else if let Some(rest) = value.strip_prefix("couchbases://") {
        format!("https://{}:18093", rest.trim_end_matches('/'))
    } else {
        value.to_string()
    }
}

fn request_containers(request: &Value) -> Vec<&Value> {
    [
        Some(request),
        request.get("profile"),
        request.get("options"),
        request.get("auth"),
        request.get("secrets"),
        request
            .get("profile")
            .and_then(|profile| profile.get("options")),
        request
            .get("profile")
            .and_then(|profile| profile.get("auth")),
        request
            .get("profile")
            .and_then(|profile| profile.get("secrets")),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn option_string(request: &Value, fields: &[&str]) -> Option<String> {
    request_containers(request)
        .into_iter()
        .find_map(|container| {
            fields.iter().find_map(|field| {
                container
                    .get(*field)
                    .map(|value| match value {
                        Value::String(value) => value.clone(),
                        Value::Number(value) => value.to_string(),
                        Value::Bool(value) => value.to_string(),
                        _ => String::new(),
                    })
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            })
        })
}

fn bool_option(request: &Value, fields: &[&str]) -> Option<bool> {
    request_containers(request)
        .into_iter()
        .find_map(|container| {
            fields
                .iter()
                .find_map(|field| container.get(*field).and_then(Value::as_bool))
        })
}

fn push_sensitive(values: &mut Vec<String>, value: Option<&str>) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        if !values.iter().any(|existing| existing == value) {
            values.push(value.to_string());
        }
    }
}

fn collect_url_auth(url: &str, values: &mut Vec<String>) {
    let Some(after_scheme) = url.split_once("://").map(|(_, rest)| rest) else {
        return;
    };
    let Some(auth) = after_scheme
        .split('/')
        .next()
        .and_then(|host| host.split('@').next())
    else {
        return;
    };
    if auth.contains(':') {
        for part in auth.split(':') {
            push_sensitive(values, Some(part));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_query_service_port() {
        let request = json!({"profile": {"host": "localhost"}});
        assert_eq!(build_url(&request), "http://localhost:8093");
    }

    #[test]
    fn maps_json_rows_to_columns() {
        let (columns, rows, truncated) = rows_to_output(vec![json!({"a": 1, "b": "x"})], false);
        assert_eq!(columns, vec!["a", "b"]);
        assert_eq!(rows[0], vec![json!(1), json!("x")]);
        assert!(!truncated);
    }
}
