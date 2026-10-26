mod common;

use common::*;
use serde_json::{json, Value};

#[tokio::test]
async fn homepage_spacers_are_persistent_public_and_admin_gated() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();
    let session = login(&base).await;
    let path = format!("{base}/api/v1/homepage/spacers");
    let admin_path = format!("{base}/api/v1/admin/homepage/spacers");
    assert_eq!(
        client
            .put(&admin_path)
            .json(&json!({"spacers": []}))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let saved = client
        .put(&admin_path)
        .header(reqwest::header::COOKIE, &session)
        .json(&json!({"spacers": [
            {"id": "first", "after": "check:one"},
            {"id": "second", "after": "section:API"}
        ]}))
        .send()
        .await
        .unwrap();
    assert_eq!(saved.status(), 200);
    let public_response = client.get(&path).send().await.unwrap();
    let public_status = public_response.status();
    let public_body = public_response.text().await.unwrap();
    assert!(
        public_status.is_success(),
        "public spacer request returned {public_status}: {public_body}"
    );
    let public: Value = serde_json::from_str(&public_body).unwrap();
    assert_eq!(
        public["spacers"],
        json!([
            {"id": "first", "after": "check:one"},
            {"id": "second", "after": "section:API"}
        ])
    );
    let bad = client
        .put(&admin_path)
        .header(reqwest::header::COOKIE, &session)
        .json(&json!({"spacers": [{"id": "dup", "after": null}, {"id": "dup", "after": null}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);
    let unchanged: Value = client
        .get(&path)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(unchanged, public);
}
