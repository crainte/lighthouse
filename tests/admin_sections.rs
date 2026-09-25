//! Admin API integration tests for status-page sections: ordering, renaming,
//! deleting, and that deleting a section never drops the checks inside it.
//!
//! Split from the original `admin_api.rs`; see `tests/common/mod.rs` for the
//! shared fixtures.

mod common;

use serde_json::{json, Value};

use common::*;

#[tokio::test]
async fn sections_are_ordered_renamed_and_deleted_without_losing_checks() {
    let addr = spawn_server(coordinator().await);
    let base = format!("http://{addr}");
    let admin = reqwest::Client::new();
    let session = login(&base).await;

    let post = |path: &str, body: Value| {
        let (admin, session, base) = (admin.clone(), session.clone(), base.clone());
        let path = path.to_string();
        async move {
            admin
                .post(format!("{base}{path}"))
                .header(reqwest::header::COOKIE, &session)
                .json(&body)
                .send()
                .await
                .unwrap()
        }
    };

    for name in ["Storage", "API", "CDN"] {
        let res = post("/api/v1/admin/sections", json!({ "name": name })).await;
        assert_eq!(res.status(), 200, "creating {name}");
    }
    // A section can exist before anything is in it, which is the whole point of
    // storing them separately from checks.
    let res = post("/api/v1/admin/sections", json!({ "name": "API" })).await;
    assert_eq!(res.status(), 409, "duplicate section names are refused");

    let names = |v: &Value| {
        v["sections"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["name"].as_str().unwrap().to_string())
            .collect::<Vec<_>>()
    };

    // Explicit order, and a name left out must not be stranded ahead of the
    // listed ones.
    let res = admin
        .put(format!("{base}/api/v1/admin/sections"))
        .header(reqwest::header::COOKIE, &session)
        .json(&json!({ "order": ["CDN", "API"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let listed: Value = res.json().await.unwrap();
    assert_eq!(names(&listed), vec!["CDN", "API", "Storage"]);

    // A check in a section follows it through a rename.
    let mut check = check_body("edge-cache", "https://edge.test");
    check["group"] = json!("CDN");
    post("/api/v1/admin/checks", check).await;

    let res = admin
        .put(format!("{base}/api/v1/admin/sections/CDN"))
        .header(reqwest::header::COOKIE, &session)
        .json(&json!({ "name": "Edge" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let checks: Value = admin
        .get(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let row = checks["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "edge-cache")
        .unwrap();
    assert_eq!(
        row["group"], "Edge",
        "renaming a section must carry its checks, not orphan them"
    );

    // Deleting a section must not delete the monitoring inside it.
    let res = admin
        .delete(format!("{base}/api/v1/admin/sections/Edge"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let checks: Value = admin
        .get(format!("{base}/api/v1/admin/checks"))
        .header(reqwest::header::COOKIE, &session)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let row = checks["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "edge-cache")
        .expect("the check survives its section being deleted");
    assert!(
        row.get("group").is_none() || row["group"].is_null(),
        "the check should be loose, got {row}"
    );
}
