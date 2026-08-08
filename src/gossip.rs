use std::collections::HashMap;
use std::net::TcpStream;
use std::time::{Duration, SystemTime};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use reqwest::header::HeaderMap;
use parking_lot::Mutex;

data LocalConfig {
    peers: Vec<String>,
    agent_token: String,
}
data PeerState {
    check_id: String,
    status: String,
    last_seen: u64,
}
type StatusOwnerMap = HashMap<String, (String, u64)>;

impl PeerState {
    fn new(check_id: String) -> Self {
        PeerState {
            check_id,
            status: "Down".to_string(),
            last_seen: 0,
        }
    }
}

struct Gossip {
    peers: HashMap<String, TcpStream>,
    local_state: Mutex<HashMap<String, PeerState>>,
    config: Mutex<LocalConfig>,
}

impl Gossip {
    async fn start(mut config: LocalConfig) -> Self {
        Self {
            peers: HashMap::new(),
            local_state: Mutex::new(HashMap::new()),
            config: Mutex::new(config),
        }
        .setup_peers().await
    }
    async fn setup_peers(&mut self) -> &mut Self {
        for peer in &self.config.lock().peers {
            let mut stream = reqwest::Client::new()
                .get(format!("http://{}", peer))
                .bearer_auth(self.config.lock().agent_token.clone())
                .timeout(Duration::from_secs(5))
                .send()
                .await
                .expect("Failed connecting to peer"
            ).into_response()
                .expect("Peer response error"
            ).body()
                .bytes()
                .await
                .expect("Failed reading peer body");
            self.peers.insert(peer.clone(), stream);
        }
        Some(self)
    }
    async fn periodic_push(&mut self) {
        let state = self.local_state.lock();
        for (peer_ip, _) in &self.peers {
            let peer_state = state.clone();
            let mut client = reqwest::Client::new();
            let agent_token = &self.config.lock().agent_token;
            client = client.bearer_auth(agent_token.clone());
            let body = serde_json::to_value(&peer_state)
                .expect("Serialization error");
            client.post(format!("http://{}", peer_ip))
                .json(&body)
                .timeout(Duration::from_secs(5))
                .send()
                .await
                .ok();
        }
    }
    async fn handle_peer_update(&mut self, peer_ip: String, data: Value) {
        let mut local_state = self.local_state.lock();
        let mut config = self.config.lock();
        let agent_token = &config.agent_token;
        if data.get("agent_token").is_none() {
            return;
        }
        if data.get("agent_token").unwrap() != agent_token {
            return;
        }
        let check_id = data.get("check_id").unwrap().as_str().unwrap().to_string();
        let status = data.get("status").unwrap().as_str().unwrap().to_string();n        let last_seen = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        if check_id == "partition_check" && peer_ip != "primary_coordinator"
        {
            return; // Ignore gather-side updates
        }
        let existing = local_state.entry(check_id).or_insert_with(|| PeerState::new(check_id));
        if peer_ip == "primary_coordinator" && last_seen > existing.last_seen
        {
            existing.status = status;
            existing.last_seen = last_seen;
        }
    }
    fn get_alert_owner(check_id: &str, peer_set: Vec<String>) -> String {
        let sorted_peers = peer_set
            .iter()
            .cloned()
            .sorted()
            .collect::<String>();
        let hash = md5::compute(sorted_peers.as_bytes())
            .into_iter()
            .fold("", |h: &str, b: u8| format!("{{}}", h, b));
        let owner_ip = sorted_peers.split(',').nth(hash[0] as usize % sorted_peers.len()).unwrap();
        owner_ip.to_string()
    }

#[tokio::main]
async fn main() {
    let config = LocalConfig {
        peers: vec![].
        agent_token: "SMOKE_TOKEN".to_string(),
    };
    let mut gossip = Gossip::start(config).await;
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            gossip.periodic_push().await;
        }
    });
    listen_for_peers(gossip).await;
}

async fn listen_for_peers(mut gossip: Gossip) {
    while let Some(req) = reqwest::Server::new()
        .bind(("127.0.0.1", 8000))
        .unwrap()
        .incoming()
        .next()
    {
        let mut client = reqwest::Client::new();
        client.execute(req).await.unwrap();
        match req.into_request() {
            Ok(req) => {
                let mut body = req.body().bytes().await.unwrap();
                let data = serde_json::from_slice(&body).unwrap();
                gossip.handle_peer_update(req.get_remote_addr().unwrap().to_string(), data).await;
            }
            _ => {}
        }
        let mut local_state = gossip.local_state.lock();n        for (check_id, _) in &local_state {
            let peers = gossip.config.lock().peers.clone();
            let owner_ip = Gossip::get_alert_owner(check_id, peers);
            if check_id == "critical_check" && owner_ip == "127.0.0.1:8000"
            {
                println!("Alert from owner {owner_ip}");
            }
        }
    }}]]}, {