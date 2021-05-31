use std::fs;
use std::fmt;
use std::sync::{Arc, Weak};

use rusqlite::{Connection, Result};

use std::future::Future;

use serenity::{async_trait, prelude::*};
use serenity::model::gateway::Ready;
use serenity::model::id::{UserId, GuildId, RoleId};
use serenity::model::guild::{Member, Guild, GuildUnavailable};
use serenity::model::event::GuildMemberUpdateEvent;
use serenity::client::bridge::gateway::GatewayIntents;

use serde::Deserialize;
use serde::de::{Deserializer, Visitor};

use weak_table::WeakValueHashMap;

struct SimpleMember {
    joined_at: i64,
    user_id: u64,
    server_id: u64,
    roles: Vec<u64>
}

impl From<&Member> for SimpleMember {
    fn from(member: &Member) -> Self {
        SimpleMember {
            joined_at: member.joined_at.unwrap().timestamp(),
            user_id: member.user.id.0,
            server_id: member.guild_id.0,
            roles: member.roles.clone().into_iter().map(|r| r.0).collect(),
        }
    }
}

impl From<&GuildMemberUpdateEvent> for SimpleMember {
    fn from(member: &GuildMemberUpdateEvent) -> Self {
        SimpleMember {
            joined_at: member.joined_at.timestamp(),
            user_id: member.user.id.0,
            server_id: member.guild_id.0,
            roles: member.roles.clone().into_iter().map(|r| r.0).collect(),
        }
    }
}

struct Handler {
    data: Mutex<Connection>,
    config: Config,
    member_locks: Mutex<WeakValueHashMap<(UserId, GuildId), Weak<Mutex<()>>>>,
}

impl Handler {
    pub fn new(config: Config) -> Result<Self> {
        let connection = Connection::open("data.db")?;

        connection.execute(
            "CREATE TABLE IF NOT EXISTS roles(
                user_id NUMBER,
                server_id NUMBER,
                role_id NUMBER
            )", 
            []
        )?;

        connection.execute(
            "CREATE TABLE IF NOT EXISTS last_seen(
                user_id NUMBER,
                server_id NUMBER,
                time INTEGER,
                PRIMARY KEY(user_id, server_id)
            )", 
            []
        )?;

        Ok(Self {
            data: Mutex::new(connection),
            config,
            member_locks: Mutex::new(WeakValueHashMap::new()),
        })
    }

    pub async fn save_member(&self, member: &SimpleMember) {
        let now = std::time::SystemTime::now();
        let since_epoch = now.duration_since(std::time::UNIX_EPOCH).unwrap();

        let mut connection = (&self.data).lock().await;
        let transaction = connection.transaction().unwrap();
        transaction.execute(
            "REPLACE INTO last_seen (user_id, server_id, time) VALUES (?1, ?2, ?3)",
            [member.user_id, member.server_id, since_epoch.as_secs()],
        ).unwrap();

        transaction.execute(
            "DELETE FROM roles WHERE user_id=?1 AND server_id=?2",
            [member.user_id, member.server_id],
        ).unwrap();

        for role_id in &member.roles {
            transaction.execute(
                "INSERT INTO roles (user_id, server_id, role_id) VALUES (?1, ?2, ?3)",
                [member.user_id, member.server_id, *role_id],
            ).unwrap();
        }

        transaction.commit().unwrap();
    }

    pub async fn restore_member(&self, context: &Context, mut member: Member) {
        let simple_member = SimpleMember::from(&member);
        let connection = (&self.data).lock().await;
        let roles: Vec<RoleId>;
        {
            let mut roles_query = connection.prepare(
                "SELECT role_id FROM roles 
                WHERE user_id=?1 AND server_id=?2",
            ).unwrap();

            roles = roles_query.query_map(
                [simple_member.user_id, simple_member.server_id],
                |row| Ok(RoleId(row.get(0)?))
            ).unwrap().collect::<Result<_>>().unwrap();
        }

        let add_roles_attempt = member.add_roles(&context.http, &roles[0..]).await;

        if let Err(error) = add_roles_attempt {
            println!(
                "error restoring roles for member {} in server {}: {:?}", 
                simple_member.user_id, 
                simple_member.server_id,
                error,
            );
        }
    }

    pub async fn observe_member(&self, context: &Context, member: Member) {
        let last_seen: Vec<i64>;

        let simple_member = SimpleMember::from(&member);
        {
            let connection = (&self.data).lock().await;
            let mut last_seen_query = connection.prepare(
                "SELECT time FROM last_seen 
                WHERE user_id=?1 AND server_id=?2",
            ).unwrap();

            last_seen = last_seen_query.query_map(
                [simple_member.user_id, simple_member.server_id],
                |row| row.get::<usize, i64>(0)
            ).unwrap().collect::<Result<_>>().unwrap();
        }

        assert!(last_seen.len() <= 1);

        if let Some(last_seen) = last_seen.get(0) {
            if *last_seen < simple_member.joined_at {
                // Member has left and rejoined since we last observed at them.
                self.restore_member(context, member).await;
            }
        }

        self.save_member(&simple_member).await;
    }

    pub async fn save_guild(&self, context: &Context, server_id: GuildId) -> std::result::Result<(), serenity::Error> {
        let result = context.http.get_guild_members(server_id.0, None, None).await;

        match result {
            Ok(members) => {
                for member in members {
                    self.observe_member(context, member).await
                }
                Ok(())
            },
            Err(e) => Err(e),
        }
    }

    pub async fn forget_guild(&self, server_id: GuildId) {
        let mut connection = (&self.data).lock().await;
        let transaction = connection.transaction().unwrap();

        transaction.execute(
            "DELETE FROM roles WHERE server_id=?2",
            [server_id.0],
        ).unwrap();

        transaction.execute(
            "DELETE FROM last_seen WHERE server_id=?2",
            [server_id.0],
        ).unwrap();

        transaction.commit().unwrap();
    }

    pub fn filter_allow_server(&self, id: GuildId) -> bool {
        if let Some(restrict) = &self.config.restrict {
            restrict.is_restricted(id.0)
        } else {
            true
        }
    } 

    pub async fn do_locked<
        F: Future<Output = ()>,
        FN: FnOnce() -> F,
    >(
        &self, 
        user_id: UserId, 
        guild_id: GuildId, 
        function: FN
    ) {
        let key = (user_id, guild_id);
        let mut locks = self.member_locks.lock().await;

        if let Some(user_lock) = locks.get(&key) {
            std::mem::drop(locks);
            let _lock = user_lock.lock().await;
            function().await
        } else {
            let user_lock = Arc::new(Mutex::new(()));
            locks.insert(key, user_lock.clone());
            let _lock = user_lock.lock().await;
            std::mem::drop(locks);
            function().await
        }
    }
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(
        &self, 
        context: Context, 
        ready: Ready
    ) {
        let guilds: Vec<_> = ready.guilds.into_iter()
            .filter(|guild| self.filter_allow_server(guild.id()))
            .collect();
        
        for guild in guilds {
            if let Err(error) = self.save_guild(&context, guild.id()).await {
                println!("Error fetching members of guild {}: {}", guild.id(), error);
            }
        }
    }

    async fn guild_create(&self, context: Context, guild: Guild) {
        if self.filter_allow_server(guild.id) {
            if let Err(error) = self.save_guild(&context, guild.id).await {
                println!("Error fetching members of guild {}: {}", guild.id.0, error);
            }
        }
    }

    async fn guild_delete(&self, _context: Context, guild: GuildUnavailable) {
        self.forget_guild(guild.id).await;
    }
        
    async fn guild_member_addition(
        &self, 
        context: Context, 
        guild_id: GuildId, 
        member: Member
    ) {
        if self.filter_allow_server(guild_id) {
            self.do_locked(
                member.user.id, 
                guild_id, 
                || async { 
                    self.restore_member(&context, member).await 
                },
            ).await;
        }
    }
    
    async fn guild_member_update(
        &self, 
        _context: Context, 
        update: GuildMemberUpdateEvent
    ) {
        if self.filter_allow_server(update.guild_id) {
            self.do_locked(
                update.user.id, 
                update.guild_id, 
                || async { 
                    self.save_member(&(&update).into()).await;
                }
            ).await;
        }
    }
}

enum RestrictionMode {
    Allow,
    Deny,
}

struct RestrictionVisitor;

impl<'de> Visitor<'de> for RestrictionVisitor {
    type Value = RestrictionMode;
    
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("the string 'allow' or the string 'deny'")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> 
    where E: serde::de::Error {
        match value {
            "allow" => Ok(RestrictionMode::Allow),
            "deny" => Ok(RestrictionMode::Deny),
            _ => Err(E::custom(format!("{} is not a restriction mode", value))),
        }
    }
}

impl<'de> Deserialize<'de> for RestrictionMode {
    fn deserialize<D>(deserializer: D) -> Result<RestrictionMode, D::Error>

    where D: Deserializer<'de> {
        deserializer.deserialize_str(RestrictionVisitor)
    }
}

#[derive(Deserialize)]
struct Restriction {
    mode: RestrictionMode,
    servers: Vec<u64>,
}

impl Restriction {
    pub fn is_restricted(&self, server_id: u64) -> bool {
        let is_listed = (&self.servers).into_iter()
            .find(|id| **id == server_id);
        
        match self.mode {
            RestrictionMode::Allow => is_listed.is_some(),
            RestrictionMode::Deny => is_listed.is_none(),
        }
    }
}

#[derive(Deserialize)]
struct Config {
    token: String,
    restrict: Option<Restriction>
}

#[tokio::main]
async fn main() {
    let config_contents = fs::read_to_string("config.json")
        .expect("Unable to read config file");
    let config: Config = serde_json::from_str(&config_contents)
        .expect("Unable to parse config file");

    let token = config.token.clone();
        
    let handler = Handler::new(config).unwrap();

    let mut client = Client::builder(&token)
        .intents(GatewayIntents::GUILDS | GatewayIntents::GUILD_MEMBERS)
        .event_handler(handler).await
        .unwrap();
    
    if let Err(cause) = client.start_autosharded().await {
        println!("Client error: {:?}", cause);
    }
}
