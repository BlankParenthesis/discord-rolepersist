use std::fs;
use std::fmt;
use std::sync::{Arc, Weak};

use rusqlite::{Connection, Result};
use serenity::all::UnavailableGuild;

use std::future::Future;

use serenity::{async_trait, prelude::*};
use serenity::model::gateway::Ready;
use serenity::model::id::{UserId, GuildId, RoleId};
use serenity::model::guild::{Member, Guild};
use serenity::model::event::GuildMemberUpdateEvent;

use serde::Deserialize;
use serde::de::{Deserializer, Visitor};

use weak_table::WeakValueHashMap;

struct SimpleMember {
    joined_at: i64,
    user_id: u64,
    server_id: u64,
    roles: Vec<u64>,
}

impl From<&Member> for SimpleMember {
    fn from(member: &Member) -> Self {
        SimpleMember {
            joined_at: member.joined_at.unwrap_or_default().unix_timestamp(),
            user_id: member.user.id.get(),
            server_id: member.guild_id.get(),
            roles: member.roles.iter().cloned().map(|r| r.get()).collect(),
        }
    }
}

impl From<Member> for SimpleMember {
    fn from(member: Member) -> Self {
        Self::from(&member)
    }
}

impl From<&GuildMemberUpdateEvent> for SimpleMember {
    fn from(member: &GuildMemberUpdateEvent) -> Self {
        SimpleMember {
            joined_at: member.joined_at.unix_timestamp(),
            user_id: member.user.id.get(),
            server_id: member.guild_id.get(),
            roles: member.roles.iter().cloned().map(|r| r.get()).collect(),
        }
    }
}

impl From<GuildMemberUpdateEvent> for SimpleMember {
    fn from(member: GuildMemberUpdateEvent) -> Self {
        Self::from(&member)
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

        let mut connection = self.data.lock().await;
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

    pub async fn restore_member(
        &self, 
        context: &Context, 
        member: &mut SimpleMember
    ) {
        let connection = self.data.lock().await;
        let roles: Vec<RoleId> = {
            let mut roles_query = connection.prepare(
                "SELECT role_id FROM roles 
                WHERE user_id=?1 AND server_id=?2",
            ).unwrap();

            roles_query.query_map(
                [member.user_id, member.server_id],
                |row| Ok(RoleId::new(row.get(0)?))
            ).unwrap().collect::<Result<_>>().unwrap()
        };

        for role in roles {
            if !member.roles.contains(&role.get()) {
                let role_add_attempt = context.http.add_member_role(
                    GuildId::new(member.server_id), 
                    UserId::new(member.user_id), 
                    role,
                    Some("Granting previously assigned roles"),
                ).await;

                if let Err(error) = role_add_attempt {
                    println!(
                        "error restoring role {} for member {} in server {}: {:?}", 
                        role.get(), 
                        member.user_id, 
                        member.server_id,
                        error,
                    );
                } else {
                    member.roles.push(role.get());
                }
           }
        }
    }

    async fn last_seen(&self, member: &SimpleMember) -> Option<i64> {
        let connection = self.data.lock().await;
        let mut last_seen_query = connection.prepare(
            "SELECT time FROM last_seen 
            WHERE user_id=?1 AND server_id=?2",
        ).unwrap();

        let last_seen: Vec<i64> = last_seen_query.query_map(
            [member.user_id, member.server_id],
            |row| row.get::<usize, i64>(0)
        ).unwrap().collect::<Result<_>>().unwrap();

        assert!(last_seen.len() <= 1);
        last_seen.first().copied()
    }

    pub async fn observe_member(&self, context: &Context, member: &mut SimpleMember) {
        let key: (UserId, GuildId) = (member.user_id.into(), member.server_id.into());
        self.do_locked(key, || async {
            if let Some(last_seen) = self.last_seen(member).await {
                if last_seen < member.joined_at {
                    // Member has left and rejoined since we last observed at them.
                    self.restore_member(context, member).await;
                }
            }
            
            self.save_member(member).await;
        }).await;
    }

    pub async fn save_guild(&self, context: &Context, server_id: GuildId) -> std::result::Result<(), serenity::Error> {
        let result = context.http.get_guild_members(server_id, None, None).await;

        match result {
            Ok(members) => {
                for member in members {
                    self.observe_member(context, &mut member.into()).await
                }
                Ok(())
            },
            Err(e) => Err(e),
        }
    }

    pub async fn forget_guild(&self, server_id: GuildId) {
        let mut connection = self.data.lock().await;
        let transaction = connection.transaction().unwrap();

        transaction.execute(
            "DELETE FROM roles WHERE server_id=?",
            [server_id.get()],
        ).unwrap();

        transaction.execute(
            "DELETE FROM last_seen WHERE server_id=?",
            [server_id.get()],
        ).unwrap();

        transaction.commit().unwrap();
    }

    pub fn filter_allow_server(&self, id: GuildId) -> bool {
        if let Some(restrict) = &self.config.restrict {
            restrict.is_restricted(id.get())
        } else {
            true
        }
    } 

    pub async fn do_locked<
        F: Future<Output = ()>,
        FN: FnOnce() -> F,
    >(
        &self, 
        key: (UserId, GuildId),
        function: FN,
    ) {
        let mut locks = self.member_locks.lock().await;

        if let Some(user_lock) = locks.get(&key) {
            std::mem::drop(locks);
            let _lock = user_lock.lock().await;
            function().await;
        } else {
            let user_lock = Arc::new(Mutex::new(()));
            locks.insert(key, user_lock.clone());
            let _lock = user_lock.lock().await;
            std::mem::drop(locks);
            function().await;
        }
    }
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, context: Context, ready: Ready) {
        let guilds: Vec<_> = ready.guilds.into_iter()
            .filter(|guild| self.filter_allow_server(guild.id))
            .collect();
        
        for guild in guilds {
            if let Err(error) = self.save_guild(&context, guild.id).await {
                println!("Error fetching members of guild {}: {}", guild.id, error);
            }
        }
    }

    async fn guild_create(&self, context: Context, guild: Guild, _is_new: Option<bool>) {
        if self.filter_allow_server(guild.id) {
            if let Err(error) = self.save_guild(&context, guild.id).await {
                println!("Error fetching members of guild {}: {}", guild.id.get(), error);
            }
        }
    }

    async fn guild_delete(&self, _context: Context, guild: UnavailableGuild, _full: Option<Guild>) {
        if !guild.unavailable {
            self.forget_guild(guild.id).await;
        }
    }
        
    async fn guild_member_addition(&self, context: Context, member: Member) {
        if self.filter_allow_server(member.guild_id) {
            self.observe_member(&context, &mut member.into()).await
        }
    }
    
    async fn guild_member_update(
        &self, 
        context: Context, 
        _old: Option<Member>,
        _new: Option<Member>,
        update: GuildMemberUpdateEvent
    ) {
        if self.filter_allow_server(update.guild_id) {
            self.observe_member(&context, &mut update.into()).await
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
        let is_listed = self.servers.iter()
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
    let intents = GatewayIntents::GUILDS | GatewayIntents::GUILD_MEMBERS;
    let handler = Handler::new(config).unwrap();

    let mut client = Client::builder(&token, intents)
        .event_handler(handler).await
        .unwrap();
    
    if let Err(cause) = client.start_autosharded().await {
        println!("Client error: {:?}", cause);
    }
}
