use std::env;

use rusqlite::{Connection, Result};

use serenity::{async_trait, prelude::*};
use serenity::model::gateway::Ready;
use serenity::model::id::{GuildId, RoleId};
use serenity::model::guild::Member;
use serenity::model::event::GuildMemberUpdateEvent;
use serenity::client::bridge::gateway::GatewayIntents;

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
}

impl Handler {
    pub fn new() -> Result<Self> {
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
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(
        &self, 
        context: Context, 
        ready: Ready
    ) {
        let futures: Vec<_> = ready.guilds.into_iter()
            .map(|guild| (&context).http.get_guild_members(guild.id().0, None, None))
            .collect();
        
        let member_lists = futures::future::join_all(futures).await;

        for result in member_lists {
            match result {
                Ok(members) => {
                    for member in members {
                        self.observe_member(&context, member).await;
                    }
                },
                Err(error) => println!("Error fetching some members: {}", error),
            }
        }
    }
        
    async fn guild_member_addition(
        &self, 
        context: Context, 
        _guild_id: GuildId, 
        member: Member
    ) {
        self.restore_member(&context, member).await;
    }
    
    async fn guild_member_update(
        &self, 
        _context: Context, 
        update: GuildMemberUpdateEvent
    ) {
        self.save_member(&(&update).into()).await;
    }
}

#[tokio::main]
async fn main() {
    let token = env::var("DISCORD_TOKEN")
        .expect("Expected DISCORD_TOKEN environment variable");

    let handler = Handler::new().unwrap();

    let mut client = Client::builder(&token)
        .intents(GatewayIntents::GUILDS | GatewayIntents::GUILD_MEMBERS)
        .event_handler(handler).await
        .unwrap();
    
    if let Err(cause) = client.start_autosharded().await {
        println!("Client error: {:?}", cause);
    }
}
