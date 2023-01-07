use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use demoji::demoji;
use rusqlite::{params, params_from_iter, Connection, OptionalExtension, Row, Statement, ToSql};
use teloxide::prelude::*;
use teloxide::requests::JsonRequest;
use teloxide::types::{
    AllowedUpdate, InlineKeyboardButton, InlineKeyboardMarkup, MessageId, UpdateKind, User,
};
use teloxide::{payloads, RequestError};
use tokio::time::sleep;

const DISPLAY_REMOVE_RANKING_BUTTON: bool = true;

#[derive(Debug)]
pub enum BotError {
    TelegramError(RequestError),
    SqliteError(rusqlite::Error),
}

impl From<RequestError> for BotError {
    fn from(error: RequestError) -> Self {
        BotError::TelegramError(error)
    }
}

impl From<rusqlite::Error> for BotError {
    fn from(error: rusqlite::Error) -> Self {
        BotError::SqliteError(error)
    }
}

struct BotWrapper<'a> {
    bot: Bot,
    get_reactions: Statement<'a>,
    find_reaction: Statement<'a>,
    add_reaction: Statement<'a>,
    delete_reaction: Statement<'a>,
    delete_message: Statement<'a>,
    check_if_reaction: Statement<'a>,
    get_reaction_post: Statement<'a>,
    show_reactions: Statement<'a>,
    hide_reactions: Statement<'a>,
    top_all_users: Statement<'a>,
    top_per_user: Statement<'a>,
    most_received: Statement<'a>,
    most_given: Statement<'a>,
    save_message: Statement<'a>,
}

impl<'a> BotWrapper<'a> {
    fn new(connection: &'a Connection) -> Result<Self, rusqlite::Error> {
        Ok(Self {
            bot: Bot::from_env(),
            get_reactions: connection.prepare("SELECT type || ': ' || GROUP_CONCAT(author, ', '), CASE WHEN COUNT(*) > 1 THEN COUNT(*) || ' ' || type ELSE type END FROM reaction WHERE parent = ? GROUP BY type")?,
            find_reaction: connection.prepare("SELECT id FROM reaction WHERE parent = ? AND author_id = ? AND type = ?")?,
            add_reaction: connection.prepare("INSERT INTO reaction (author_id, author, type, timestamp, parent) VALUES (?, ?, ?, ?, ?)")?,
            delete_reaction: connection.prepare("DELETE FROM reaction WHERE id = ?")?,
            delete_message: connection.prepare("DELETE FROM message WHERE id = ?")?,
            check_if_reaction: connection.prepare("SELECT t.is_bot_reaction, u.original_id FROM message AS t LEFT JOIN message AS u ON t.parent = u.id WHERE t.id = ?")?,
            get_reaction_post: connection.prepare("SELECT original_id, expanded FROM message WHERE parent = ? AND is_bot_reaction = TRUE")?,
            show_reactions: connection.prepare("UPDATE message SET expanded = TRUE WHERE id = ?")?,
            hide_reactions: connection.prepare("UPDATE message SET expanded = FALSE WHERE id = ?")?,
            top_all_users: connection.prepare("SELECT message.original_id, COUNT(*) AS c FROM message \
                INNER JOIN reaction \
                ON message.id = reaction.parent WHERE reaction.timestamp > ? \
                AND message.chat_id = ? \
                GROUP BY message.id ORDER BY c DESC \
                LIMIT ?")?,
            top_per_user: connection.prepare("select message.original_id, count(*) as c from message \
                inner join reaction \
                on message.id = reaction.parent where reaction.timestamp > ? \
                and message.chat_id = ? \
                and message.author = ? \
                group by message.id order by c desc \
                limit ?")?,
            most_received: connection.prepare("SELECT author, sum(msg_reactions.cnt) \
                from message inner join (select parent, count(*) as cnt from reaction where timestamp > ? group by parent) as msg_reactions \
                on message.id=msg_reactions.parent \
                where chat_id=? \
                group by message.author_id, message.author \
                order by sum(msg_reactions.cnt) desc")?,
            most_given: connection.prepare("SELECT author, count(*) \
                from reaction inner join (select id, chat_id from message) as reaction_msg \
                on reaction_msg.id=reaction.parent \
                where timestamp > ? and reaction_msg.chat_id = ? \
                group by reaction.author_id, reaction.author \
                order by count(*) desc")?,
            save_message: connection.prepare("INSERT INTO message (id, original_id, author_id, author, chat_id, parent, is_bot_reaction, is_ranking, is_anon) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")?,
        })
    }

    fn get_updates(&self) -> JsonRequest<payloads::GetUpdates> {
        self.bot.get_updates()
    }

    async fn delete_message(
        &mut self,
        chat_id: ChatId,
        message_id: MessageId,
    ) -> Result<(), BotError> {
        self.bot.delete_message(chat_id, message_id).await?;
        self.delete_message
            .execute([get_custom_msg_id(chat_id, message_id)])?;
        Ok(())
    }

    async fn reply_to(&mut self, msg: &Message, text: &str) -> Result<(), BotError> {
        let msg = &self
            .bot
            .send_message(msg.chat.id, text)
            .reply_to_message_id(msg.id)
            .await?;
        self.save_msg_to_db(msg, false, false)
    }

    async fn react_to(
        &mut self,
        mut msg_id: MessageId,
        chat_id: ChatId,
        author: &User,
        reactions: Vec<&str>,
    ) -> Result<(), BotError> {
        // TODO puste reactions
        let mut msg_custom_id = get_custom_msg_id(chat_id, msg_id);
        let (is_bot_reaction, grandparent_id): (bool, Option<i32>) = self
            .check_if_reaction
            .query_row([msg_custom_id], |row| Ok((row.get(0)?, row.get(1)?)))?;

        if is_bot_reaction {
            assert!(grandparent_id.is_some());
            msg_id = MessageId(grandparent_id.unwrap());
            msg_custom_id = get_custom_msg_id(chat_id, msg_id);
        }

        for &reaction in reactions.iter() {
            self.save_single_reaction(msg_custom_id, author, reaction)
                .await?;
        }

        let reaction_post_data: Option<(i32, bool)> = self
            .get_reaction_post
            .query_row([msg_custom_id], |row| Ok((row.get(0)?, row.get(1)?)))
            .optional()?;

        match reaction_post_data {
            Some((reaction_post_id, expanded)) => {
                let inline_keyboard_markup = self.get_inline_keyboard(msg_custom_id, expanded)?;
                match inline_keyboard_markup {
                    None => {
                        self.delete_message(chat_id, MessageId(reaction_post_id))
                            .await
                    }
                    Some((inline_keyboard_markup, reaction_message_text)) => {
                        self.bot
                            .edit_message_text(
                                chat_id,
                                MessageId(reaction_post_id),
                                reaction_message_text,
                            )
                            .reply_markup(inline_keyboard_markup)
                            .await?; // TODO walidacja? Bo docsy mówią niesamowite rzeczy o tym co to zwraca xD
                        Ok(())
                    }
                }
            }
            None => {
                let message = self
                    .bot
                    .send_message(chat_id, "\u{ad}\u{ad}")
                    .reply_to_message_id(msg_id)
                    .reply_markup(self.get_inline_keyboard(msg_custom_id, false)?.unwrap().0)
                    .await?;
                self.save_msg_to_db(&message, true, false)
            }
        }
    }

    async fn save_single_reaction(
        &mut self,
        msg_custom_id: u64,
        author: &User,
        reaction: &str,
    ) -> Result<(), rusqlite::Error> {
        let row: Option<i64> = self
            .find_reaction
            .query_row(params![msg_custom_id, author.id.0, reaction], |row| {
                row.get(0)
            })
            .optional()?;
        match row {
            Some(id) => self.delete_reaction.execute([id]),
            None => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                let params = params![author.id.0, author.username, reaction, now, msg_custom_id];
                self.add_reaction.execute(params)
            }
        }
        .map(|_| ())
    }

    async fn handle_callback_query(&mut self, cq: CallbackQuery) -> Result<(), BotError> {
        match cq.data {
            None => panic!("co to jest???"),
            Some(text) => {
                let msg = cq.message.unwrap();
                let parent = msg.reply_to_message().unwrap();
                let msg_custom_id = get_custom_msg_id(msg.chat.id, parent.id);
                match text.as_str() {
                    "show_reactions" | "hide_reactions" => {
                        if text.as_str() == "show_reactions" {
                            self.show_reactions.execute([msg_custom_id])?;
                        } else {
                            self.hide_reactions.execute([msg_custom_id])?;
                        }
                        let (markup, text) =
                            self.get_inline_keyboard(msg_custom_id, true)?.unwrap();
                        self.bot
                            .edit_message_text(msg.chat.id, msg.id, text)
                            .reply_markup(markup)
                            .await?;
                        Ok(())
                    }
                    "delete_ranking" => self.delete_message(msg.chat.id, msg.id).await,
                    text => {
                        self.react_to(parent.id, msg.chat.id, &cq.from, vec![text])
                            .await
                    }
                }
            }
        }
    }

    async fn top(&mut self, msg: &Message, text: &str) -> Result<(), BotError> {
        let args = text.split(' ').collect::<Vec<_>>();
        if args.len() > 4 {
            return self
                .reply_to(msg, "Usage: /top [days] [number of messages] [@author]")
                .await;
        }
        let mut days: u64 = 7;
        let mut number_of_messages = 10;
        if args.len() > 1 {
            match args[1].parse::<u64>() {
                Ok(i) => days = i,
                Err(_) => {
                    return self
                        .reply_to(msg, "Usage: /top [days] [number of messages] [@author]")
                        .await
                }
            }
            if days < 1 {
                return self.reply_to(msg, "Days argument must be >= 1.").await;
            }
            days = min(days, 10 * 365);
        }
        if args.len() > 2 {
            match args[2].parse::<i32>() {
                Ok(i) => number_of_messages = i,
                Err(_) => {
                    return self
                        .reply_to(msg, "Usage: /top [days] [number of messages] [@author]")
                        .await
                }
            }
            if !(1..30).contains(&number_of_messages) {
                return self
                    .reply_to(msg, "Number of messages must be between 1 and 30.")
                    .await;
            }
            number_of_messages *= 3;
        }
        let min_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
            - days * (24 * 60 * 60 * 1000000000);

        let mut query_arguments = vec![
            min_timestamp.to_sql().unwrap(),
            msg.chat.id.0.to_sql().unwrap(),
            number_of_messages.to_sql().unwrap(),
        ];

        let f = |row: &Row| Ok((row.get(0)?, row.get(1)?));
        let rows = if args.len() == 4 {
            query_arguments.insert(2, args[3].to_sql().unwrap());
            let params = params_from_iter(query_arguments.iter());
            self.top_per_user.query_map(params, f)?
        } else {
            let params = params_from_iter(query_arguments.iter());
            self.top_all_users.query_map(params, f)?
        }
        .map(|row| row.unwrap())
        .collect::<Vec<_>>();
        let mut how_many = 0;
        for row in rows {
            let (original_id, count): (i32, i64) = row;
            let msg = self
                .bot
                .send_message(msg.chat.id, format!("{}. {}", how_many + 1, count))
                .reply_to_message_id(MessageId(original_id))
                .await;
            if msg.is_ok() {
                how_many += 1;
                self.save_msg_to_db(&msg?, false, false)?;
            }
            if how_many == number_of_messages / 3 {
                break;
            }
            sleep(Duration::from_secs_f64(0.5)).await;
        }
        Ok(())
    }

    async fn ranking(&mut self, msg: &Message, text: &str) -> Result<(), BotError> {
        let args = text.split(' ').collect::<Vec<_>>();
        if args.len() > 2 {
            return self.reply_to(msg, "Usage: /ranking [days]").await;
        }
        let mut days: u64 = 7;
        if args.len() == 1 {
            match args[1].parse::<u64>() {
                Ok(i) => days = i,
                Err(_) => return self.reply_to(msg, "Usage: /ranking [days]").await,
            }
            if days < 1 {
                return self.reply_to(msg, "Days argument must be >= 1.").await;
            }
            days = min(days, 10 * 365);
        }
        let min_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
            - days * (24 * 60 * 60 * 1000000000);

        let mut text = format!("Reactions received in the last {days} days\n");

        let rows = self
            .most_received
            .query_map(params![min_timestamp, msg.chat.id.0], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?;

        for (i, row) in rows.enumerate() {
            let (username, count): (String, i64) = row?;
            text += &format!("{}. {username}: {count}\n", i + 1);
        }

        text += &format!("\nReactions given in the last {days} days\n");

        let rows = self
            .most_given
            .query_map(params![min_timestamp, msg.chat.id.0], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?;

        for (i, row) in rows.enumerate() {
            let (username, count): (String, i64) = row?;
            text += &format!("{}. {username}: {count}\n", i + 1);
        }

        let mut action = self
            .bot
            .send_message(msg.chat.id, text)
            .reply_to_message_id(msg.id);

        if DISPLAY_REMOVE_RANKING_BUTTON {
            // TODO yet another forbidden reaction
            action = action.reply_markup(InlineKeyboardMarkup {
                inline_keyboard: vec![vec![InlineKeyboardButton::callback(
                    "delete ranking",
                    "delete_ranking",
                )]],
            });
        }
        self.save_msg_to_db(&action.await?, false, true)
    }

    async fn help(&mut self, msg: &Message, text: &str) -> Result<(), BotError> {
        if text != "/help " {
            return self.reply_to(msg, "Usage: /help").await;
        }
        self.reply_to(msg, "TODO blep").await
    }

    async fn anon(&mut self, msg: &Message, text: &str) -> Result<(), BotError> {
        let position = text.chars().position(|c| c == ' ').unwrap() + 1;
        let text = format!("Anonim: {}", &text[position..]);
        match msg.reply_to_message() {
            None => {
                let send = &self.bot.send_message(msg.chat.id, text).await?;
                self.save_msg_to_db(send, false, false)?
            }
            Some(msg) => self.reply_to(msg, &text).await?,
        }
        self.delete_message(msg.chat.id, msg.id).await
    }

    async fn handle_command(
        &mut self,
        cmd: Command,
        msg: &Message,
        text: &str,
    ) -> Result<(), BotError> {
        match cmd {
            Command::Help => self.help(msg, text).await,
            Command::Top => self.top(msg, text).await,
            Command::Ranking => self.ranking(msg, text).await,
            Command::Anon => self.anon(msg, text).await,
        }
    }

    fn save_msg_to_db(
        &mut self,
        msg: &Message,
        is_bot_reaction: bool,
        is_ranking: bool,
    ) -> Result<(), BotError> {
        let custom_id = get_custom_msg_id(msg.chat.id, msg.id);
        let parent = msg
            .reply_to_message()
            .map(|parent| get_custom_msg_id(msg.chat.id, parent.id));
        let author = msg.from().unwrap();

        let params = params![
            custom_id,
            msg.id.0,
            author.id.0,
            author.username,
            msg.chat.id.0,
            parent,
            is_bot_reaction,
            is_ranking,
            "FALSE"
        ];
        self.save_message.execute(params)?;
        Ok(())
    }

    // TODO zmiana nazwy
    fn get_inline_keyboard(
        &mut self,
        msg_custom_id: u64,
        expanded: bool,
    ) -> Result<Option<(InlineKeyboardMarkup, String)>, rusqlite::Error> {
        let reactions_data = self
            .get_reactions
            .query_map([msg_custom_id], |row| Ok((row.get(0)?, row.get(1)?)))?;
        let (reactions_authors, mut inline_keyboard_buttons): (Vec<_>, Vec<_>) = reactions_data
            .map(|reaction_data| {
                let (reaction_authors, reaction): (String, String) = reaction_data.unwrap();
                let reaction_text = if reaction == "+1" {
                    format!("+{}", reaction_authors.matches(',').count() + 1)
                } else if reaction == "-1" {
                    format!("-{}", reaction_authors.matches(',').count() + 1)
                } else {
                    reaction.clone()
                };
                (
                    reaction_authors,
                    InlineKeyboardButton::callback(reaction_text, reaction),
                )
            })
            .unzip();

        if inline_keyboard_buttons.is_empty() {
            return Ok(None);
        }

        let mut reaction_message_text = "\u{ad}\u{ad}".to_string();
        let mut show_hide_button_data = "show_reactions";
        let reaction_authors_text = reactions_authors.join("\n");
        if expanded {
            show_hide_button_data = "hide_reactions";
            reaction_message_text = reaction_authors_text;
        }

        inline_keyboard_buttons.push(InlineKeyboardButton::callback("ℹ️", show_hide_button_data));
        let inline_keyboard_markup = InlineKeyboardMarkup {
            inline_keyboard: inline_keyboard_buttons
                .chunks(4)
                .collect::<Vec<_>>()
                .into_iter()
                .map(|s| s.to_vec())
                .collect(),
        };
        Ok(Some((inline_keyboard_markup, reaction_message_text)))
    }
}

fn hash_string(s: String) -> u64 {
    let mut hash = 0xcbf29ce484222325;
    for c in s.chars() {
        hash ^= c as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash & 0xfffffffffffffff
}

fn get_custom_msg_id(chat_id: ChatId, msg_id: MessageId) -> u64 {
    hash_string(format!("{}:{}", chat_id.0, msg_id.0))
}

enum Command {
    Help,
    Top,
    Ranking,
    Anon,
}

fn get_command(text: &str) -> Option<Command> {
    if text.starts_with("/help ") || text.starts_with("/help@") {
        Some(Command::Help)
    } else if text.starts_with("/top ") || text.starts_with("top@") {
        Some(Command::Top)
    } else if text.starts_with("/ranking ") || text.starts_with("/ranking@") {
        Some(Command::Ranking)
    } else if text.starts_with("!a ") || text.starts_with("!anon ") {
        Some(Command::Anon)
    } else {
        None
    }
}

async fn handle_update(update: Update, bot: &mut BotWrapper<'_>) -> Result<(), BotError> {
    // TODO log
    match update.kind {
        UpdateKind::CallbackQuery(cq) => bot.handle_callback_query(cq).await,
        UpdateKind::Message(msg) => {
            bot.save_msg_to_db(&msg, false, false)?;
            println!("{msg:?}");
            if let Some(text) = msg.text() {
                match get_command(text) {
                    Some(cmd) => bot.handle_command(cmd, &msg, text).await?,
                    None => {
                        if let Some(parent_msg) = msg.reply_to_message() {
                            if let Some(reactions) = get_reactions_from(text) {
                                let author = msg.from().unwrap();
                                bot.delete_message(msg.chat.id, msg.id).await?;
                                bot.react_to(parent_msg.id, msg.chat.id, author, reactions)
                                    .await?;
                            }
                        }
                    }
                }
            }
            Ok(())
        }
        wtf => panic!("nie ma prawa do tego dojść {wtf:?}"),
    }
}

#[tokio::main]
async fn main() -> Result<(), BotError> {
    pretty_env_logger::init();
    log::info!("Starting reactions bot...");

    let connection = &Connection::open("db.sqlite")?;
    let bot = &mut BotWrapper::new(connection)?;
    let mut offset = 0;

    loop {
        let updates = bot // TODO zrobić webhooka
            .get_updates()
            .offset(offset)
            .allowed_updates([AllowedUpdate::Message, AllowedUpdate::CallbackQuery])
            .await?;

        for update in updates {
            offset = update.id + 1;
            handle_update(update, bot).await?;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn get_reactions_from(text: &str) -> Option<Vec<&str>> {
    let reaction_commands = vec!["!r ", "!react "];
    for reaction_command in reaction_commands {
        if let Some(reaction) = text.strip_prefix(reaction_command) {
            return Some(vec![reaction]);
        }
    }
    let emojis = emojito::find_emoji(text);
    if emojis.len() > 3 {
        return None;
    }
    let demojified_text = demoji(text);
    if !emojis.is_empty() && demojified_text.is_empty() {
        let reactions = emojis
            .into_iter()
            .map(|emoji| emoji.glyph)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        return Some(reactions);
    }
    let lennyface =
        std::str::from_utf8(b"( \xcd\xa1\xc2\xb0 \xcd\x9c\xca\x96 \xcd\xa1\xc2\xb0)").unwrap();
    let reacts = HashMap::from([
        ("xd", "xD"),
        ("rel", "rel"),
        ("nierel", "nierel"),
        ("rigcz", "RiGCz"),
        ("rak", "rak"),
        ("fakty", "fakty"),
        ("baza", "baza"),
        ("based", "baza"),
        ("lenny", lennyface),
        (lennyface, lennyface),
        ("+1", "+1"),
        ("-1", "-1"),
    ]);
    reacts
        .get(text.to_lowercase().as_str())
        .map(|&reaction| vec![reaction])
}
