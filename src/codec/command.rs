use super::error::CodecError;
use crate::SocketType;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::Display;

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Copy, Clone)]
pub enum ZmqCommandName {
    READY,
    PING,
    PONG,
}

impl ZmqCommandName {
    pub const fn as_str(&self) -> &'static str {
        match self {
            ZmqCommandName::READY => "READY",
            ZmqCommandName::PING => "PING",
            ZmqCommandName::PONG => "PONG",
        }
    }
}

impl Display for ZmqCommandName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone)]
pub struct ZmqCommand {
    pub name: ZmqCommandName,
    pub properties: HashMap<String, Bytes>,
    /// For PING commands: time-to-live in tenths of seconds
    pub ttl: Option<u16>,
    /// For PING/PONG commands: context data (0-16 bytes)
    pub context: Option<Bytes>,
}

impl ZmqCommand {
    pub fn ready(socket: SocketType) -> Self {
        let mut properties = HashMap::new();
        properties.insert("Socket-Type".into(), socket.as_str().into());
        Self {
            name: ZmqCommandName::READY,
            properties,
            ttl: None,
            context: None,
        }
    }

    /// Create a PING command with optional TTL and context
    ///
    /// # Arguments
    /// * `ttl` - Time-to-live in tenths of seconds (0-6553.5 seconds max), or None for 0
    /// * `context` - Optional context data (0-16 bytes)
    pub fn ping(ttl: Option<u16>, context: Option<Bytes>) -> Self {
        Self {
            name: ZmqCommandName::PING,
            properties: HashMap::new(),
            ttl,
            context,
        }
    }

    /// Create a PONG command with optional context
    ///
    /// # Arguments
    /// * `context` - Context data from the PING command (0-16 bytes)
    pub fn pong(context: Option<Bytes>) -> Self {
        Self {
            name: ZmqCommandName::PONG,
            properties: HashMap::new(),
            ttl: None,
            context,
        }
    }

    pub fn add_prop(&mut self, name: String, value: Bytes) -> &mut Self {
        self.properties.insert(name, value);
        self
    }

    pub fn add_properties(&mut self, map: HashMap<String, Bytes>) -> &mut Self {
        self.properties.extend(map);
        self
    }
}

impl TryFrom<Bytes> for ZmqCommand {
    type Error = CodecError;

    fn try_from(mut buf: Bytes) -> Result<Self, Self::Error> {
        let command_len = buf.get_u8() as usize;
        // command-name-char = ALPHA according to https://rfc.zeromq.org/spec:23/ZMTP/
        let command_name_buf = &buf[..command_len];
        let command = match command_name_buf {
            b"READY" => ZmqCommandName::READY,
            b"PING" => ZmqCommandName::PING,
            b"PONG" => ZmqCommandName::PONG,
            _ => return Err(CodecError::Command("Unknown command received")),
        };
        buf.advance(command_len);

        match command {
            ZmqCommandName::READY => {
                let mut properties = HashMap::new();
                while !buf.is_empty() {
                    // Collect command properties
                    let prop_len = buf.get_u8() as usize;
                    let property = match String::from_utf8(buf.split_to(prop_len).to_vec()) {
                        Ok(p) => p,
                        Err(_) => return Err(CodecError::Decode("Invalid property identifier")),
                    };

                    let prop_val_len = buf.get_u32() as usize;
                    let prop_value = buf.split_to(prop_val_len);
                    properties.insert(property, prop_value);
                }
                Ok(Self {
                    name: command,
                    properties,
                    ttl: None,
                    context: None,
                })
            }
            ZmqCommandName::PING => {
                if buf.len() < 2 {
                    return Err(CodecError::Decode("PING command requires at least 2 bytes for TTL"));
                }
                let ttl = buf.get_u16();

                // Remaining bytes are context (0-16 bytes)
                if buf.len() > 16 {
                    return Err(CodecError::Decode("PING context exceeds 16 bytes"));
                }
                let context = if buf.is_empty() {
                    None
                } else {
                    Some(buf.split_to(buf.len()))
                };

                Ok(Self {
                    name: command,
                    properties: HashMap::new(),
                    ttl: Some(ttl),
                    context,
                })
            }
            ZmqCommandName::PONG => {
                // PONG command contains only context (0-16 bytes)
                if buf.len() > 16 {
                    return Err(CodecError::Decode("PONG context exceeds 16 bytes"));
                }
                let context = if buf.is_empty() {
                    None
                } else {
                    Some(buf.split_to(buf.len()))
                };

                Ok(Self {
                    name: command,
                    properties: HashMap::new(),
                    ttl: None,
                    context,
                })
            }
        }
    }
}

impl From<ZmqCommand> for BytesMut {
    fn from(command: ZmqCommand) -> Self {
        let mut message_len = 0;

        let command_name = command.name.as_str();
        message_len += command_name.len() + 1;

        let start_message = |msg_len: usize| -> BytesMut {
            let long_message = msg_len > 255;

            let mut bytes = BytesMut::new();
            if long_message {
                bytes.reserve(msg_len + 9);
                bytes.put_u8(0x06);
                bytes.put_u64(msg_len as u64);
            } else {
                bytes.reserve(msg_len + 2);
                bytes.put_u8(0x04);
                bytes.put_u8(msg_len as u8);
            };
            bytes.put_u8(command_name.len() as u8);
            bytes.extend_from_slice(command_name.as_ref());
            bytes
        };

        match command.name {
            ZmqCommandName::READY => {
                for (prop, val) in command.properties.iter() {
                    message_len += prop.len() + 1;
                    message_len += val.len() + 4;
                }

                let mut bytes = start_message(message_len);

                for (prop, val) in command.properties.iter() {
                    bytes.put_u8(prop.len() as u8);
                    bytes.extend_from_slice(prop.as_ref());
                    bytes.put_u32(val.len() as u32);
                    bytes.extend_from_slice(val.as_ref());
                }
                bytes
            }
            ZmqCommandName::PING => {
                // PING command: command-name (1 byte len + name) + ttl (2 bytes) + context (0-16 bytes)
                let context_len = command.context.as_ref().map_or(0, |c| c.len());
                message_len += 2 + context_len;

                let mut bytes = start_message(message_len);

                bytes.put_u16(command.ttl.unwrap_or(0));
                if let Some(ctx) = command.context {
                    bytes.extend_from_slice(&ctx);
                }
                bytes
            }
            ZmqCommandName::PONG => {
                // PONG command: command-name (1 byte len + name) + context (0-16 bytes)
                let context_len = command.context.as_ref().map_or(0, |c| c.len());
                message_len += context_len;

                let mut bytes = start_message(message_len);

                if let Some(ctx) = command.context {
                    bytes.extend_from_slice(&ctx);
                }
                bytes
            }
        }
    }
}
