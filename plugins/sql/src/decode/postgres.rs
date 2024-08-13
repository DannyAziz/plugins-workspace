// Copyright 2019-2023 Tauri Programme within The Commons Conservancy
// SPDX-License-Identifier: Apache-2.0
// SPDX-License-Identifier: MIT

use serde_json::Value as JsonValue;
use sqlx::{postgres::PgTypeKind, postgres::PgValueRef, TypeInfo, Value, ValueRef};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};

use crate::Error;

use std::io::{Cursor, BufRead};
use byteorder::{BigEndian, ReadBytesExt};

#[derive(Debug, Clone)]
struct LexemeMeta {
    position: u16,
    weight: u8,
}

impl From<u16> for LexemeMeta {
    fn from(value: u16) -> Self {
        let weight = ((value >> 14) & 0b11) as u8;
        let position = value & 0x3fff;
        Self { weight, position }
    }
}

#[derive(Debug)]
struct Lexeme {
    word: String,
    positions: Vec<LexemeMeta>,
}

#[derive(Debug)]
struct TsVector {
    words: Vec<Lexeme>,
}

impl TsVector {
    fn try_from(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let mut reader = Cursor::new(bytes);
        let mut words = vec![];

        let num_lexemes = reader.read_u32::<BigEndian>()?;

        for _ in 0..num_lexemes {
            let mut word = vec![];
            reader.read_until(0, &mut word)?;
            let word = String::from_utf8(word)?.trim_end_matches('\0').to_string();

            let num_positions = reader.read_u16::<BigEndian>()?;
            let mut positions = Vec::with_capacity(num_positions as usize);

            for _ in 0..num_positions {
                let position = reader.read_u16::<BigEndian>()?;
                positions.push(LexemeMeta::from(position));
            }

            words.push(Lexeme { word, positions });
        }

        Ok(Self { words })
    }

    fn to_string(&self) -> String {
        self.words
            .iter()
            .map(|lexeme| {
                let positions = lexeme
                    .positions
                    .iter()
                    .map(|meta| {
                        let weight_char = match meta.weight {
                            3 => Some('A'),
                            2 => Some('B'),
                            1 => Some('C'),
                            _ => None,
                        };
                        format!("{}{}", meta.position, weight_char.unwrap_or_default())
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                
                if positions.is_empty() {
                    format!("'{}'", lexeme.word)
                } else {
                    format!("'{}':{}", lexeme.word, positions)
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    }
}

pub(crate) fn to_json(v: PgValueRef) -> Result<JsonValue, Error> {
    if v.is_null() {
        return Ok(JsonValue::Null);
    }

    let res = match v.type_info().name() {
        "CHAR" | "VARCHAR" | "TEXT" | "NAME" => {
            if let Ok(v) = ValueRef::to_owned(&v).try_decode() {
                JsonValue::String(v)
            } else {
                JsonValue::Null
            }
        }
        "FLOAT4" => {
            if let Ok(v) = ValueRef::to_owned(&v).try_decode::<f32>() {
                JsonValue::from(v)
            } else {
                JsonValue::Null
            }
        }
        "FLOAT8" => {
            if let Ok(v) = ValueRef::to_owned(&v).try_decode::<f64>() {
                JsonValue::from(v)
            } else {
                JsonValue::Null
            }
        }
        "INT2" => {
            if let Ok(v) = ValueRef::to_owned(&v).try_decode::<i16>() {
                JsonValue::Number(v.into())
            } else {
                JsonValue::Null
            }
        }
        "INT4" => {
            if let Ok(v) = ValueRef::to_owned(&v).try_decode::<i32>() {
                JsonValue::Number(v.into())
            } else {
                JsonValue::Null
            }
        }
        "INT8" => {
            if let Ok(v) = ValueRef::to_owned(&v).try_decode::<i64>() {
                JsonValue::Number(v.into())
            } else {
                JsonValue::Null
            }
        }
        "BOOL" => {
            if let Ok(v) = ValueRef::to_owned(&v).try_decode() {
                JsonValue::Bool(v)
            } else {
                JsonValue::Null
            }
        }
        "DATE" => {
            if let Ok(v) = ValueRef::to_owned(&v).try_decode::<Date>() {
                JsonValue::String(v.to_string())
            } else {
                JsonValue::Null
            }
        }
        "TIME" => {
            if let Ok(v) = ValueRef::to_owned(&v).try_decode::<Time>() {
                JsonValue::String(v.to_string())
            } else {
                JsonValue::Null
            }
        }
        "TIMESTAMP" => {
            if let Ok(v) = ValueRef::to_owned(&v).try_decode::<PrimitiveDateTime>() {
                JsonValue::String(v.to_string())
            } else {
                JsonValue::Null
            }
        }
        "TIMESTAMPTZ" => {
            if let Ok(v) = ValueRef::to_owned(&v).try_decode::<OffsetDateTime>() {
                JsonValue::String(v.to_string())
            } else {
                JsonValue::Null
            }
        }
        "JSON" | "JSONB" => ValueRef::to_owned(&v).try_decode().unwrap_or_default(),
        "BYTEA" => {
            if let Ok(v) = ValueRef::to_owned(&v).try_decode::<Vec<u8>>() {
                JsonValue::Array(v.into_iter().map(|n| JsonValue::Number(n.into())).collect())
            } else {
                JsonValue::Null
            }
        }
        "VOID" => JsonValue::Null,
        "tsvector" => {
            if let Ok(ts_vector) = TsVector::try_from(v.as_bytes().map_err(|e| Error::from(sqlx::Error::Decode(e.into())))?) {
                println!("ts_vector: {}", ts_vector.to_string());
                JsonValue::String(ts_vector.to_string())
            } else {
                JsonValue::Null
            }
        }
        _ => {
            match *v.type_info().kind() {
                PgTypeKind::Enum(ref variants) => {
                    let raw_value = match v.as_bytes() {
                        Ok(bytes) => bytes,
                        Err(e) => return Err(Error::from(sqlx::Error::Decode(e.into()))),
                    };
                    let raw_str = String::from_utf8_lossy(raw_value);
                    if variants.contains(&raw_str.to_string()) {
                        JsonValue::String(raw_str.to_string())
                    } else {
                        JsonValue::Null
                    }
                }
                _ => {
                    let raw_value = match v.as_bytes() {
                        Ok(bytes) => bytes,
                        Err(_) => return Err(Error::UnsupportedDatatype(v.type_info().name().to_string())),
                    };
                    println!("Raw value: {:?}", raw_value);
                    let raw_str = String::from_utf8_lossy(raw_value);
                    println!("Raw value: {}", raw_str);
                    if let Ok(v) = ValueRef::to_owned(&v).try_decode::<String>() {
                        JsonValue::String(v)
                    } else {
                        println!("unsupported datatype: {:?}", v.type_info());
                        println!("unsupported datatype name: {}", v.type_info().name());
                        println!("unsupported datatype kind: {:?}", v.type_info().kind());
                        JsonValue::Null
                    }
                }
            }
        }
    };

    Ok(res)
}