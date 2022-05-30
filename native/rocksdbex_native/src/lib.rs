extern crate lazy_static;
extern crate rocksdb;
extern crate rustler;

// use rocksdb::DBIterator;
use rocksdb::{DBCompactionStyle, Direction, IteratorMode, Options, WriteBatch, DB};
use rustler::resource::ResourceArc;
// use rustler::schedule::SchedulerFlags;
use rustler::types::binary::{Binary, OwnedBinary};
use rustler::types::map::MapIterator;
use rustler::{Encoder, Env, Error, NifResult, Term};

use std::sync::RwLock;

mod atoms {
    rustler::atoms! {

        ok,
        err,
        not_found,
        vn1

    }
}

struct DbResource {
    db: RwLock<DB>,
    path: String,
}

fn on_load<'a>(env: Env<'a>, _load_info: Term<'a>) -> bool {
    rustler::resource!(DbResource, env);
    // resource_struct_init!(IteratorResource, env);
    true
}

#[rustler::nif(schedule = "DirtyIo")]
fn open_default<'a>(env: Env<'a>, path: std::string::String) -> NifResult<Term<'a>> {
    match DB::open_default(path.clone()) {
        Ok(db) => {
            let resource = ResourceArc::new(DbResource {
                db: RwLock::new(db),
                path: path.clone(),
            });
            Ok((atoms::ok(), resource.encode(env)).encode(env))
        }
        Err(e) => Ok((atoms::err(), e.to_string()).encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn open<'a>(env: Env<'a>, path: std::string::String, iter: MapIterator) -> NifResult<Term<'a>> {
    let mut opts = Options::default();
    for (key, value) in iter {
        let param = key.atom_to_string()?;
        match param.as_str() {
            "create_if_missing" => {
                if value.atom_to_string()?.as_str() == "true" {
                    opts.create_if_missing(true);
                }
            }
            "create_missing_column_families" => {
                if value.atom_to_string()?.as_str() == "true" {
                    opts.create_missing_column_families(true);
                }
            }
            "set_max_open_files" => {
                let limit: i32 = value.decode()?;
                opts.set_max_open_files(limit);
            }
            "set_use_fsync" => {
                if value.atom_to_string()?.as_str() == "true" {
                    opts.set_use_fsync(true);
                }
            }
            "set_bytes_per_sync" => {
                let limit: u64 = value.decode()?;
                opts.set_bytes_per_sync(limit);
            }
            "optimize_for_point_lookup" => {
                let limit: u64 = value.decode()?;
                opts.optimize_for_point_lookup(limit);
            }
            "set_table_cache_num_shard_bits" => {
                let limit: i32 = value.decode()?;
                opts.set_table_cache_num_shard_bits(limit);
            }
            "set_max_write_buffer_number" => {
                let limit: i32 = value.decode()?;
                opts.set_max_write_buffer_number(limit);
            }
            "set_write_buffer_size" => {
                let limit: usize = value.decode()?;
                opts.set_write_buffer_size(limit);
            }
            "set_target_file_size_base" => {
                let limit: u64 = value.decode()?;
                opts.set_target_file_size_base(limit);
            }
            "set_min_write_buffer_number_to_merge" => {
                let limit: i32 = value.decode()?;
                opts.set_min_write_buffer_number_to_merge(limit);
            }
            "set_level_zero_stop_writes_trigger" => {
                let limit: i32 = value.decode()?;
                opts.set_level_zero_stop_writes_trigger(limit);
            }
            "set_level_zero_slowdown_writes_trigger" => {
                let limit: i32 = value.decode()?;
                opts.set_level_zero_slowdown_writes_trigger(limit);
            }
            // "set_max_background_compactions" => {
            //     let limit: i32 = value.decode()?;
            //     opts.set_max_background_compactions(limit);
            // }
            // "set_max_background_flushes" => {
            //     let limit: i32 = value.decode()?;
            //     opts.set_max_background_flushes(limit);
            // }
            "set_disable_auto_compactions" => {
                if value.atom_to_string()?.as_str() == "true" {
                    opts.set_disable_auto_compactions(true);
                }
            }
            "set_compaction_style" => {
                let style = value.atom_to_string()?;
                if style == "level" {
                    opts.set_compaction_style(DBCompactionStyle::Level);
                } else if style == "universal" {
                    opts.set_compaction_style(DBCompactionStyle::Universal);
                } else if style == "fifo" {
                    opts.set_compaction_style(DBCompactionStyle::Fifo);
                }
            }
            "prefix_length" => {
                let limit: usize = value.decode()?;
                let prefix_extractor = rocksdb::SliceTransform::create_fixed_prefix(limit);
                opts.set_prefix_extractor(prefix_extractor);
            }
            _ => {}
        }
    }

    match DB::open(&opts, path.clone()) {
        Ok(db) => {
            let resource = ResourceArc::new(DbResource {
                db: RwLock::new(db),
                path: path.clone(),
            });
            Ok((atoms::ok(), resource.encode(env)).encode(env))
        }
        Err(e) => Ok((atoms::err(), e.to_string()).encode(env)),
    }
}

#[rustler::nif]
fn put<'a>(
    env: Env<'a>,
    resource: ResourceArc<DbResource>,
    key: Binary,
    value: Binary,
) -> NifResult<Term<'a>> {
    let db = resource.db.write().unwrap();
    match db.put(key.as_slice(), value.as_slice()) {
        Ok(_) => Ok((atoms::ok()).encode(env)),
        Err(e) => Ok((atoms::err(), e.to_string()).encode(env)),
    }
}

#[rustler::nif]
fn delete<'a>(env: Env<'a>, resource: ResourceArc<DbResource>, key: Binary) -> NifResult<Term<'a>> {
    let db = resource.db.write().unwrap();
    match db.delete(key.as_slice()) {
        Ok(_) => Ok((atoms::ok()).encode(env)),
        Err(e) => Ok((atoms::err(), e.to_string()).encode(env)),
    }
}

#[rustler::nif]
fn get<'a>(env: Env<'a>, resource: ResourceArc<DbResource>, key: Binary) -> NifResult<Term<'a>> {
    let db = resource.db.read().unwrap();
    match db.get(key.as_slice()) {
        Ok(Some(v)) => {
            let mut value = OwnedBinary::new(v[..].len()).unwrap();
            value.clone_from_slice(&v[..]);
            Ok((atoms::ok(), value.release(env)).encode(env))
        }
        Ok(None) => Ok((atoms::not_found()).encode(env)),
        Err(e) => Ok((atoms::err(), e.to_string()).encode(env)),
    }
}

rustler::init!(
    "Elixir.RocksdbEx.Native",
    [open, open_default, put, get, delete],
    load = on_load
);
