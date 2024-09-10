use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path as ObjectStorePath;
use object_store::{DynObjectStore, ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncBufReadExt;
use url::Url;

use crate::{Args, ObjectExport};

#[derive(Debug, Parser)]
pub struct Find {
    /// The paths to recurse from, if not specified, individual object metadata will be read from stdin.
    #[arg(short, long)]
    root: Option<String>,
    /// Objects full paths must match this regex.
    ///
    /// Case sensitive by default, `(?i)foo` would match `FOO`, `Foo`, `foo`, etc.
    ///
    /// See https://docs.rs/regex/1.5.4/regex/#syntax for full regex syntax
    #[arg(short, long)]
    path_match: Option<String>,
    /// Object's basenames must match this regex. Same syntax as `path_match`.
    #[arg(short, long)]
    basename_match: Option<String>,
    /// Invert the meaning of both regexes: only show objects that don't match
    #[arg(long("not"))]
    invert: bool,
    /// Objects should be at least this size in bytes
    #[arg(long)]
    min_size: Option<usize>,
    /// Objects should be at most this size in bytes
    #[arg(long)]
    max_size: Option<usize>,
    /// Objects should have been modified after this time in RFC3339 format
    #[arg(long)]
    after_absolute: Option<DateTime<Utc>>,
    /// Objects should have been modified before this time in RFC3339 format
    #[arg(long)]
    before_absolute: Option<DateTime<Utc>>,
    /// Objects should have been modified after this many seconds ago
    #[arg(long)]
    after: Option<i64>,
    /// Objects should have been modified before this many seconds ago
    #[arg(long)]
    before: Option<i64>,
}

impl Find {
    fn base_url(&self) -> Result<Option<Url>> {
        let root = match &self.root {
            Some(r) => r,
            None => return Ok(None),
        };
        // If it parses, it's a URL
        if let Ok(u) = Url::parse(root) {
            return Ok(Some(u));
        }
        // If it doesn't parse, try interpreting it as a local path
        let path = std::path::PathBuf::from(root).canonicalize()?;
        Ok(Some(
            Url::from_file_path(path).map_err(|_| anyhow::anyhow!("Invalid path"))?,
        ))
    }

    /// Produce an asynchronous stream of ObjectMeta objects read as NDJSON from stdin
    fn read_stdin() -> impl futures::Stream<Item = Result<ObjectMeta>> {
        let stdin = tokio::io::stdin();
        let reader = tokio::io::BufReader::new(stdin).lines();
        let stream = tokio_stream::wrappers::LinesStream::new(reader);
        let stream = stream
            .map_err(anyhow::Error::from)
            .and_then(
                |line| async move { anyhow::Ok(serde_json::from_str::<ObjectExport>(&line)?) },
            )
            .map_ok(ObjectMeta::from);
        stream
    }

    pub async fn run(&self, global_args: &Args) -> Result<()> {
        // These ref's mean that `async move` later doesn't take ownership of the fields
        let ref path_regex = self
            .path_match
            .as_ref()
            .map(|s| regex::Regex::new(s))
            .transpose()?;
        let ref base_regex = self
            .basename_match
            .as_ref()
            .map(|s| regex::Regex::new(s))
            .transpose()?;

        let print_matches = |meta: ObjectMeta| {
            let mut valid = true;
            // Try to match the path
            if let Some(reg) = &path_regex {
                valid &= reg.is_match(&meta.location.to_string());
            }
            // Try to match the basename
            if let Some(reg) = &base_regex {
                valid &= reg.is_match(&meta.location.filename().unwrap_or_default());
            }

            // Try to match the size
            if let Some(min_size) = self.min_size {
                valid &= meta.size >= min_size;
            }
            if let Some(max_size) = self.max_size {
                valid &= meta.size <= max_size;
            }

            // Try to match the absolute last modified time
            if let Some(after_absolute) = self.after_absolute {
                valid &= meta.last_modified >= after_absolute;
            }
            if let Some(before_absolute) = self.before_absolute {
                valid &= meta.last_modified <= before_absolute;
            }

            // Try to match the relative last modified time
            if let Some(after) = self.after {
                valid &= meta.last_modified >= (Utc::now() - chrono::Duration::seconds(after));
            }
            if let Some(before) = self.before {
                valid &= meta.last_modified <= Utc::now() - chrono::Duration::seconds(before);
            }
            if valid == !self.invert {
                println!("{}", serde_json::to_string(&ObjectExport::from(meta))?);
            }
            Ok(())
        };

        match self.base_url()? {
            Some(url) => {
                let (store, path) = object_store::parse_url(&url)?;
                let store = Arc::new(store);
                let gen = store.list(Some(&path)).map_err(anyhow::Error::from);
                gen.try_for_each_concurrent(global_args.concurrency, |meta| async move {
                    print_matches(meta)
                })
                .await?;
            }
            None => {
                Self::read_stdin()
                    .try_for_each_concurrent(global_args.concurrency, |meta| async move {
                        print_matches(meta)
                    })
                    .await?;
            }
        };
        Ok(())
    }
}
