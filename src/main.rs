
use anyhow::Result;
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta};
use serde::{Deserialize, Serialize};
use url::Url;

mod find;

#[derive(Debug, Parser)]
struct Args {
    #[arg(short, long)]
    verbose: bool,
    #[arg(long, default_value = "128")]
    concurrency: usize,
    #[command(subcommand)]
    cmd: IOAction,
}

#[derive(Debug, Subcommand)]
enum IOAction {
    /// List objects recursively, and filter them by various criteria. Can be chained.
    ///
    /// Example: `obvious3 find -r /path -b '.*\.parquet' | obvious3 find --not --after 3`
    Find(find::Find),
}

impl IOAction {
    async fn run(&self, global_args: &Args) -> Result<()> {
        match self {
            IOAction::Find(f) => f.run(global_args).await,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    args.cmd.run(&args).await?;
    Ok(())
}

/// The metadata that describes an object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectExport {
    /// The full path to the object
    pub location: String,
    /// The last modified time
    pub last_modified: DateTime<Utc>,
    /// The size in bytes of the object
    pub size: usize,
    /// The unique identifier for the object
    ///
    /// <https://datatracker.ietf.org/doc/html/rfc9110#name-etag>
    pub e_tag: Option<String>,
    /// A version indicator for this object
    pub version: Option<String>,
}

impl From<ObjectMeta> for ObjectExport {
    fn from(meta: ObjectMeta) -> Self {
        Self {
            location: meta.location.to_string(),
            last_modified: meta.last_modified,
            size: meta.size,
            e_tag: meta.e_tag,
            version: meta.version,
        }
    }
}

impl From<ObjectExport> for ObjectMeta {
    fn from(export: ObjectExport) -> Self {
        Self {
            location: ObjectStorePath::from(export.location),
            last_modified: export.last_modified,
            size: export.size,
            e_tag: export.e_tag,
            version: export.version,
        }
    }
}

/// A header line for each object listing,
/// which includes the object store they refer to, along with some metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "file_type")]
pub enum Preamble {
    /// Protocol version 0
    Obvious3_0 {
        /// The object store that the objects are from
        root: Url,
    },
}
