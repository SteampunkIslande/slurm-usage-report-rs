pub mod post_run;
pub mod usage_report;
pub mod utils;

pub use post_run::*;
pub use usage_report::*;
pub use utils::*;

use include_dir::{Dir, include_dir};
use minijinja::Environment;
use std::sync::LazyLock;

pub static TEMPLATES_DIR: LazyLock<Dir> = LazyLock::new(|| include_dir!("assets/templates"));

fn format_header(s: &str) -> String {
    s.replace("_", "\n")
}

pub static JINJA_ENV: LazyLock<Environment> = LazyLock::new(|| {
    let mut environment: Environment = Environment::new();
    environment.set_loader(move |name| {
        if let Some(file) = TEMPLATES_DIR.get_file(name) {
            if let Some(content) = file.contents_utf8() {
                Ok(Some(content.to_string()))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    });
    environment.add_filter("format_header", format_header);
    environment
});
