pub mod post_run;
pub mod utils;

use include_dir::{Dir, include_dir};
use minijinja::Environment;
use std::sync::LazyLock;

pub use post_run::*;
pub use utils::*;

pub static TEMPLATES_DIR: LazyLock<Dir> = LazyLock::new(|| include_dir!("assets/templates"));

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
    environment
});
