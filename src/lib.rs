pub mod error;
pub mod post_run;
pub mod usage_report;
pub mod utils;

pub use error::*;
pub use post_run::*;
pub use usage_report::*;
pub use utils::*;

use include_dir::{Dir, include_dir};
use minijinja::Environment;
use std::sync::LazyLock;

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
    environment.add_filter("format_header", format_header);
    environment.add_filter("get_color", get_color);
    environment
});

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_templates() {
        let template = JINJA_ENV
            .get_template("snakemake_report_template.html.j2")
            .unwrap();
        let output = template.render(json! (
        {
            "mem_box_plot": "<div>This would be a box plot (mem_box_plot)</div>",
            "cpu_box_plot": "<div>This would be a box plot (cpu_box_plot)</div>",
            "runtime_box_plot": "<div>This would be a box plot (runtime_box_plot)</div>",
            "relative_mem_box_plot": "<div>This would be a box plot (relative_mem_box_plot)</div>",
            "relative_runtime_box_plot": "<div>This would be a box plot (relative_runtime_box_plot)</div>",
            "efficiency_table_mem": json!({"memory_efficiency_col1":[10,20,80],"memory_efficiency_col1":[10,20,80]}),
            "efficiency_table_cpu": json!({"cpu_efficiency_col1":[100,50,150],"cpu_efficiency_col1":[100,50,150]}),
            "efficiency_table_runtime": json!({"runtime_efficiency_col1":[100,50,150],"runtime_efficiency_col1":[100,50,150]}),
            "efficiency_table_relative_mem": json!({"relative_mem_efficiency_col1":[100,50,150],"relative_mem_efficiency_col1":[100,50,150]}),
            "efficiency_table_relative_runtime": json!({"relative_runtime_efficiency_col1":[100,50,150],"relative_runtime_efficiency_col1":[100,50,150]}),
        })).unwrap();

        assert_eq!(
            output.as_bytes(),
            include_bytes!("../assets/tests/expected_template.html")
        );
        eprintln!("{}", output)
    }
}
