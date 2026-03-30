use std::collections::HashMap;
use std::fs;
use std::path::Path;

use chrono::Local;
use polars::prelude::*;
use serde_json::{Map, Value, json};

use crate::{
    JINJA_ENV, UsageReportError, add_daily_duration, add_job_duration_cols, add_wait_time_cols,
    generic_report,
};

fn anyvalue_to_f64(v: &AnyValue) -> f64 {
    match v {
        AnyValue::Null => 0.0,
        AnyValue::Int8(n) => *n as f64,
        AnyValue::Int16(n) => *n as f64,
        AnyValue::Int32(n) => *n as f64,
        AnyValue::Int64(n) => *n as f64,
        AnyValue::UInt8(n) => *n as f64,
        AnyValue::UInt16(n) => *n as f64,
        AnyValue::UInt32(n) => *n as f64,
        AnyValue::UInt64(n) => *n as f64,
        AnyValue::Float32(n) => *n as f64,
        AnyValue::Float64(n) => *n,
        _ => 0.0,
    }
}

fn dataframe_row_to_map(
    df: &DataFrame,
    row_idx: usize,
    exclude: &str,
) -> serde_json::Map<String, Value> {
    let mut map = serde_json::Map::new();
    for col_name in df.get_column_names() {
        if col_name == exclude {
            continue;
        }
        let col = df.column(col_name).unwrap();
        let val = col.get(row_idx).unwrap();
        map.insert(col_name.to_string(), json!(anyvalue_to_f64(&val)));
    }
    map
}

pub fn compute_daily_metrics(
    lf: LazyFrame,
    date: &str,
    cluster_capacity: HashMap<String, i64>,
) -> Result<Value, UsageReportError> {
    let mut lf = lf;

    lf = generic_report(lf)?;

    lf = add_daily_duration(lf, date);
    lf = add_wait_time_cols(lf);
    lf = add_job_duration_cols(lf);

    let jobs_aggregations: Vec<Expr> = vec![
        col("TotalCPU_seconds").sum().alias("CPU.Secondes"),
        (col("TotalCPU_seconds").sum().cast(DataType::Float64) / lit(3600.0)).alias("CPU.Heures"),
        ((col("TotalCPU_seconds").sum().cast(DataType::Float64)
            / lit(*cluster_capacity.get("cpu_secondes").unwrap_or(&1) as f64))
            * lit(100.0))
        .alias("Pourcentage d'utilisation CPU"),
        (((col("MaxRSS") / lit(2i64.pow(30)) * col("ElapsedRaw").cast(DataType::Float64)).sum())
            .alias("GB.Secondes")),
        (((col("MaxRSS") / lit(2i64.pow(30)) * col("ElapsedRaw").cast(DataType::Float64)).sum()
            / lit(*cluster_capacity.get("gb_secondes").unwrap_or(&1) as f64))
            * lit(100.0))
        .alias("Taux d'occupation de la RAM"),
        col("wait_time_seconds")
            .mean()
            .alias("Temps d'attente moyen en queue (secondes)"),
        col("wait_time_seconds")
            .median()
            .alias("Temps d'attente médian en queue (secondes)"),
        col("wait_time_seconds")
            .min()
            .alias("Temps d'attente minimum en queue (secondes)"),
        col("wait_time_seconds")
            .max()
            .alias("Temps d'attente maximum en queue (secondes)"),
        col("JobID")
            .filter(
                col("Submit")
                    .str()
                    .to_datetime(None, None, StrptimeOptions::default(), lit("raise"))
                    .dt()
                    .date()
                    .eq(lit(date).str().to_date(StrptimeOptions {
                        format: Some(PlSmallStr::from_str("%Y-%m-%d")),
                        ..Default::default()
                    })),
            )
            .count()
            .alias("Jobs soumis"),
        col("JobID")
            .filter(
                col("Start")
                    .str()
                    .to_datetime(None, None, StrptimeOptions::default(), lit("raise"))
                    .dt()
                    .date()
                    .eq(lit(date).str().to_date(StrptimeOptions {
                        format: Some(PlSmallStr::from_str("%Y-%m-%d")),
                        ..Default::default()
                    })),
            )
            .count()
            .alias("Jobs démarrés"),
        col("JobID")
            .filter(
                col("End")
                    .str()
                    .to_datetime(None, None, StrptimeOptions::default(), lit("raise"))
                    .dt()
                    .date()
                    .eq(lit(date).str().to_date(StrptimeOptions {
                        format: Some(PlSmallStr::from_str("%Y-%m-%d")),
                        ..Default::default()
                    })),
            )
            .count()
            .alias("Jobs terminés"),
        col("JobID")
            .filter(col("State").eq(lit("FAILED")))
            .count()
            .alias("Jobs échoués"),
        col("JobID")
            .filter(col("State").eq(lit("COMPLETED")))
            .count()
            .alias("Jobs terminés avec succès"),
        col("JobID")
            .filter(col("State").eq(lit("PREEMPTED")))
            .count()
            .alias("Jobs préemptés"),
        col("job_duration_seconds")
            .mean()
            .alias("Durée moyenne d'exécution (secondes)"),
        col("job_duration_seconds")
            .median()
            .alias("Durée médiane d'exécution (secondes)"),
        col("job_duration_seconds")
            .min()
            .alias("Durée minimum d'exécution (secondes)"),
        col("job_duration_seconds")
            .max()
            .alias("Durée maximum d'exécution (secondes)"),
    ];

    let metric_names: Vec<&str> = vec![
        "CPU.Secondes",
        "CPU.Heures",
        "Pourcentage d'utilisation CPU",
        "GB.Secondes",
        "Taux d'occupation de la RAM",
        "Temps d'attente moyen en queue (secondes)",
        "Temps d'attente médian en queue (secondes)",
        "Temps d'attente minimum en queue (secondes)",
        "Temps d'attente maximum en queue (secondes)",
        "Jobs soumis",
        "Jobs démarrés",
        "Jobs terminés",
        "Jobs terminés avec succès",
        "Jobs échoués",
        "Jobs préemptés",
        "Durée moyenne d'exécution (secondes)",
        "Durée médiane d'exécution (secondes)",
        "Durée minimum d'exécution (secondes)",
        "Durée maximum d'exécution (secondes)",
    ];

    let lf_qos_grouped = lf
        .clone()
        .group_by([col("QOS")])
        .agg(jobs_aggregations.clone())
        .select(
            ["QOS".to_string()]
                .into_iter()
                .chain(metric_names.clone().into_iter().map(|s| s.to_string()))
                .map(|colname| col(colname))
                .collect::<Vec<_>>(),
        );

    let lf_global = lf
        .clone()
        .group_by([col("date")])
        .agg(jobs_aggregations.clone())
        .select(
            ["date".to_string()]
                .into_iter()
                .chain(metric_names.clone().into_iter().map(|s| s.to_string()))
                .map(|colname| col(colname))
                .collect::<Vec<_>>(),
        );

    let qos_df = lf_qos_grouped.collect()?;
    let global_df = lf_global.collect()?;

    let mut result = serde_json::Map::new();

    for row_idx in 0..qos_df.height() {
        let qos_name = qos_df
            .column("QOS")?
            .str()?
            .get(row_idx)
            .unwrap_or_default()
            .to_string();
        let row_map = dataframe_row_to_map(&qos_df, row_idx, "QOS");
        result.insert(qos_name, Value::Object(row_map));
    }

    let mut global_map = serde_json::Map::new();
    for row_idx in 0..global_df.height() {
        global_map = dataframe_row_to_map(&global_df, row_idx, "date");
    }
    result.insert("global".to_string(), Value::Object(global_map));

    Ok(Value::Object(result))
}

pub fn generate_daily_report<P: AsRef<Path>>(
    date: &str,
    database: P,
    html_output: P,
    json_output: P,
    cluster_capacity: Option<HashMap<String, i64>>,
) -> Result<(), UsageReportError> {
    let database = database.as_ref();
    let expected_file = database.join(format!("{}.parquet", date));

    if !expected_file.is_file() {
        eprintln!("Impossible de trouver {}", expected_file.display());
        std::process::exit(1);
    }

    let lf = LazyFrame::scan_parquet(
        PlRefPath::try_from_path(&expected_file)?,
        Default::default(),
    )?;

    let capacity = cluster_capacity.unwrap_or_else(|| {
        let mut map = HashMap::new();
        map.insert("cpu_secondes".to_string(), 96 * 86400 * 4 + 40 * 86400);
        map.insert("gb_secondes".to_string(), 2183 * 86400);
        map
    });

    let metrics = compute_daily_metrics(lf, date, capacity)?;

    let json_content = serde_json::to_string_pretty(&metrics)?;
    fs::write(&json_output, &json_content)?;

    let html_content = generate_daily_html_report(&metrics, Some(date))?;
    fs::write(&html_output, &html_content)?;

    Ok(())
}

pub fn generate_daily_html_report(
    metrics: &Value,
    date: Option<&str>,
) -> Result<String, UsageReportError> {
    let template = JINJA_ENV.get_template("daily_efficiency.html.j2")?;

    let global_metrics = metrics.get("global").cloned().unwrap_or(json!({}));

    let qos_metrics = metrics
        .as_object()
        .map(|obj| {
            Value::Object(
                obj.iter()
                    .filter(|(k, _)| *k != "global")
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<Map<String, Value>>(),
            )
        })
        .unwrap_or(json!({}));

    let html = template.render(json!({
        "date": date.unwrap_or("N/A"),
        "global_metrics": global_metrics,
        "qos_metrics": qos_metrics,
        "generation_time": Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
    }))?;

    Ok(html)
}
