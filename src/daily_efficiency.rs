use std::fs;
use std::path::Path;
use std::{collections::HashMap, str::FromStr};

use base64::Engine;
use charming::datatype::{DataPoint, DataPointItem};
use chrono::{Local, NaiveDate, Utc};
use plotly::{Layout, Scatter};
use polars::prelude::*;
use serde_json::{Map, Value, json};

use charming::component::{Calendar, VisualMap};
use charming::element::CoordinateSystem;
use charming::series::Heatmap;
use charming::{Chart, ImageRenderer};

use crate::{
    JINJA_ENV, UsageReportError, add_daily_duration, add_job_duration_cols, add_wait_time_cols,
    date_conversion_options, datetime_conversion_options, generic_report,
};

fn dataframe_row_to_map(
    df: &DataFrame,
    row_idx: usize,
    exclude: &str,
) -> Result<serde_json::Map<String, Value>, UsageReportError> {
    let mut map = serde_json::Map::new();
    for col_name in df.get_column_names() {
        if col_name == exclude {
            continue;
        }
        let col = df.column(col_name).unwrap();
        let val = col.get(row_idx).unwrap();
        map.insert(col_name.to_string(), Value::from_str(&val.to_string())?);
    }
    Ok(map)
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
        ((col("MaxRSS").cast(DataType::Float64) / lit(2f64.powi(30))
            * col("ElapsedRaw").cast(DataType::Float64))
        .sum())
        .alias("GB.Secondes"),
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
                    .to_datetime(None, None, datetime_conversion_options(), lit("raise"))
                    .dt()
                    .date()
                    .eq(lit(date).str().to_date(date_conversion_options())),
            )
            .count()
            .alias("Jobs soumis"),
        col("JobID")
            .filter(
                col("Start")
                    .str()
                    .to_datetime(None, None, datetime_conversion_options(), lit("raise"))
                    .dt()
                    .date()
                    .eq(lit(date).str().to_date(date_conversion_options())),
            )
            .count()
            .alias("Jobs démarrés"),
        col("JobID")
            .filter(
                col("End")
                    .str()
                    .to_datetime(None, None, datetime_conversion_options(), lit("raise"))
                    .dt()
                    .date()
                    .eq(lit(date).str().to_date(date_conversion_options())),
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
        let row_map = dataframe_row_to_map(&qos_df, row_idx, "QOS")?;
        result.insert(qos_name, Value::Object(row_map));
    }

    let mut global_map = serde_json::Map::new();
    if global_df.height() != 1 {
        eprintln!("Something is wrong");
    }
    for row_idx in 0..global_df.height() {
        global_map = dataframe_row_to_map(&global_df, row_idx, "date")?;
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

const METRICS_CONFIG: [(&'static str, &'static str); 2] = [
    (
        "Taux d'occupation de la RAM",
        "Taux d'occupation de la RAM (% de la capacité totale en GB.secondes)",
    ),
    (
        "Pourcentage d'utilisation CPU",
        "Taux d'utilisation des CPUs (% de la capacité totale en CPU.secondes)",
    ),
];

fn generate_calendar_heatmap(dates: &[String], values: &[f64]) -> Result<String, UsageReportError> {
    let vals = Vec::new();
    let chart = Chart::new()
        .visual_map(VisualMap::new().show(false).min(0).max(10000))
        .calendar(Calendar::new().range((
            dates.first().unwrap().as_str(),
            dates.last().unwrap().as_str(),
        )))
        .series(
            Heatmap::new()
                .coordinate_system(CoordinateSystem::Calendar)
                .data(dates.iter().zip(values).fold(vals, |mut v, (date, val)| {
                    v.push(vec![DataPoint::Item(
                        DataPointItem::new(*val).name(date.to_string()),
                    )]);
                    v
                })),
        );

    let mut renderer = ImageRenderer::new(1000, 800);
    let buffer = renderer.render_format(charming::ImageFormat::Png, &chart)?;

    let base64_str = base64::engine::general_purpose::STANDARD.encode(&buffer);
    Ok(format!(
        "<img src=\"data:image/png;base64,{}\" />",
        base64_str
    ))
}

fn generate_line_plot(
    dates: &[String],
    values: &[f64],
    title: &str,
) -> Result<String, UsageReportError> {
    if dates.is_empty() || values.is_empty() {
        return Ok(String::new());
    }

    let trace = Scatter::new(dates.to_vec(), values.to_vec())
        .mode(plotly::common::Mode::Lines)
        .name("");

    let layout = Layout::new()
        .title(plotly::common::Title::from(""))
        .x_axis(plotly::layout::Axis::new().title(""))
        .y_axis(plotly::layout::Axis::new().title(title));

    let mut plot = plotly::Plot::new();
    plot.add_trace(trace);
    plot.set_layout(layout);

    Ok(plot.to_html())
}

fn load_reports_data(
    database: &Path,
    from_date: &str,
    to_date: &str,
) -> Result<(Vec<String>, Vec<Map<String, Value>>), UsageReportError> {
    let start = NaiveDate::parse_from_str(from_date, "%Y-%m-%d")?;
    let end = NaiveDate::parse_from_str(to_date, "%Y-%m-%d")?;

    let mut dates = Vec::new();
    let mut reports_data = Vec::new();
    let mut current = start;

    while current <= end {
        let date_str = current.format("%Y-%m-%d").to_string();
        let json_file = database.join(format!("{}.json", date_str));

        if json_file.is_file() {
            let content = std::fs::read_to_string(&json_file)?;
            let data: Value = serde_json::from_str(&content)?;
            if let Some(global) = data.get("global") {
                if let Some(obj) = global.as_object() {
                    dates.push(date_str);
                    reports_data.push(obj.clone());
                } else {
                    dates.push(date_str);
                    reports_data.push(Map::new());
                }
            }
        }

        current = current.succ_opt().unwrap_or(end);
    }

    if reports_data.is_empty() {
        return Err(UsageReportError::EmptyList {
            listname: "reports_data".to_string(),
            message: "Aucune donnée trouvée pour la période demandée.".to_string(),
        });
    }

    Ok((dates, reports_data))
}

pub fn generate_aggregate_report(
    from_date: &str,
    to_date: &str,
    database: &Path,
    output: &Path,
) -> Result<(), UsageReportError> {
    let (dates, reports_data) = load_reports_data(database, from_date, to_date)?;

    let mut calendars = Vec::new();
    let mut line_plots = Vec::new();

    for (metric_key, metric_title) in METRICS_CONFIG {
        let values: Vec<f64> = reports_data
            .iter()
            .map(|r| r.get(metric_key).and_then(|v| v.as_f64()).unwrap_or(0.0))
            .collect();

        let min_value = values.iter().cloned().fold(0.0f64, f64::min);
        let max_value = values.iter().cloned().fold(0.0f64, f64::max);

        let calendar_html = generate_calendar_heatmap(&dates, &values)?;

        calendars.push(serde_json::json!({
            "title": metric_title,
            "calendar_html": calendar_html,
            "min_value": min_value,
            "max_value": max_value,
        }));

        let line_html = generate_line_plot(&dates, &values, metric_title)?;

        line_plots.push(serde_json::json!({
            "title": metric_title,
            "html": line_html,
        }));
    }

    let num_days = reports_data.iter().filter(|r| !r.is_empty()).count();

    let template = JINJA_ENV.get_template("aggregated_efficiency.html.j2")?;
    let html = template.render(serde_json::json!({
        "from_date": from_date,
        "to_date": to_date,
        "num_days": num_days,
        "calendars": calendars,
        "line_plots": line_plots,
        "generation_time": Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
    }))?;

    std::fs::write(output, &html)?;

    eprintln!("Rapport agrégé généré dans {}", output.display());

    Ok(())
}
