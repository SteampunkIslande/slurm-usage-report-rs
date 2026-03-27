pub mod snakemake_rules_plot;

use same_file::is_same_file;
pub use snakemake_rules_plot::*;

pub mod snakemake_parse_log;
pub use snakemake_parse_log::*;

pub mod sacct_get;
pub use sacct_get::*;

use std::{fs::OpenOptions, path::Path};

use crate::{
    JINJA_ENV, UsageReportError, add_metrics_relative_to_input_size_inplace, add_snakerule_col,
    aggregate_per_snakemake_rule, generic_report, utils,
};

pub fn generate_snakemake_efficiency_report<I, P, S>(
    output_html: P,
    input_parquet: P,
    job_names: I,
    output_parquet: P,
    input_sizes_csv: Option<P>,
    use_tabs: bool,
) -> Result<(), UsageReportError>
where
    I: IntoIterator<Item = S>,
    P: AsRef<Path>,
    S: AsRef<str>,
{
    use crate::utils::sink_parquet;
    use polars::prelude::*;
    use serde_json::json;
    use std::io::Write;

    // On ne doit pas toucher à input_parquet, c'est le parquet brut, directement issu de SACCT (avec tous ses éventuels doublons, et toutes les infos inhérentes aux jobs et à leurs jobsteps).
    std::fs::copy(input_parquet.as_ref(), output_parquet.as_ref())?;

    let args = ScanArgsParquet::default();
    let mut lf = LazyFrame::scan_parquet(PlRefPath::try_from_path(output_parquet.as_ref())?, args)?;

    lf = generic_report(lf)?;
    // Filter by job name, only after the aggregate is done
    for job_name in job_names {
        lf = lf.filter(
            col("JobName")
                .str()
                .contains(lit(job_name.as_ref().to_string()), false),
        );
    }
    lf = add_snakerule_col(lf);

    if let Some(input_sizes) = input_sizes_csv.as_ref() {
        // Sauvegarder le parquet en cours de lecture par le lazyframe, car on s'apprête à écrire dedans
        let parquet_temp = output_parquet.as_ref().with_extension("tmp.parquet");
        sink_parquet(lf, &parquet_temp)?;

        // Cette fonction est `in-place`, c'est à dire qu'elle est conçue pour prendre le même chemin en entrée et en sortie
        // En l'occurence, le fichier temporaire que l'on vient de créer
        add_metrics_relative_to_input_size_inplace(parquet_temp.as_path(), input_sizes.as_ref())?;

        // Plus besoin du fichier temporaire, on remplace input_parquet par le fichier enrichi
        std::fs::rename(parquet_temp, output_parquet.as_ref())?;

        let args = ScanArgsParquet::default();
        // input_parquet a été enrichi, on le réouvre sous forme de Layframe pour continuer l'extraction des données
        lf = LazyFrame::scan_parquet(PlRefPath::try_from_path(output_parquet.as_ref())?, args)?;
    }

    let mem_box_plot = plot_snakemake_rules(
        &lf,
        "MemEfficiencyPercent",
        "Efficacité mémoire (en %)",
        Some("memef_percent"),
    )?;

    let cpu_box_plot = plot_snakemake_rules(
        &lf,
        "CPUEfficiencyPercent",
        "Taux d'utilisation des CPUs",
        Some("cpuef_percent"),
    )?;

    let runtime_box_plot = plot_snakemake_rules(
        &lf,
        "ElapsedRaw",
        "Durée d'exécution (en secondes)",
        Some("runtime_seconds"),
    )?;

    let relative_mem_box_plot = if input_sizes_csv.is_some() {
        Some(plot_snakemake_rules(
            &lf,
            "UsedRAMPerMo",
            "Quantité de RAM utilisée (en Mo) par Mo de fichier(s) d'entrée",
            Some("relative_ram_per_inputmo"),
        )?)
    } else {
        None
    };
    let relative_runtime_box_plot = if input_sizes_csv.is_some() {
        Some(plot_snakemake_rules(
            &lf,
            "MinPerMo",
            "Durée d'exécution (en minutes) par Mo de fichier(s) d'entrée",
            Some("min_per_input_mo"),
        )?)
    } else {
        None
    };

    let lf_agg = aggregate_per_snakemake_rule(lf.clone(), input_sizes_csv.is_some());
    let efficiency_table_mem = utils::df_to_columnar_json(
        &(lf_agg
            .clone()
            .select(&[
                col("rule_name").alias("Nom de la règle"),
                col("MemEfficiencyPercent_mean").alias("Efficacité mémoire moyenne"),
                col("MemEfficiencyPercent_median").alias("Efficacité mémoire médiane"),
                col("MemEfficiencyPercent_std").alias("Efficacité mémoire (écart-type)"),
                col("MemEfficiencyPercent_min").alias("Efficacité mémoire minimum"),
                col("MemEfficiencyPercent_max").alias("Efficacité mémoire maximum"),
            ])
            .sort(["Nom de la règle"], SortMultipleOptions::default())
            .collect()?),
    )?;
    let efficiency_table_cpu = utils::df_to_columnar_json(
        &(lf_agg
            .clone()
            .select(&[
                col("rule_name").alias("Nom de la règle"),
                col("CPUEfficiencyPercent_mean").alias("Efficacité CPU moyenne"),
                col("CPUEfficiencyPercent_median").alias("Efficacité CPU médiane"),
                col("CPUEfficiencyPercent_std").alias("Efficacité CPU (écart-type)"),
                col("CPUEfficiencyPercent_min").alias("Efficacité CPU minimum"),
                col("CPUEfficiencyPercent_max").alias("Efficacité CPU maximum"),
            ])
            .sort(["Nom de la règle"], SortMultipleOptions::default())
            .collect()?),
    )?;
    let efficiency_table_runtime = utils::df_to_columnar_json(
        &(lf_agg
            .clone()
            .select(&[
                col("rule_name").alias("Nom de la règle"),
                col("ElapsedRaw_mean").alias("Durée moyenne"),
                col("ElapsedRaw_median").alias("Durée médiane"),
                col("ElapsedRaw_std").alias("Durée (écart-type)"),
                col("ElapsedRaw_min").alias("Durée minimum"),
                col("ElapsedRaw_max").alias("Durée maximum"),
            ])
            .sort(["Nom de la règle"], SortMultipleOptions::default())
            .collect()?),
    )?;
    let efficiency_table_relative_mem = if input_sizes_csv.is_some() {
        Some(utils::df_to_columnar_json(
            &(lf_agg
                .clone()
                .select(&[
                    col("rule_name").alias("Nom de la règle"),
                    col("UsedRAMPerMo_mean").alias("RAM utilisée par Mo (moyenne)"),
                    col("UsedRAMPerMo_median").alias("RAM utilisée par Mo (médiane)"),
                    col("UsedRAMPerMo_std").alias("RAM utilisée par Mo (écart-type)"),
                    col("UsedRAMPerMo_min").alias("RAM utilisée par Mo (minimum)"),
                    col("UsedRAMPerMo_max").alias("RAM utilisée par Mo (maximum)"),
                ])
                .sort(["Nom de la règle"], SortMultipleOptions::default())
                .collect()?),
        )?)
    } else {
        None
    };
    let efficiency_table_relative_runtime = if input_sizes_csv.is_some() {
        Some(utils::df_to_columnar_json(
            &(lf_agg
                .clone()
                .select(&[
                    col("rule_name").alias("Nom de la règle"),
                    col("MinPerMo_mean").alias("Minutes par Mo (moyenne)"),
                    col("MinPerMo_median").alias("Minutes par Mo (médiane)"),
                    col("MinPerMo_std").alias("Minutes par Mo (écart-type)"),
                    col("MinPerMo_min").alias("Minutes par Mo (minimum)"),
                    col("MinPerMo_max").alias("Minutes par Mo (maximum)"),
                ])
                .sort(["Nom de la règle"], SortMultipleOptions::default())
                .collect()?),
        )?)
    } else {
        None
    };
    let template = JINJA_ENV
        .get_template("snakemake_report_template.html.j2")
        .expect("Template cannot be found (for a test, this is bad)");
    let output = template.render(json! (
        {
            "mem_box_plot": mem_box_plot,
            "cpu_box_plot": cpu_box_plot,
            "runtime_box_plot": runtime_box_plot,
            "relative_mem_box_plot": relative_mem_box_plot,
            "relative_runtime_box_plot": relative_runtime_box_plot,
            "efficiency_table_mem": efficiency_table_mem,
            "efficiency_table_cpu": efficiency_table_cpu,
            "efficiency_table_runtime": efficiency_table_runtime,
            "efficiency_table_relative_mem": efficiency_table_relative_mem,
            "efficiency_table_relative_runtime": efficiency_table_relative_runtime,
            "use_tabs": use_tabs
        }
    ))?;
    let mut f = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output_html)?;
    write!(f, "{}", output)?;

    let parquet_temp = output_parquet.as_ref().with_extension("tmp.parquet");
    if parquet_temp.exists() || is_same_file(&parquet_temp, &output_parquet).unwrap_or(false) {
        return Err(UsageReportError::SameFile(
            parquet_temp.display().to_string(),
            output_parquet.as_ref().display().to_string(),
        ));
    }
    utils::sink_parquet(lf.clone(), parquet_temp.as_path())?;
    std::fs::rename(parquet_temp.as_path(), output_parquet.as_ref())?;

    Ok(())
}
