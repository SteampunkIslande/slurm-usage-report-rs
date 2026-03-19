//! Module pour générer des graphiques Plotly des règles Snakemake.
//!
//! Ce module fournit des fonctions pour créer des visualisations interactives
//! des performances des règles Snakemake.

use plotlars::Plot;
use polars::prelude::*;

/// Génère des graphiques d'efficacité pour chaque règle Snakemake unique.
///
/// Cette fonction prend un LazyFrame contenant des données de règles Snakemake,
/// récupère les valeurs uniques de la colonne `rule_name`, et génère un graphique
/// Plotly (boxplot) pour chaque règle.
///
/// # Arguments
///
/// * `lf` - Un LazyFrame contenant au moins les colonnes `rule_name` et la colonne à visualiser
/// * `column` - Le nom de la colonne à visualiser (ex: "runtime")
/// * `title` - Le titre du graphique
///
/// # Returns
///
/// Un vecteur de Strings contenant les graphiques HTML inline pour chaque règle
pub fn plot_all_snakemake_rule_efficiencies(
    lf: LazyFrame,
    column: &str,
    title: &str,
) -> Vec<String> {
    // Collecter les valeurs uniques de rule_name
    let df_unique = lf
        .clone()
        .select([col("rule_name")])
        .unique(None, UniqueKeepStrategy::First)
        .collect()
        .unwrap();

    let rule_name_series = df_unique.column("rule_name").unwrap();
    let rule_names: Vec<String> = rule_name_series
        .as_series()
        .expect("Cannot get series")
        .str()
        .expect("rule_name is not a string series")
        .iter()
        .flatten()
        .map(|s| s.to_string())
        .collect();

    // Générer un graphique pour chaque rule_name
    let mut plots: Vec<String> = Vec::new();

    for rule_name in &rule_names {
        // Filtrer le LazyFrame pour cette règle
        let filtered_lf = lf
            .clone()
            .filter(col("rule_name").eq(lit(rule_name.as_str())));

        // Collecter le DataFrame
        let df = filtered_lf.collect().unwrap();

        // Générer le graphique
        let plot_html = plot_snakemake_rule_efficiency(&df, column, title, rule_name);

        plots.push(plot_html);
    }

    // Ajouter toutes les colonnes confondues
    let filtered_lf = lf.clone();

    // Collecter le DataFrame
    let df = filtered_lf.collect().unwrap();

    // Générer le graphique
    let plot_html = plot_snakemake_rule_efficiency(&df, column, title, "ALL");

    plots.push(plot_html);

    plots
}

pub fn plot_snakemake_rule_efficiency(
    df: &DataFrame,
    column: &str,
    title: &str,
    col_value: &str,
) -> String {
    let plot = plotlars::BoxPlot::builder()
        .data(&df)
        .labels("rule_name")
        .values(column)
        .orientation(plotlars::Orientation::Horizontal)
        .box_points(true)
        .point_offset(-1.5)
        .jitter(0.01)
        .opacity(0.1)
        .colors(vec![
            plotlars::Rgb(0, 191, 255),
            plotlars::Rgb(57, 255, 20),
            plotlars::Rgb(255, 105, 180),
        ])
        .plot_title(plotlars::Text::from(title).font("Arial").size(18))
        .x_title(plotlars::Text::from(title).font("Arial").size(15))
        .y_title(
            plotlars::Text::from("Noms des règles")
                .font("Arial")
                .size(15),
        )
        .y_axis(&plotlars::Axis::new().value_thousands(true))
        .legend(&plotlars::Legend::new().border_width(1).x(0.9))
        .build();

    plot.to_inline_html(Some(col_value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plot_snakemake_rule_efficiency() {
        // Créer un DataFrame de test
        let lf = df!(
            "rule_name" => ["rule1", "rule1", "rule2", "rule2"],
            "runtime" => [10.0, 20.0, 30.0, 40.0]
        )
        .unwrap()
        .lazy();

        let df1 = lf
            .clone()
            .filter(col("rule_name").eq("rule1"))
            .collect()
            .unwrap();
        let df2 = lf
            .clone()
            .filter(col("rule_name").eq("rule2"))
            .collect()
            .unwrap();

        let html_rule1 =
            plot_snakemake_rule_efficiency(&df1, "runtime", "Temps d'exécution (s)", "rule1");
        let html_rule2 =
            plot_snakemake_rule_efficiency(&df2, "runtime", "Temps d'exécution (s)", "rule2");
        eprintln!("{}\n\n{}", html_rule1, html_rule2);
    }

    #[test]
    fn test_plot_all_snakemake_rule_efficiencies() {
        // Créer un LazyFrame de test
        let lf = df!(
            "rule_name" => ["rule1", "rule1", "rule2", "rule2", "rule3"],
            "runtime" => [10.0, 20.0, 30.0, 40.0, 50.0]
        )
        .unwrap()
        .lazy();

        // Tester la nouvelle fonction
        let plots = plot_all_snakemake_rule_efficiencies(lf, "runtime", "Temps d'exécution (s)");

        // Devrait avoir 4 graphiques: rule1, rule2, rule3 et ALL
        assert_eq!(plots.len(), 4);

        // Vérifier que chaque graphique contient du HTML
        for plot in &plots {
            assert!(plot.contains("plotly"));
        }

        eprintln!("Generated {} plots", plots.len());
    }
}
