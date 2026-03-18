//! Module pour générer des graphiques Plotly des règles Snakemake.
//!
//! Ce module fournit des fonctions pour créer des visualisations interactives
//! des performances des règles Snakemake.

use polars::prelude::*;
use serde::{Deserialize, Serialize};

/// Génère un graphique de type box plot interactif pour l'efficacité des règles Snakemake.
///
/// # Arguments
///
/// * `df` - DataFrame Polars contenant les données des règles Snakemake
/// * `column` - Nom de la colonne à visualiser (ex: "runtime", "memory")
/// * `title` - Titre du graphique
///
/// # Returns
///
/// Une chaîne HTML contenant le graphique Plotly interactif
pub fn plot_snakemake_rule_efficiency(df: &DataFrame, column: &str, title: &str) -> String {
    // Extraire les noms de règles uniques et les trier par ordre alphabétique inverse
    let mut rule_names: Vec<String> = df
        .column("rule_name")
        .expect("Colonne 'rule_name' non trouvée")
        .unique()
        .expect("Erreur lors de l'extraction des règles uniques")
        .str()
        .expect("Type de colonne incorrect")
        .into_iter()
        .filter_map(|s| s)
        .filter_map(|s| Some(String::from(s)))
        .collect();

    rule_names.sort_by(|a, b| b.cmp(a)); // Tri décroissant

    let num_rules = rule_names.len();

    // Créer les boutons pour le dropdown
    let mut buttons: Vec<serde_json::Value> = Vec::new();

    // Bouton "ALL" pour afficher toutes les règles (par défaut)
    let all_visible: Vec<bool> = vec![true; num_rules];
    buttons.push(serde_json::json!({
        "label": "ALL",
        "method": "restyle",
        "args": [{"visible": all_visible}]
    }));

    // Boutons pour chaque règle individuelle
    for i in 0..num_rules {
        let mut visible: Vec<bool> = vec![false; num_rules];
        visible[i] = true;

        buttons.push(serde_json::json!({
            "label": rule_names[i],
            "method": "restyle",
            "args": [{"visible": visible}]
        }));
    }

    // Créer les traces pour chaque règle
    let mut traces: Vec<serde_json::Value> = Vec::new();

    for rule_name in &rule_names {
        let mask = col("rule_name").eq(rule_name.as_str());
        // Filtrer les données pour cette règle
        let rule_data = df
            .filter(&mask)
            .expect("Erreur lors du filtrage des données");

        // Extraire les valeurs de la colonne
        let values: Vec<f64> = rule_data
            .column(column)
            .expect(&format!("Colonne '{}' non trouvée", column))
            .cast(&DataType::Float64)
            .expect(&format!("Type de colonne '{}' incorrect", column))
            .as_series()
            .expect("Cannot convert to series")
            .iter()
            .collect();

        // Créer les valeurs y (une copie du nom de règle pour chaque point)
        let y_values: Vec<&str> = vec![rule_name.as_str(); values.len()];

        // Créer la trace box
        let trace = serde_json::json!({
            "type": "box",
            "x": values,
            "y": y_values,
            "name": rule_name,
            "boxpoints": "suspectedoutliers",
            "orientation": "h"
        });

        traces.push(trace);
    }

    // Calculer la hauteur dynamique
    let height = 400 + 30 * num_rules;

    // Construire le layout
    let layout = serde_json::json!({
        "updatemenus": [{
            "buttons": buttons,
            "direction": "down",
            "showactive": true,
            "x": 1.0,
            "xanchor": "left",
            "y": 1.02,
            "yanchor": "top",
            "active": 0
        }],
        "showlegend": false,
        "xaxis": {
            "title": {
                "text": title
            }
        },
        "yaxis": {
            "title": {
                "text": "Règles Snakemake"
            }
        },
        "height": height
    });

    // Construire la figure complète
    let fig = serde_json::json!({
        "data": traces,
        "layout": layout
    });

    // Convertir en HTML
    let fig_json = serde_json::to_string(&fig).expect("Erreur lors de la sérialisation JSON");

    // Générer le HTML Plotly
    generate_plotly_html(&fig_json)
}

/// Génère le HTML complet pour afficher une figure Plotly.
///
/// # Arguments
///
/// * `fig_json` - Représentation JSON de la figure Plotly
///
/// # Returns
///
/// Une chaîne HTML complète
fn generate_plotly_html(fig_json: &str) -> String {
    format!(
        r#"<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8" />
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
</head>
<body>
    <div id="plotly-div"></div>
    <script>
        var fig = {};
        Plotly.newPlot('plotly-div', fig.data, fig.layout);
    </script>
</body>
</html>"#,
        fig_json = fig_json
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plot_snakemake_rule_efficiency() {
        // Créer un DataFrame de test
        let df = df!(
            "rule_name" => ["rule1", "rule1", "rule2", "rule2"],
            "runtime" => [10.0, 20.0, 30.0, 40.0]
        )
        .unwrap();

        let html = plot_snakemake_rule_efficiency(&df, "runtime", "Temps d'exécution (s)");

        // Vérifier que le HTML contient les éléments attendus
        assert!(html.contains("plotly-div"));
        assert!(html.contains("plotly-latest.min.js"));
        assert!(html.contains("rule1"));
        assert!(html.contains("rule2"));
    }
}
