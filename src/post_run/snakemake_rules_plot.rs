//! Module pour générer des graphiques Plotly des règles Snakemake.
//!
//! Ce module fournit des fonctions pour créer des visualisations interactives
//! des performances des règles Snakemake.

use polars::prelude::*;

use plotly::{
    BoxPlot, Layout,
    box_plot::BoxPoints,
    common::{Anchor, Orientation},
    layout::{
        Axis,
        update_menu::{Button, ButtonMethod, UpdateMenu, UpdateMenuDirection},
    },
};

use crate::UsageReportError;

/// Génère un graphique unique avec une combobox pour sélectionner les règles.
///
/// Cette fonction retourne une chaîne HTML contenant un div nommé par l'utilisateur,
/// une combobox pour sélectionner soit toutes les règles (ALL) soit une règle spécifique,
/// et les graphiques boxplot correspondants.
///
/// # Arguments
///
/// * `lf` - Un LazyFrame contenant au moins les colonnes `rule_name` et la colonne à visualiser
/// * `column` - Le nom de la colonne à visualiser (ex: "runtime")
/// * `title` - Le titre du graphique
/// * `div_name` - Le nom/ID du div racine (choisi par l'utilisateur)
///
/// # Returns
///
/// Une chaîne de caractères contenant un div HTML unique avec combobox et graphiques.
/// N'inclus pas le script de plotly.js, celui-ci doit être ajouté au header
/// du document html dans lequel ce graphique est utilisé
pub fn plot_snakemake_rules(
    lf: &LazyFrame,
    column: &str,
    title: &str,
    div_name: Option<&str>,
) -> Result<String, UsageReportError> {
    let rule_names: Vec<String> = lf
        .clone()
        .collect()?
        .column("rule_name")?
        .unique()?
        .sort(SortOptions::new().with_order_reversed())?
        .as_series()
        .ok_or(UsageReportError::NoneValueError {
            message: "Could not get a series for rule_name column".to_string(),
            file: file!().into(),
            line: line!(),
            column: column!(),
        })?
        .str()?
        .into_iter()
        .filter_map(|e| e.map(|e| e.to_string()))
        .collect();

    let mut buttons: Vec<Button> = Vec::new();
    buttons.push(
        Button::new()
            .label("ALL")
            .method(ButtonMethod::Restyle)
            .args(serde_json::json!([
                {"visible": ([true]).repeat(rule_names.len()).into_iter().collect::<Vec<bool>>()}
            ])),
    );
    let mut figure = plotly::Plot::new();

    for (i, rule_name) in rule_names.iter().enumerate() {
        let mut visible: Vec<bool> = [false]
            .repeat(rule_names.len())
            .into_iter()
            .collect::<Vec<bool>>();
        visible[i] = true;
        buttons.push(
            Button::new()
                .label(rule_name)
                .method(ButtonMethod::Restyle)
                .args(serde_json::json!([
                    {"visible": visible}
                ])),
        );
        let rule_data = lf
            .clone()
            .filter(col("rule_name").eq(lit(rule_name.to_string())))
            .collect()?;
        figure.add_trace(
            BoxPlot::new_xy(
                rule_data[column]
                    .cast(&DataType::Float32)?
                    .as_series()
                    .ok_or(UsageReportError::NoneValueError {
                        message: "Could not get a series for rule_name column".to_string(),
                        file: file!().into(),
                        line: line!(),
                        column: column!(),
                    })?
                    .f32()?
                    .into_iter()
                    .collect(),
                [rule_name]
                    .repeat(rule_data.shape().0)
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            )
            .name(rule_name)
            .box_points(BoxPoints::SuspectedOutliers)
            .orientation(Orientation::Horizontal),
        );
    }
    let update_menus = UpdateMenu::new()
        .buttons(buttons)
        .direction(UpdateMenuDirection::Down)
        .show_active(true)
        .x(1.0)
        .y(1.02)
        .y_anchor(Anchor::Top)
        .active(0);
    figure.set_layout(
        Layout::new()
            .update_menus(vec![update_menus])
            .title(title)
            .x_axis(Axis::new().title(column))
            .y_axis(
                Axis::new()
                    .title("Nom de la règle")
                    .auto_margin(true)
                    .tick_mode(plotly::common::TickMode::Auto)
                    .tick_length(5),
            )
            .show_legend(false)
            .height(400 + 40 * rule_names.len()),
    );

    Ok(figure.to_inline_html(div_name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_plot_function() {
        let lf = df!(
            "rule_name" => ["rule1", "rule1", "rule2", "rule2", "rule3", "rule3", "rule3", "rule3"],
            "runtime" => [10.0, 20.0, 30.0, 40.0, 50.0, 45.0, 55.0, 70.0]
        )
        .expect("Cannot create dataframe")
        .lazy();

        let res = plot_snakemake_rules(
            &lf,
            "runtime",
            "Durée d'exécution (en minutes) par règle",
            Some("test_div"),
        )
        .unwrap();

        eprintln!("{}", res);

        assert_eq!(
            res,
            r#"<div id="test_div" class="plotly-graph-div" style="height:100%; width:100%;"></div>
<script type="text/javascript">
    Plotly.newPlot("test_div", {"data":[{"type":"box","x":[50.0,45.0,55.0,70.0],"y":["rule3","rule3","rule3","rule3"],"name":"rule3","orientation":"h","boxpoints":"suspectedoutliers"},{"type":"box","x":[30.0,40.0],"y":["rule2","rule2"],"name":"rule2","orientation":"h","boxpoints":"suspectedoutliers"},{"type":"box","x":[10.0,20.0],"y":["rule1","rule1"],"name":"rule1","orientation":"h","boxpoints":"suspectedoutliers"}],"layout":{"title":{"text":"Durée d\u0027exécution (en minutes) par règle"},"showlegend":false,"height":520,"xaxis":{"title":{"text":"runtime"}},"yaxis":{"title":{"text":"Nom de la règle"},"tickmode":"auto","ticklen":5,"automargin":true},"updatemenus":[{"active":0,"buttons":[{"args":[{"visible":[true,true,true]}],"label":"ALL","method":"restyle"},{"args":[{"visible":[true,false,false]}],"label":"rule3","method":"restyle"},{"args":[{"visible":[false,true,false]}],"label":"rule2","method":"restyle"},{"args":[{"visible":[false,false,true]}],"label":"rule1","method":"restyle"}],"direction":"down","showactive":true,"x":1.0,"y":1.02,"yanchor":"top"}]},"config":{},"frames":null});
</script>"#
        );
    }
}
