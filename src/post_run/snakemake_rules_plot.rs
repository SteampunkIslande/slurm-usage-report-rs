//! Module pour générer des graphiques Plotly des règles Snakemake.
//!
//! Ce module fournit des fonctions pour créer des visualisations interactives
//! des performances des règles Snakemake.

use plotlars::Plot;
use polars::prelude::*;

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
/// Une chaîne de caractères contenant un div HTML unique avec combobox et graphiques
pub fn plot_snakemake_rules_with_selector(
    lf: LazyFrame,
    column: &str,
    title: &str,
    div_name: &str,
) -> String {
    // Collecter les valeurs uniques de rule_name
    let df_unique = lf
        .clone()
        .select([col("rule_name")])
        .unique(None, UniqueKeepStrategy::First)
        .collect()
        .expect("Cannot collect dataframe");

    let rule_name_series = df_unique
        .column("rule_name")
        .expect("No column named rule_name");
    let rule_names: Vec<String> = rule_name_series
        .as_series()
        .expect("Cannot get series")
        .str()
        .expect("rule_name is not a string series")
        .sort(true)
        .iter()
        .flatten()
        .map(|s| s.to_string())
        .collect();

    // Générer les options de la combobox
    let mut options_html = String::from("<option value=\"ALL\">ALL</option>");
    for rule_name in &rule_names {
        options_html.push_str(&format!(
            "<option value=\"{}\">{}</option>",
            rule_name, rule_name
        ));
    }

    // Générer les graphiques pour chaque règle
    let mut plots_html = String::new();

    // Graphique pour "ALL" (visible par défaut)
    let filtered_lf_all = lf.clone();
    let df_all = filtered_lf_all.collect().expect("Cannot collect dataframe");
    let plot_all = plot_snakemake_rule_efficiency(&df_all, column, title, "ALL");
    plots_html.push_str(&format!(
        r#"<div id="{}_plot_ALL" class="rule-plot">{}</div>"#,
        div_name, plot_all
    ));

    // Graphique pour chaque règle spécifique (cachés par défaut)
    for rule_name in &rule_names {
        let filtered_lf = lf
            .clone()
            .filter(col("rule_name").eq(lit(rule_name.as_str())));

        let df = filtered_lf.collect().expect("Cannot collect dataframe");
        let plot_html = plot_snakemake_rule_efficiency(&df, column, title, rule_name);

        plots_html.push_str(&format!(
            r#"<div id="{}_plot_{}" class="rule-plot">{}</div>"#,
            div_name, rule_name, plot_html
        ));
    }

    // Construire le HTML final avec la combobox et le JavaScript
    let html = format!(
        r#"<div id="{}">
  <select id="{}_selector" onchange="{}_updatePlot(this.value)">
    {}
  </select>
  {}
  <script>
    function {}_updatePlot(value) {{
      var plots = document.getElementsByClassName('rule-plot');
      for (var i = 0; i < plots.length; i++) {{
        plots[i].style.display = 'none';
      }}
      var selectedPlot = document.getElementById('{}_plot_' + value);
      if (selectedPlot) {{
        selectedPlot.style.display = 'block';
      }}
    }}
  </script>
</div>"#,
        div_name, div_name, div_name, options_html, plots_html, div_name, div_name
    );

    html
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
        .expect("Cannot build dataframe")
        .lazy();

        let df1 = lf
            .clone()
            .filter(col("rule_name").eq(lit("rule1")))
            .collect()
            .expect("Cannot collect lazyframe");
        let df2 = lf
            .clone()
            .filter(col("rule_name").eq(lit("rule2")))
            .collect()
            .expect("Cannot collect lazyframe");

        let html_rule1 =
            plot_snakemake_rule_efficiency(&df1, "runtime", "Temps d'exécution (s)", "rule1");
        let html_rule2 =
            plot_snakemake_rule_efficiency(&df2, "runtime", "Temps d'exécution (s)", "rule2");

        assert_eq!(
            html_rule1.as_str(),
            r#"<div id="rule1" class="plotly-graph-div" style="height:100%; width:100%;"></div>
<script type="text/javascript">
    Plotly.newPlot("rule1", {"data":[{"type":"box","x":[10.0,20.0],"y":["rule1","rule1"],"orientation":"h","marker":{"opacity":0.1,"color":"rgb(0, 191, 255)"},"boxpoints":"all","pointpos":-1.5,"jitter":0.01}],"layout":{"title":{"text":"Temps d\u0027exécution (s)","font":{"family":"Arial","size":18,"color":"rgb(0, 0, 0)"},"x":0.5,"y":0.9},"legend":{"bgcolor":"rgb(255, 255, 255)","borderwidth":1,"x":0.9},"xaxis":{"title":{"text":"Temps d\u0027exécution (s)","font":{"family":"Arial","size":15,"color":"rgb(0, 0, 0)"},"x":0.5,"y":-0.15}},"yaxis":{"title":{"text":"Noms des règles","font":{"family":"Arial","size":15,"color":"rgb(0, 0, 0)"},"x":-0.08,"y":0.5},"separatethousands":true}},"config":{},"frames":null});
</script>"#
        );

        assert_eq!(
            html_rule2.as_str(),
            r#"<div id="rule2" class="plotly-graph-div" style="height:100%; width:100%;"></div>
<script type="text/javascript">
    Plotly.newPlot("rule2", {"data":[{"type":"box","x":[30.0,40.0],"y":["rule2","rule2"],"orientation":"h","marker":{"opacity":0.1,"color":"rgb(0, 191, 255)"},"boxpoints":"all","pointpos":-1.5,"jitter":0.01}],"layout":{"title":{"text":"Temps d\u0027exécution (s)","font":{"family":"Arial","size":18,"color":"rgb(0, 0, 0)"},"x":0.5,"y":0.9},"legend":{"bgcolor":"rgb(255, 255, 255)","borderwidth":1,"x":0.9},"xaxis":{"title":{"text":"Temps d\u0027exécution (s)","font":{"family":"Arial","size":15,"color":"rgb(0, 0, 0)"},"x":0.5,"y":-0.15}},"yaxis":{"title":{"text":"Noms des règles","font":{"family":"Arial","size":15,"color":"rgb(0, 0, 0)"},"x":-0.08,"y":0.5},"separatethousands":true}},"config":{},"frames":null});
</script>"#
        );
    }

    #[test]
    fn test_plot_snakemake_rules_with_selector() {
        // Créer un LazyFrame de test
        let lf = df!(
            "rule_name" => ["rule1", "rule1", "rule2", "rule2", "rule3", "rule3", "rule3", "rule3"],
            "runtime" => [10.0, 20.0, 30.0, 40.0, 50.0, 45.0, 55.0, 70.0]
        )
        .expect("Cannot create dataframe")
        .lazy();

        // Tester la nouvelle fonction avec selector
        let html = plot_snakemake_rules_with_selector(
            lf,
            "runtime",
            "Temps d'exécution (s)",
            "my_plot_div",
        );

        assert_eq!(
            html.as_str(),
            r#"<div id="my_plot_div">
  <select id="my_plot_div_selector" onchange="my_plot_div_updatePlot(this.value)">
    <option value="ALL">ALL</option><option value="rule3">rule3</option><option value="rule2">rule2</option><option value="rule1">rule1</option>
  </select>
  <div id="my_plot_div_plot_ALL" class="rule-plot"><div id="ALL" class="plotly-graph-div" style="height:100%; width:100%;"></div>
<script type="text/javascript">
    Plotly.newPlot("ALL", {"data":[{"type":"box","x":[10.0,20.0,30.0,40.0,50.0,45.0,55.0,70.0],"y":["rule1","rule1","rule2","rule2","rule3","rule3","rule3","rule3"],"orientation":"h","marker":{"opacity":0.1,"color":"rgb(0, 191, 255)"},"boxpoints":"all","pointpos":-1.5,"jitter":0.01}],"layout":{"title":{"text":"Temps d\u0027exécution (s)","font":{"family":"Arial","size":18,"color":"rgb(0, 0, 0)"},"x":0.5,"y":0.9},"legend":{"bgcolor":"rgb(255, 255, 255)","borderwidth":1,"x":0.9},"xaxis":{"title":{"text":"Temps d\u0027exécution (s)","font":{"family":"Arial","size":15,"color":"rgb(0, 0, 0)"},"x":0.5,"y":-0.15}},"yaxis":{"title":{"text":"Noms des règles","font":{"family":"Arial","size":15,"color":"rgb(0, 0, 0)"},"x":-0.08,"y":0.5},"separatethousands":true}},"config":{},"frames":null});
</script></div><div id="my_plot_div_plot_rule3" class="rule-plot"><div id="rule3" class="plotly-graph-div" style="height:100%; width:100%;"></div>
<script type="text/javascript">
    Plotly.newPlot("rule3", {"data":[{"type":"box","x":[50.0,45.0,55.0,70.0],"y":["rule3","rule3","rule3","rule3"],"orientation":"h","marker":{"opacity":0.1,"color":"rgb(0, 191, 255)"},"boxpoints":"all","pointpos":-1.5,"jitter":0.01}],"layout":{"title":{"text":"Temps d\u0027exécution (s)","font":{"family":"Arial","size":18,"color":"rgb(0, 0, 0)"},"x":0.5,"y":0.9},"legend":{"bgcolor":"rgb(255, 255, 255)","borderwidth":1,"x":0.9},"xaxis":{"title":{"text":"Temps d\u0027exécution (s)","font":{"family":"Arial","size":15,"color":"rgb(0, 0, 0)"},"x":0.5,"y":-0.15}},"yaxis":{"title":{"text":"Noms des règles","font":{"family":"Arial","size":15,"color":"rgb(0, 0, 0)"},"x":-0.08,"y":0.5},"separatethousands":true}},"config":{},"frames":null});
</script></div><div id="my_plot_div_plot_rule2" class="rule-plot"><div id="rule2" class="plotly-graph-div" style="height:100%; width:100%;"></div>
<script type="text/javascript">
    Plotly.newPlot("rule2", {"data":[{"type":"box","x":[30.0,40.0],"y":["rule2","rule2"],"orientation":"h","marker":{"opacity":0.1,"color":"rgb(0, 191, 255)"},"boxpoints":"all","pointpos":-1.5,"jitter":0.01}],"layout":{"title":{"text":"Temps d\u0027exécution (s)","font":{"family":"Arial","size":18,"color":"rgb(0, 0, 0)"},"x":0.5,"y":0.9},"legend":{"bgcolor":"rgb(255, 255, 255)","borderwidth":1,"x":0.9},"xaxis":{"title":{"text":"Temps d\u0027exécution (s)","font":{"family":"Arial","size":15,"color":"rgb(0, 0, 0)"},"x":0.5,"y":-0.15}},"yaxis":{"title":{"text":"Noms des règles","font":{"family":"Arial","size":15,"color":"rgb(0, 0, 0)"},"x":-0.08,"y":0.5},"separatethousands":true}},"config":{},"frames":null});
</script></div><div id="my_plot_div_plot_rule1" class="rule-plot"><div id="rule1" class="plotly-graph-div" style="height:100%; width:100%;"></div>
<script type="text/javascript">
    Plotly.newPlot("rule1", {"data":[{"type":"box","x":[10.0,20.0],"y":["rule1","rule1"],"orientation":"h","marker":{"opacity":0.1,"color":"rgb(0, 191, 255)"},"boxpoints":"all","pointpos":-1.5,"jitter":0.01}],"layout":{"title":{"text":"Temps d\u0027exécution (s)","font":{"family":"Arial","size":18,"color":"rgb(0, 0, 0)"},"x":0.5,"y":0.9},"legend":{"bgcolor":"rgb(255, 255, 255)","borderwidth":1,"x":0.9},"xaxis":{"title":{"text":"Temps d\u0027exécution (s)","font":{"family":"Arial","size":15,"color":"rgb(0, 0, 0)"},"x":0.5,"y":-0.15}},"yaxis":{"title":{"text":"Noms des règles","font":{"family":"Arial","size":15,"color":"rgb(0, 0, 0)"},"x":-0.08,"y":0.5},"separatethousands":true}},"config":{},"frames":null});
</script></div>
  <script>
    function my_plot_div_updatePlot(value) {
      var plots = document.getElementsByClassName('rule-plot');
      for (var i = 0; i < plots.length; i++) {
        plots[i].style.display = 'none';
      }
      var selectedPlot = document.getElementById('my_plot_div_plot_' + value);
      if (selectedPlot) {
        selectedPlot.style.display = 'block';
      }
    }
  </script>
</div>"#
        );
    }
}
