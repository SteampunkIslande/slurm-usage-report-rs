import duckdb as db
from copy import deepcopy
import yaml
from IPython.display import display
from ipywidgets import widgets
import os


class OptimizerUI:
    def __init__(self, config_file_path="config.yaml"):
        if os.path.isfile("config.yaml"):
            with open("config.yaml") as f:
                self.unoptimized_all: dict = yaml.safe_load(f)
        else:
            self.unoptimized_all: dict = {}

        self.unoptimized_resources: dict = self.unoptimized_all.get("set-resources", {})
        self.all_rules: list[str] = [
            i["rule_name"]
            for i in db.sql("SELECT DISTINCT rule_name FROM 'usage-report.parquet'")
            .pl()
            .to_dicts()
        ]

        # état interne
        self.optimized_resources: dict[str, dict] = deepcopy(self.unoptimized_resources)
        self.resource_inputs: dict[str, widgets.Text] = {}
        self.current_rule: str = None

        self.EDITABLE_RESOURCES = {
            "mem_mb": {"label": "Mémoire (MB)"},
            "runtime": {"label": "Durée d'exécution maximale (minutes)"},
            "threads": {"label": "Nombre de threads"},
        }

        # Barre de recherche
        self.search: widgets.Text = widgets.Text(
            placeholder="Rechercher une règle...", layout=widgets.Layout(width="100%")
        )
        self.search.observe(self.on_search_change, names="value")

        # Sélection d'une règle à optimiser
        self.rule_selection: widgets.Select = widgets.Select(
            options=[], layout=widgets.Layout(width="100%", height="300px")
        )

        self.rule_selection.observe(self.on_rule_change, names="value")

        # Conteneur
        self.form_container: widgets.VBox = widgets.VBox(
            layout=widgets.Layout(width="100%")
        )

        self.update_rule_list()

        if self.all_rules:
            self.build_form(self.all_rules[0])

    def format_rule_label(self, rule):
        return (
            f"{rule} (optimisée)"
            if self.is_optimized(rule)
            else f"{rule} (à optimiser)"
        )

    def is_optimized(self, rule):
        # Une règle n'est pas optimisée si elle n'est pas dans optimized_resources
        if rule not in self.optimized_resources:
            return False

        # Si la règle n'était pas dans unoptimized_resources à l'origine, elle est optimisée
        if rule not in self.unoptimized_resources:
            return True

        # Si optimized_resources[rule] est vide (aucune clé), alors elle n'est pas optimisée
        if not self.optimized_resources[rule]:
            return False

        # Compare les valeurs - différent = optimisé
        return self.optimized_resources[rule] != self.unoptimized_resources[rule]

    def update_rule_list(self, filter_text=""):
        current_value = self.rule_selection.value

        filtered = [r for r in self.all_rules if filter_text.lower() in r.lower()]
        options = [(self.format_rule_label(r), r) for r in filtered]

        self.rule_selection.options = options  # toujours setter

        if current_value in filtered:
            self.rule_selection.value = current_value
        elif filtered:
            self.rule_selection.value = filtered[0]
        else:
            self.rule_selection.value = None

    def get_rule_metrics(self, rule_name):
        return db.sql(f"""
            SELECT 
                max(CPUEfficiencyPercent) AS cpu_max,
                max(MaxRSS_G) AS max_mem,
                max(MemEfficiencyPercent) AS max_mem_ratio_percent,
                max(ElapsedRaw)/60 AS max_runtime_minutes
            FROM 'usage-report.parquet'
            WHERE rule_name='{rule_name}'
            GROUP BY ALL
        """).pl().to_dicts()[0]

    def serialize_str_to_yaml(self, value: str) -> int | float | str:
        try:
            value = int(value)
        except ValueError:
            try:
                value = float(value)
            except ValueError:
                value = value

        return value

    def save_current_rule(self):
        if self.current_rule is None:
            return

        updated = {
            k: self.serialize_str_to_yaml(w.value)
            for k, w in self.resource_inputs.items()
        }
        # Filtrer les valeurs vides (ne pas les inclure dans optimized_resources)
        updated = {k: v for k, v in updated.items() if v != ""}

        # Initialiser si nécessaire
        if self.current_rule not in self.optimized_resources:
            self.optimized_resources[self.current_rule] = dict()

        # Si updated est vide et que la règle existait dans optimized_resources, on la supprime
        if not updated:
            if self.current_rule in self.optimized_resources:
                del self.optimized_resources[self.current_rule]
        else:
            # Sinon, mettre à jour avec les nouvelles valeurs
            self.optimized_resources[self.current_rule] = updated

        self.update_rule_list(self.search.value)

    def on_rule_change(self, change):
        if change["name"] != "value":
            return

        self.save_current_rule()
        self.build_form(change["new"])

    def on_search_change(self, change):
        self.update_rule_list(change["new"])

    def build_form(self, rule_name):
        self.current_rule = rule_name

        stats = self.get_rule_metrics(rule_name)
        current_resources = self.optimized_resources.get(rule_name, {})
        original_resources = self.unoptimized_resources.get(rule_name, {})

        widgets_list: list[widgets.Widget] = []

        widgets_list.append(
            widgets.Label(
                "Métriques avant optimisation",
                layout=widgets.Layout(width="100%", font_weight="bold"),
            )
        )

        metrics_box: widgets.VBox = widgets.VBox(
            layout=widgets.Layout(width="100%", border="1px solid #ccc", padding="10px")
        )
        metrics_children = []

        metrics_children.append(
            widgets.HBox(
                [
                    widgets.Label(
                        "Utilisation CPU max (%)",
                        layout=widgets.Layout(width="40%", font_weight="bold"),
                    ),
                    widgets.Label(
                        f"{stats['cpu_max']:.2f} %", layout=widgets.Layout(width="60%")
                    ),
                ]
            )
        )
        metrics_children.append(
            widgets.HBox(
                [
                    widgets.Label(
                        "Utilisation mémoire max (Go)",
                        layout=widgets.Layout(width="40%", font_weight="bold"),
                    ),
                    widgets.Label(
                        f"{stats['max_mem']:.2f} Go", layout=widgets.Layout(width="60%")
                    ),
                ]
            )
        )
        metrics_children.append(
            widgets.HBox(
                [
                    widgets.Label(
                        "Taux d'utilisation mémoire max (%)",
                        layout=widgets.Layout(width="40%", font_weight="bold"),
                    ),
                    widgets.Label(
                        f"{stats['max_mem_ratio_percent']:.2f} %",
                        layout=widgets.Layout(width="60%"),
                    ),
                ]
            )
        )
        metrics_children.append(
            widgets.HBox(
                [
                    widgets.Label(
                        "Durée max (minutes)",
                        layout=widgets.Layout(width="40%", font_weight="bold"),
                    ),
                    widgets.Label(
                        f"{stats['max_runtime_minutes']:.2f} min",
                        layout=widgets.Layout(width="60%"),
                    ),
                ]
            )
        )

        metrics_box.children = metrics_children
        widgets_list.append(metrics_box)

        widgets_list.append(
            widgets.Label(
                "Paramètres originaux (set-resources)",
                layout=widgets.Layout(width="100%", font_weight="bold"),
            )
        )
        orig_box: widgets.VBox = widgets.VBox(
            layout=widgets.Layout(width="100%", border="1px solid #ccc", padding="10px")
        )
        orig_children = []

        if original_resources:
            for key, meta in self.EDITABLE_RESOURCES.items():
                value = original_resources.get(key, "(non défini)")
                orig_children.append(
                    widgets.HBox(
                        [
                            widgets.Label(
                                meta["label"],
                                layout=widgets.Layout(width="40%", font_weight="bold"),
                            ),
                            widgets.Label(
                                str(value),
                                layout=widgets.Layout(width="60%", font_style="italic"),
                            ),
                        ]
                    )
                )
        else:
            orig_children.append(
                widgets.Label(
                    "Aucun paramètre n'était défini à l'origine pour cette règle",
                    layout=widgets.Layout(fg_color="#ffff00"),
                )
            )

        orig_box.children = orig_children
        widgets_list.append(orig_box)

        widgets_list.append(widgets.Label("", layout=widgets.Layout(height="20px")))
        widgets_list.append(
            widgets.Label(
                "Modifier les paramètres (set-resources)",
                layout=widgets.Layout(width="100%", font_weight="bold"),
            )
        )
        widgets_list.append(
            widgets.Label(
                "Saisissez les nouvelles valeurs (laissez vide pour supprimer) :",
                layout=widgets.Layout(width="100%"),
            )
        )

        self.resource_inputs.clear()

        for key, meta in self.EDITABLE_RESOURCES.items():
            value = current_resources.get(key)
            if value is None:
                value = ""
            else:
                value = str(value)

            input_widget = widgets.Text(value=value, layout=widgets.Layout(width="60%"))
            label = widgets.Label(meta["label"], layout=widgets.Layout(width="40%"))
            row = widgets.HBox([label, input_widget])

            self.resource_inputs[key] = input_widget
            widgets_list.append(row)

        self.form_container.children = widgets_list

    def export_yaml(self, path="set-resources.yaml"):
        self.save_current_rule()
        with open(path, "w") as f:
            yaml.dump({"set-resources": self.optimized_resources}, f)

        print(f"Définition de `set-resources` exportée vers {path}")

    def display(self):
        display(self.search)
        display(self.rule_selection)
        display(self.form_container)
