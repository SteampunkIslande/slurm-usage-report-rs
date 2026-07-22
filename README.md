# Utilisation

Ce programme propose plusieurs sous-commandes, listées ci-dessous.

```
Usage: slurm-usage-report-rs [OPTIONS] <COMMAND>

Commands:
  post-run-cmd      Programme post-run pour générer un rapport d'usage Slurm à partir des logs de Snakemake.
  autocomplete      Générer l'autocomplétion
  csv-to-parquet    Convertir un fichier CSV en parquet
  daily-efficiency  Collecte les métriques d'utilisation quotidiennes du cluster.
  usage-aggregate   Aggrège les données quotidiennes d'utilisation du cluster.
  help              Print this message or the help of the given subcommand(s)

Options:
  -v, --verbose  Set verbose (ON/OFF)
  -h, --help     Print help
```

## `post-run-cmd`

```
Programme post-run pour générer un rapport d'usage Slurm à partir des logs de Snakemake.

Possibilité de spécifier plusieurs fichiers de log (ex: .snakemake/log/xxx.log) pour consolider les métriques
à partir de plusieurs runs de Snakemake mais avec le même pipeline.

Usage: slurm-usage-report-rs post-run-cmd [OPTIONS]

Options:
  -i, --input <INPUT>...
          Chemin vers le(s) fichier(s) de log snakemake

  -o, --output-dir <OUTPUT_DIR>
          Chemin vers le dossier

      --no-input-size-relative-metrics
          Ne pas ajouter les métriques relatives aux tailles des fichiers d'entrée

      --no-tabs
          

  -d, --db <DB>
          Chemin vers la base de données SACCT maison du cluster (dossier avec les fichiers parquet).
          
          Permet de contourner sacct en cherchant directement les données dans les fichiers parquet

  -f, --force
          Force l'écriture du dossier de sortie si ce dernier existe déjà.
          
          Attention, cela supprime le dossier déjà existant avant même de démarrer.

  -h, --help
          Print help (see a summary with '-h')
```

## `autocomplete`

```
Générer l'autocomplétion

Usage: slurm-usage-report-rs autocomplete [OPTIONS] --output <OUTPUT> --shell <SHELL>

Options:
  -o, --output <OUTPUT>  Nom du fichier d'autocomplétion
  -s, --shell <SHELL>    Type de shell [possible values: bash, elvish, fish, powershell, zsh]
  -f, --force            Force overwrite
  -h, --help             Print help
```

## `csv-to-parquet`

Ne pas utiliser. Raccourci pour passer du CSV généré par `sacct` au parquet. Suppose un schéma exact, un nombre de colonnes fixes... Peu ergonomique et à l'utilité limitée.

Testé seulement avec SLURM 22.05, utilisez à vos risques et périls.

## `daily-efficiency`

Génère un rapport d'efficacité du cluster. Pour l'instant, la CLI ne permet pas de passer les caractéristiques du cluster. La valeur par défaut utilisée correspond aux capacités du cluster sur lequel cet outil a été développé.

```
Collecte les métriques d'utilisation quotidiennes du cluster.

Produit un rapport quotidien avec les principales métriques d'intérêt pour évaluer la charge de travail sur le cluster.

Usage: slurm-usage-report-rs daily-efficiency --date <DATE> --database <DATABASE> --html-output <HTML_OUTPUT> --json-output <JSON_OUTPUT>

Options:
      --date <DATE>
          Date du rapport au format YYYY-MM-DD

      --database <DATABASE>
          Chemin vers la base de données SACCT (collection de fichiers parquet)

      --html-output <HTML_OUTPUT>
          

      --json-output <JSON_OUTPUT>
          

  -h, --help
          Print help (see a summary with '-h')
```

## `usage-aggregate`

A partir des données d'utilisation quotidiennes du cluster, permet de suivre l'évolution temporelle de l'utilisation du cluster (% de la RAM totale disponible réellement utilisée, et % du temps CPU disponible réellement utilisé).
Cette commande utilise la base de données générée par la commande `daily-efficiency`.

```
Aggrège les données quotidiennes d'utilisation du cluster.

Produit un rapport montrant l'évolution de l'utilisation du cluster, par jour (aggrège quelques métriques sur une fenêtre de temps).

Usage: slurm-usage-report-rs usage-aggregate --from <FROM> --to <TO> --database <DATABASE> --output <OUTPUT>

Options:
  -f, --from <FROM>
          Date du début de l'aggrégat au format YYYY-MM-DD

  -t, --to <TO>
          Date de fin de l'aggrégat au format YYYY-MM-DD

  -d, --database <DATABASE>
          Chemin vers la base de données SACCT (collection de fichiers parquet)

  -o, --output <OUTPUT>
          

  -h, --help
          Print help (see a summary with '-h')
```
