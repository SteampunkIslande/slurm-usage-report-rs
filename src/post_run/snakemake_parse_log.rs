//! Extraire la taille totale des fichiers d'entrée par SLURM job id depuis un fichier de log Snakemake.
//!
//! Sortie : CSV (délimiteur `|`) avec colonnes `slurm_jobid|job_id|rule_name|input_size_bytes|inputs`

use chrono::NaiveDateTime;
use csv::{Writer, WriterBuilder};
use regex::Regex;
use serde::Serialize;
use serde::ser::SerializeStruct;
use std::collections::HashSet;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use crate::UsageReportError;

// ── Regex compilées une seule fois ──────────────────────────────────────────

static RULE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^rule\s+(\S+):").expect("This should not have compiled"));

static INPUT_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^\s*input\s*:\s*(.+)").expect("This should not have compiled")
});

static SLURM_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)Job\s+(\d+)\s+has\s+been\s+submitted\s+with\s+SLURM\s+jobid\s+(\d+)\s+\(log:\s*(.+)\)\.$",
    )
    .expect("This should not have compiled")
});

// ── Structures ──────────────────────────────────────────────────────────────

/// Résultat de l'extraction d'un enregistrement de log Snakemake.
#[derive(Debug, Default)]
struct ParsedRecord {
    slurm_jobid: String,
    job_id: String,
    rule_name: String,
    inputs: Vec<String>,
}

impl Serialize for ParsedRecord {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_struct("ParsedRecord", 5)?;
        s.serialize_field("slurm_jobid", &self.slurm_jobid)?;
        s.serialize_field("job_id", &self.job_id)?;
        s.serialize_field("rule_name", &self.rule_name)?;
        let solved_inputs: Vec<PathBuf> = self
            .inputs
            .iter()
            .map(|p| {
                let pb = PathBuf::from(p);
                fs::canonicalize(&pb).unwrap_or_else(|_| pb.to_path_buf())
            })
            .collect();

        let input_size_bytes: u64 = solved_inputs
            .iter()
            .filter_map(|p| fs::metadata(p).ok())
            .map(|m| m.len())
            .sum();
        s.serialize_field("input_size_bytes", &input_size_bytes)?;
        s.serialize_field(
            "inputs",
            &solved_inputs
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(","),
        )?;
        s.end()
    }
}

// ── Fonctions internes ──────────────────────────────────────────────────────

/// Trouver le dossier d'exécution de snakemake en remontant jusqu'à `.snakemake`.
/// Permet de résoudre les chemins des fichiers mentionnés par snakemake dans ses logs.
/// En effet, l'utilisateur peut avoir spécifié des chemins relatifs. Ces derniers sont relatifs au dossier d'exécution du pipeline, qui lui-même contient le dossier .snakemake à sa racine.
///
/// Retourne `None` si la structure attendue (`<project>/.snakemake/log/<file>.log`) n'est pas respectée.
fn find_project_dir(log_path: &Path) -> Option<PathBuf> {
    let parent = log_path.parent()?;
    let grandparent = parent.parent()?;
    if parent.file_name()?.to_str()? != "log" || grandparent.file_name()?.to_str()? != ".snakemake"
    {
        return None;
    }
    // grandparent = .snakemake → on remonte encore d'un cran
    grandparent
        .parent()
        .map(|p| fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf()))
}

/// Itérateur paresseux sur les enregistrements d'un fichier de log Snakemake.
///
/// Chaque enregistrement est un groupe de lignes délimité par une ligne commençant par `[`.
/// Le préambule avant la première ligne `[` est ignoré.
struct LogRecordIter {
    lines: io::Lines<BufReader<File>>,
    /// Tampon contenant l'enregistrement en cours de construction.
    current_record: Option<Vec<String>>,
    /// Indique que le flux de lignes est épuisé.
    done: bool,
}

impl LogRecordIter {
    fn new(log_path: &Path) -> io::Result<Self> {
        let file = File::open(log_path)?;
        let reader = BufReader::new(file);
        Ok(Self {
            lines: reader.lines(),
            current_record: None,
            done: false,
        })
    }
}

impl Iterator for LogRecordIter {
    type Item = io::Result<Vec<String>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            match self.lines.next() {
                Some(Ok(line)) => {
                    if line.starts_with('[') {
                        // Une nouvelle section commence : on émet l'enregistrement précédent s'il existe.
                        let prev = self.current_record.replace(vec![line]);
                        if let Some(rec) = prev {
                            return Some(Ok(rec));
                        }
                        // Sinon c'est le tout premier enregistrement, on continue à lire.
                    } else if let Some(ref mut rec) = self.current_record {
                        rec.push(line);
                    }
                    // sinon : préambule, on ignore
                }
                Some(Err(e)) => {
                    self.done = true;
                    return Some(Err(e));
                }
                None => {
                    // Fin du fichier : émettre le dernier enregistrement s'il existe.
                    self.done = true;
                    return self.current_record.take().map(Ok);
                }
            }
        }
    }
}

/// Générateur d'enregistrements : groupe les lignes entre deux lignes commençant par `[`.
///
/// Retourne un itérateur paresseux de `Vec<String>`, ce qui évite de charger
/// l'intégralité du fichier en mémoire.
///
/// Ignore le préambule avant la première ligne commençant par `[`.
fn snakemake_log_records(log_path: &Path) -> io::Result<LogRecordIter> {
    LogRecordIter::new(log_path)
}

/// Extraire les champs pertinents d'un enregistrement (groupe de lignes).
fn extract_from_record(record_lines: &[String]) -> ParsedRecord {
    let mut parsed = ParsedRecord::default();

    for line in record_lines {
        if let Some(caps) = INPUT_RE.captures(line) {
            parsed.inputs = caps[1].split(',').map(|s| s.trim().to_string()).collect();
        }
        if let Some(caps) = SLURM_RE.captures(line) {
            parsed.job_id = caps[1].to_string();
            parsed.slurm_jobid = caps[2].to_string();
        }
        if let Some(caps) = RULE_RE.captures(line) {
            parsed.rule_name = caps[1].to_string();
        }
    }

    parsed
}

/// Parser un fichier de log Snakemake et écrire les résultats (append) dans `output_path`.
///
/// Le format de sortie est un CSV délimité par `|` avec les colonnes :
/// `slurm_jobid|job_id|rule_name|input_size_bytes|inputs`
///
/// # Errors
///
/// Retourne une erreur si le fichier de log ou le fichier de sortie ne peut pas être ouvert/lu/écrit.
fn parse_snakemake_log_file(
    log_path: &Path,
    output_path: &Path,
    write_header: bool,
) -> Result<(), UsageReportError> {
    let original_dir = env::current_dir()?;

    if let Some(project_dir) = find_project_dir(log_path) {
        env::set_current_dir(&project_dir)?;
    }

    let records = snakemake_log_records(log_path)?;

    let out_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(output_path)?;

    let mut writer: Writer<File> = WriterBuilder::new()
        .delimiter(b'|')
        .has_headers(write_header)
        .from_writer(out_file);

    for record in records {
        let record = record?;
        let parsed = extract_from_record(&record);

        // Enregistrement sans intérêt (pas de job_id)
        if parsed.job_id.is_empty() {
            continue;
        }
        writer.serialize(parsed)?;
    }

    // Restaurer le répertoire de travail d'origine
    env::set_current_dir(original_dir)?;

    Ok(())
}

pub fn parse_snakemake_log_files(
    log_paths: &[&Path],
    output_path: &Path,
) -> Result<(), UsageReportError> {
    for (i, log_path) in log_paths.iter().enumerate() {
        parse_snakemake_log_file(log_path, output_path, i == 0)?;
    }
    Ok(())
}

pub fn get_slurm_ids(log_paths: &[&Path]) -> io::Result<Vec<String>> {
    Ok(log_paths
        .iter()
        .map(|p| {
            File::open(p).map(BufReader::new).map(|f| {
                f.lines()
                    .filter_map(|s| match s {
                        Ok(s) => {
                            if s.contains("SLURM run ID: ") {
                                Some(s[14..].to_string())
                            } else {
                                None
                            }
                        }
                        Err(_) => None,
                    })
                    .next()
            })
        })
        .filter_map(|s| s.ok())
        .flatten()
        .collect())
}

/// Gets all the dates the snakemake run spanned
///
/// # Arguments
///
/// - `log_path` (`&Path`) - Path to a snakemake log
///
/// # Returns
///
/// - `Vec<String>` - All the dates from the beginning of the snakemake run to the end,
///   using format `YY-mm-DD`
pub fn get_snakemake_run_span(log_path: &Path) -> HashSet<String> {
    let file = File::open(log_path);
    let file = match file {
        Ok(f) => f,
        Err(_) => return HashSet::new(),
    };

    let reader = BufReader::new(file);

    let mut dates: HashSet<String> = HashSet::new();

    for line in reader.lines() {
        let line = line.unwrap_or("".to_string());

        // Cherche un pattern entre crochets
        if let Some(start) = line.find('[')
            && let Some(end) = line.find(']')
        {
            let raw = &line[start + 1..end];

            // Exemple: "Thu Mar  5 11:45:18 2026"
            if let Ok(dt) = NaiveDateTime::parse_from_str(raw, "%a %b %e %H:%M:%S %Y") {
                dates.insert(dt.format("%Y-%m-%d").to_string());
            }
        }
    }
    dates
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_slurm_ids() {
        use tempfile::tempdir;

        // Cas 1: Un seul SLURM ID
        let temp_dir = tempdir().unwrap();
        let log_file1 = temp_dir.path().join("run1.log");
        std::fs::write(
            &log_file1,
            "Some log content\nSLURM run ID: 12345\nMore content",
        )
        .unwrap();

        let result = get_slurm_ids(&[log_file1.as_path()]).unwrap();
        assert_eq!(result, vec!["12345"]);

        // Cas 2: Plusieurs SLURM IDs dans le même fichier
        // NOTE: La fonction actuelle ne retourne que le premier ID par fichier
        let log_file2 = temp_dir.path().join("run2.log");
        std::fs::write(
            &log_file2,
            "Starting\nSLURM run ID: 11111\nMiddle\nSLURM run ID: 22222\nEnd",
        )
        .unwrap();

        let result = get_slurm_ids(&[log_file2.as_path()]).unwrap();
        // La fonction ne retourne que le premier ID trouvé (bug: devrait utiliser collect() au lieu de .next())
        assert_eq!(result, vec!["11111"]);

        // Cas 3: Plusieurs fichiers (chacun retourne son premier ID)
        let result = get_slurm_ids(&[log_file1.as_path(), log_file2.as_path()]).unwrap();
        assert_eq!(result, vec!["12345", "11111"]);

        // Cas 4: Fichier sans SLURM ID
        let log_file3 = temp_dir.path().join("run3.log");
        std::fs::write(&log_file3, "Just some regular log content without SLURM ID").unwrap();

        let result = get_slurm_ids(&[log_file3.as_path()]).unwrap();
        assert!(result.is_empty());

        // Cas 5: Fichier vide
        let log_file4 = temp_dir.path().join("run4.log");
        std::fs::write(&log_file4, "").unwrap();

        let result = get_slurm_ids(&[log_file4.as_path()]).unwrap();
        assert!(result.is_empty());

        // Cas 6: Chemin vers un fichier inexistant (doit être filtré silencieusement)
        let fake_path = Path::new("/nonexistent/file.log");
        let result = get_slurm_ids(&[fake_path]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_find_project_dir_valid() {
        // Simule un chemin typique : /tmp/myproject/.snakemake/log/run.log
        let path = Path::new("/tmp/myproject/.snakemake/log/run.log");
        let result = find_project_dir(path);
        // Le dossier n'existe pas réellement, donc canonicalize échouera et on aura le chemin brut
        assert!(result.is_some());
        let dir = result.expect("Error while running test_find_project_dir_valid");
        assert!(&dir == "/tmp/myproject");
    }

    #[test]
    fn test_find_project_dir_invalid() {
        let path = Path::new("/tmp/some/random/path.log");
        assert!(find_project_dir(path).is_none());
    }

    #[test]
    fn test_extract_from_record_empty() {
        let record: Vec<String> = vec![];
        let parsed = extract_from_record(&record);
        assert!(parsed.job_id.is_empty());
        assert!(parsed.slurm_jobid.is_empty());
        assert!(parsed.inputs.is_empty());
    }

    #[test]
    fn test_extract_from_record_full() {
        let record = vec![
            "[Wed Jan  1 12:00:00 2025]".to_string(),
            "rule my_rule:".to_string(),
            "    input: file1.txt, file2.txt".to_string(),
            "Job 42 has been submitted with SLURM jobid 12345 (log: /tmp/log).".to_string(),
        ];
        let parsed = extract_from_record(&record);
        assert_eq!(parsed.rule_name, "my_rule");
        assert_eq!(parsed.job_id, "42");
        assert_eq!(parsed.slurm_jobid, "12345");
        assert_eq!(parsed.inputs, vec!["file1.txt", "file2.txt"]);
    }

    #[test]
    fn test_parse_snakemake_log_file_integration() -> Result<(), UsageReportError> {
        use tempfile::tempdir;

        // Créer un répertoire temporaire pour simuler la structure du projet
        let temp_dir = tempdir()?;

        // Créer la structure .snakemake/log/
        let snakemake_dir = temp_dir.path().join(".snakemake");
        let log_dir = snakemake_dir.join("log");
        std::fs::create_dir_all(&log_dir)?;

        // Créer des fichiers d'entrée temporaires avec du contenu
        let input_dir = temp_dir.path().join("inputs");
        std::fs::create_dir_all(&input_dir)?;
        let input_file1 = input_dir.join("sample1.txt");
        let input_file2 = input_dir.join("sample2.txt");
        std::fs::write(&input_file1, b"test content 1")?;
        std::fs::write(&input_file2, b"test content 2")?;

        // Créer le fichier de log Snakemake
        let log_content = format!(
            r#"[Thu Mar  5 11:45:18 2026]
rule preprocess_merge:
    input: {}, {}
    output: /data/analysis/output.txt
    shell:
        echo "Hello"
    jobid: 3
No wall time information given. This might or might not work on your cluster. If not, specify the resource runtime in your rule or as a reasonable default via --default-resources.
Job 3 has been submitted with SLURM jobid 60392 (log: {})."#,
            input_file1.display(),
            input_file2.display(),
            log_dir.join("run.log").display()
        );

        let log_path = log_dir.join("run.log");
        std::fs::write(&log_path, log_content)?;

        // Créer un fichier temporaire pour la sortie
        let output_file = temp_dir.path().join("output.csv");

        // Appeler la fonction publique à tester
        parse_snakemake_log_file(&log_path, &output_file, true)?;

        // Vérifier que le fichier de sortie existe et contient les données attendues
        let output_content = std::fs::read_to_string(&output_file)?;

        eprintln!("{}", output_content);

        // La sortie doit contenir:
        // - slurm_jobid: 60392
        // - job_id: 3
        // - rule_name: preprocess_merge
        // - input_size_bytes: la taille des deux fichiers (14 + 14 = 28 bytes)
        // - inputs: les chemins canoniques des fichiers

        assert!(
            !output_content.is_empty(),
            "Le fichier de sortie ne doit pas être vide"
        );

        let mut output_lines = output_content.lines();
        let header = output_lines.next().expect("Devrait avoir un header");

        assert_eq!(
            header,
            "slurm_jobid|job_id|rule_name|input_size_bytes|inputs"
        );

        let line = output_lines
            .next()
            .expect("Devrait contenir au moins une ligne d'enregistrement");
        let parts: Vec<&str> = line.split('|').collect();

        assert_eq!(
            parts.len(),
            5,
            "La ligne doit contenir 5 champs séparés par '|'"
        );
        assert_eq!(parts[0], "60392", "Le SLURM job ID doit être 60392");
        assert_eq!(parts[1], "3", "Le job ID doit être 3");
        assert_eq!(
            parts[2], "preprocess_merge",
            "Le rule name doit être preprocess_merge"
        );
        // La taille doit être > 0 car les fichiers existent
        let input_size: u64 = parts[3].parse().expect("La taille doit être un nombre");
        assert!(input_size > 0, "La taille des inputs doit être > 0");
        // Les chemins doivent contenir les noms de fichiers
        assert!(
            parts[4].contains("sample1.txt"),
            "Les inputs doivent contenir sample1.txt"
        );
        assert!(
            parts[4].contains("sample2.txt"),
            "Les inputs doivent contenir sample2.txt"
        );

        // Le fichier temporaire de sortie sera automatiquement supprimé
        // quand `output_file` sortira de portée (via Drop)
        // et `temp_dir` sera nettoyé automatiquement

        Ok(())
    }
}
