//! Extraire la taille totale des fichiers d'entrée par SLURM job id depuis un fichier de log Snakemake.
//!
//! Sortie : CSV (délimiteur `|`) avec colonnes `slurm_jobid|job_id|rule_name|input_size_bytes|inputs`

use regex::Regex;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

// ── Regex compilées une seule fois ──────────────────────────────────────────

static RULE_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^rule\s+(\S+):").unwrap());

static INPUT_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)^\s*input\s*:\s*(.+)").unwrap());

static SLURM_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)Job\s+(\d+)\s+has\s+been\s+submitted\s+with\s+SLURM\s+jobid\s+(\d+)\s+\(log:\s*(.+)\)\.$",
    )
    .unwrap()
});

// ── Structures ──────────────────────────────────────────────────────────────

/// Résultat de l'extraction d'un enregistrement de log Snakemake.
#[derive(Debug, Default)]
struct ParsedRecord {
    inputs: Vec<String>,
    slurm_id: String,
    rule_name: String,
    job_id: String,
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
            parsed.slurm_id = caps[2].to_string();
        }
        if let Some(caps) = RULE_RE.captures(line) {
            parsed.rule_name = caps[1].to_string();
        }
    }

    parsed
}

// ── Fonction publique ───────────────────────────────────────────────────────

/// Parser un fichier de log Snakemake et écrire les résultats (append) dans `output_path`.
///
/// Le format de sortie est un CSV délimité par `|` avec les colonnes :
/// `slurm_jobid|job_id|rule_name|input_size_bytes|inputs`
///
/// # Errors
///
/// Retourne une erreur si le fichier de log ou le fichier de sortie ne peut pas être ouvert/lu/écrit.
pub fn parse_snakemake_log_file(log_path: &Path, output_path: &Path) -> io::Result<()> {
    let original_dir = env::current_dir()?;

    if let Some(project_dir) = find_project_dir(log_path) {
        env::set_current_dir(&project_dir)?;
    }

    let records = snakemake_log_records(log_path)?;

    let mut out_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(output_path)?;

    for record in records {
        let record = record?;
        let parsed = extract_from_record(&record);

        // Enregistrement sans intérêt (pas de job_id)
        if parsed.job_id.is_empty() {
            continue;
        }

        let solved_inputs: Vec<PathBuf> = parsed
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

        let inputs_str = solved_inputs
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(",");

        writeln!(
            out_file,
            "{}|{}|{}|{}|{}",
            parsed.slurm_id, parsed.job_id, parsed.rule_name, input_size_bytes, inputs_str
        )?;
    }

    // Restaurer le répertoire de travail d'origine
    env::set_current_dir(original_dir)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_project_dir_valid() {
        // Simule un chemin typique : /tmp/myproject/.snakemake/log/run.log
        let path = Path::new("/tmp/myproject/.snakemake/log/run.log");
        let result = find_project_dir(path);
        // Le dossier n'existe pas réellement, donc canonicalize échouera et on aura le chemin brut
        assert!(result.is_some());
        let dir = result.unwrap();
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
        assert!(parsed.slurm_id.is_empty());
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
        assert_eq!(parsed.slurm_id, "12345");
        assert_eq!(parsed.inputs, vec!["file1.txt", "file2.txt"]);
    }
}
