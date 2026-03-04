use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Datelike, Duration, FixedOffset, NaiveDate, TimeZone, Utc};
use std::io::IsTerminal;
use clap::{Parser, Subcommand};
use colored::Colorize;
use fsrs::{DEFAULT_PARAMETERS, MemoryState, FSRS};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const DESIRED_RETENTION: f32 = 0.9;
const TRACKER_FILE: &str = "GARP RAI Quiz Tracker.md";
const STATE_FILE: &str = ".garp-fsrs-state.json";
const DRILLS_FILE: &str = "GARP RAI Definition Drills.md";

const MODE_THRESHOLDS: &[(f64, &str)] = &[(0.60, "drill"), (0.70, "free-recall"), (1.01, "MCQ")];

const GARP_RAI_SYLLABUS: &[&str] = &[
    "M1-ai-risks",
    "M1-classical-ai",
    "M1-ml-types",
    "M2-clustering",
    "M2-data-prep",
    "M2-econometric",
    "M2-intro-tools",
    "M2-model-estimation",
    "M2-model-eval",
    "M2-neural-networks",
    "M2-nlp-genai",
    "M2-nlp-traditional",
    "M2-regression-classification",
    "M2-semi-rl",
    "M2-semi-supervised",
    "M3-autonomy-safety",
    "M3-bias-unfairness",
    "M3-fairness-measures",
    "M3-genai-risks",
    "M3-global-challenges",
    "M3-reputational-existential",
    "M3-xai",
    "M4-bias-discrimination",
    "M4-ethical-frameworks",
    "M4-ethics-principles",
    "M4-governance-challenges",
    "M4-privacy-cybersecurity",
    "M4-regulatory",
    "M5-data-governance",
    "M5-genai-governance",
    "M5-governance-recommendations",
    "M5-implementation",
    "M5-model-changes-review",
    "M5-model-dev-testing",
    "M5-model-governance",
    "M5-model-risk-roles",
    "M5-model-validation",
];

#[derive(Parser, Debug)]
#[command(name = "melete", about = "GARP RAI spaced repetition")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    Session { n: Option<usize> },
    Record { topic: String, rating: String },
    End,
    Today,
    Stats,
    Topics,
    Due,
    Coverage,
    Reconcile,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct ReviewEntry {
    topic: String,
    rating: String,
    date: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PyCard {
    card_id: i64,
    state: i32,
    step: Option<i32>,
    stability: f64,
    difficulty: f64,
    due: String,
    last_review: String,
}

#[derive(Clone, Debug, Default)]
struct State {
    cards: HashMap<String, PyCard>,
    review_log: Vec<ReviewEntry>,
}

#[derive(Clone, Debug, Deserialize)]
struct RawState {
    #[serde(default)]
    cards: HashMap<String, Value>,
    #[serde(default)]
    review_log: Vec<ReviewEntry>,
}

#[derive(Clone, Debug, Default)]
struct TopicInfo {
    attempts: i32,
    correct: i32,
    rate: f64,
}

#[derive(Clone, Debug, Default)]
struct SummaryInfo {
    total: i32,
    correct: i32,
    rate: i32,
    sessions: i32,
}

#[derive(Clone, Debug, Default)]
struct MissInfo {
    date: String,
    topic: String,
    concept: String,
}

#[derive(Clone, Debug, Default)]
struct Tracker {
    summary: SummaryInfo,
    topics: HashMap<String, TopicInfo>,
    recent_misses: Vec<MissInfo>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RatingKind {
    Again,
    Hard,
    Good,
    Easy,
}

impl RatingKind {
    fn fsrs_value(self) -> u32 {
        match self {
            RatingKind::Again => 1,
            RatingKind::Hard => 2,
            RatingKind::Good => 3,
            RatingKind::Easy => 4,
        }
    }

    fn result_str(self) -> &'static str {
        match self {
            RatingKind::Again => "MISS",
            RatingKind::Hard => "OK-GUESS",
            RatingKind::Good | RatingKind::Easy => "OK",
        }
    }

    fn display_str(self) -> &'static str {
        match self {
            RatingKind::Again => "Again (miss)",
            RatingKind::Hard => "Hard (guess)",
            RatingKind::Good => "Good",
            RatingKind::Easy => "Easy",
        }
    }

    fn log_name(self) -> &'static str {
        match self {
            RatingKind::Again => "again",
            RatingKind::Hard => "hard",
            RatingKind::Good => "good",
            RatingKind::Easy => "easy",
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Some(Command::Session { n }) => cmd_session(n)?,
        Some(Command::Record { topic, rating }) => cmd_record(&topic, &rating)?,
        Some(Command::End) => cmd_end_session()?,
        Some(Command::Today) => cmd_today()?,
        Some(Command::Stats) => cmd_stats()?,
        Some(Command::Topics) => cmd_topics()?,
        Some(Command::Due) => cmd_due()?,
        Some(Command::Coverage) => cmd_coverage()?,
        Some(Command::Reconcile) => cmd_reconcile()?,
        None => print_help(),
    }
    Ok(())
}

fn print_help() {
    println!();
    println!("{}", "melete — GARP RAI spaced repetition".bold());
    println!();
    println!("  {} [N]", "session".cyan());
    println!("  {} TOPIC RATING", "record".cyan());
    println!("  {}", "end".cyan());
    println!("  {}", "today".cyan());
    println!("  {}", "stats".cyan());
    println!("  {}", "topics".cyan());
    println!("  {}", "due".cyan());
    println!("  {}", "coverage".cyan());
    println!("  {}", "reconcile".cyan());
    println!();
}

fn hkt() -> FixedOffset {
    FixedOffset::east_opt(8 * 3600).expect("valid HKT")
}

fn now_hkt() -> DateTime<FixedOffset> {
    Utc::now().with_timezone(&hkt())
}

fn today_hkt() -> NaiveDate {
    now_hkt().date_naive()
}

fn exam_date_hkt() -> DateTime<FixedOffset> {
    hkt().with_ymd_and_hms(2026, 4, 4, 10, 45, 0).unwrap()
}

fn days_until_exam() -> i64 {
    exam_date_hkt().signed_duration_since(now_hkt()).num_days()
}

fn get_phase() -> (i32, &'static str) {
    let d = today_hkt();
    let cruise_end = NaiveDate::from_ymd_opt(2026, 3, 13).unwrap();
    let ramp_end = NaiveDate::from_ymd_opt(2026, 3, 28).unwrap();
    if d <= cruise_end {
        (1, "Cruise")
    } else if d <= ramp_end {
        (2, "Ramp")
    } else {
        (3, "Peak")
    }
}

fn daily_quota() -> usize {
    match get_phase().0 {
        1 => 5,
        2 => 15,
        _ => 20,
    }
}

fn default_count() -> usize {
    daily_quota()
}

fn get_mode(rate: f64) -> &'static str {
    for (threshold, label) in MODE_THRESHOLDS {
        if rate < *threshold {
            return label;
        }
    }
    "MCQ"
}

fn home_dir() -> Result<PathBuf> {
    let home = env::var("HOME").context("HOME is not set")?;
    Ok(PathBuf::from(home))
}

fn notes_dir() -> Result<PathBuf> {
    Ok(home_dir()?.join("notes"))
}

fn tracker_path() -> Result<PathBuf> {
    Ok(notes_dir()?.join(TRACKER_FILE))
}

fn state_path() -> Result<PathBuf> {
    Ok(notes_dir()?.join(STATE_FILE))
}

fn drills_path() -> Result<PathBuf> {
    Ok(notes_dir()?.join(DRILLS_FILE))
}

fn module_path(module: char) -> Result<PathBuf> {
    Ok(notes_dir()?.join(format!("GARP RAI Module {} - Raw Content.md", module)))
}

fn atomic_write(path: &Path, content: &str) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("path has no parent: {}", path.display()))?;
    fs::create_dir_all(parent)?;
    let ts = Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let tmp = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name().unwrap_or_default().to_string_lossy(),
        ts
    ));
    fs::write(&tmp, content)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

fn parse_datetime_any(s: &str) -> Option<DateTime<FixedOffset>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .or_else(|| DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f%:z").ok())
}

fn card_due_hkt(card: &PyCard) -> Option<DateTime<FixedOffset>> {
    parse_datetime_any(&card.due).map(|d| d.with_timezone(&hkt()))
}

fn card_last_review(card: &PyCard) -> Option<DateTime<FixedOffset>> {
    parse_datetime_any(&card.last_review).map(|d| d.with_timezone(&hkt()))
}

fn new_card(now: DateTime<FixedOffset>) -> PyCard {
    PyCard {
        card_id: Utc::now().timestamp_millis(),
        state: 1,
        step: Some(0),
        stability: 0.0,
        difficulty: 0.0,
        due: now.with_timezone(&Utc).to_rfc3339(),
        last_review: now.with_timezone(&Utc).to_rfc3339(),
    }
}

fn load_state() -> Result<State> {
    let path = state_path()?;
    if !path.exists() {
        return Ok(State::default());
    }

    let text = fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let raw: RawState = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(_) => {
            eprintln!("{}", "Warning: corrupt state file, starting fresh".red());
            return Ok(State::default());
        }
    };

    let mut cards = HashMap::new();
    for (topic, v) in raw.cards {
        let parsed = match v {
            Value::String(s) => serde_json::from_str::<PyCard>(&s).ok(),
            Value::Object(obj) => serde_json::from_value::<PyCard>(Value::Object(obj)).ok(),
            _ => None,
        };
        if let Some(card) = parsed {
            cards.insert(topic, card);
        } else {
            eprintln!(
                "{}",
                format!("Warning: skipping corrupt card for {}", topic).yellow()
            );
        }
    }

    Ok(State {
        cards,
        review_log: raw.review_log,
    })
}

fn save_state(state: &State) -> Result<()> {
    let path = state_path()?;
    let cutoff = (now_hkt() - Duration::days(90)).to_rfc3339();
    let log: Vec<ReviewEntry> = state
        .review_log
        .iter()
        .filter(|e| e.date >= cutoff)
        .cloned()
        .collect();

    let mut cards_map = Map::new();
    for (topic, card) in &state.cards {
        let card_json = serde_json::to_string(card)?;
        cards_map.insert(topic.clone(), Value::String(card_json));
    }

    let out = serde_json::json!({
        "cards": cards_map,
        "review_log": log,
    });

    atomic_write(&path, &serde_json::to_string_pretty(&out)?)
}

fn parse_tracker() -> Result<Tracker> {
    let path = tracker_path()?;
    if !path.exists() {
        return Ok(Tracker::default());
    }

    let text = fs::read_to_string(&path)?;

    let topics_re =
        Regex::new(r"(?m)^\|\s*(M\d-[\w-]+)\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*([\d—-]+%?)\s*\|")?;
    let mut topics = HashMap::new();
    for cap in topics_re.captures_iter(&text) {
        let topic = cap[1].to_string();
        let attempts = cap[2].parse::<i32>().unwrap_or(0);
        let correct = cap[3].parse::<i32>().unwrap_or(0);
        let rate_str = cap[4].trim();
        let rate = if rate_str == "—" || rate_str == "-" {
            0.0
        } else {
            rate_str.trim_end_matches('%').parse::<f64>().unwrap_or(0.0) / 100.0
        };
        topics.insert(
            topic,
            TopicInfo {
                attempts,
                correct,
                rate,
            },
        );
    }

    let summary_re = Regex::new(
        r"(?ms)^\|\s*Total Questions\s*\|\s*(\d+)\s*\|.*?^\|\s*Correct\s*\|\s*(\d+)\s*\|.*?^\|\s*Rate\s*\|\s*(\d+)%\s*\|.*?^\|\s*Sessions\s*\|\s*(\d+)\s*\|",
    )?;
    let summary = if let Some(cap) = summary_re.captures(&text) {
        SummaryInfo {
            total: cap[1].parse().unwrap_or(0),
            correct: cap[2].parse().unwrap_or(0),
            rate: cap[3].parse().unwrap_or(0),
            sessions: cap[4].parse().unwrap_or(0),
        }
    } else {
        SummaryInfo::default()
    };

    let miss_re = Regex::new(r"^\|\s*([\d-]+)\s*\|\s*(M\d-[\w-]+)\s*\|\s*(.+?)\s*\|")?;
    let mut recent_misses = Vec::new();
    let mut in_misses = false;
    for line in text.lines() {
        if line.contains("## Recent Misses") {
            in_misses = true;
            continue;
        }
        if in_misses && line.starts_with("## ") {
            break;
        }
        if in_misses {
            if let Some(cap) = miss_re.captures(line) {
                if &cap[1] != "Date" {
                    recent_misses.push(MissInfo {
                        date: cap[1].to_string(),
                        topic: cap[2].to_string(),
                        concept: cap[3].trim().to_string(),
                    });
                }
            }
        }
    }

    if topics.is_empty() {
        eprintln!(
            "{}",
            "Warning: No topics parsed from tracker. Check markdown format.".yellow()
        );
    }

    Ok(Tracker {
        summary,
        topics,
        recent_misses,
    })
}

fn update_tracker_record(topic: &str, rating: RatingKind) -> Result<()> {
    let path = tracker_path()?;
    if !path.exists() {
        return Ok(());
    }

    let mut text = fs::read_to_string(&path)?;
    let is_correct = rating == RatingKind::Good || rating == RatingKind::Easy;

    let total_re = Regex::new(r"(\|\s*Total Questions\s*\|\s*)(\d+)(\s*\|)")?;
    let correct_re = Regex::new(r"(\|\s*Correct\s*\|\s*)(\d+)(\s*\|)")?;
    let rate_re = Regex::new(r"(\|\s*Rate\s*\|\s*)(\d+)(%\s*\|)")?;

    if let (Some(t), Some(c)) = (total_re.captures(&text), correct_re.captures(&text)) {
        let new_total = t[2].parse::<i32>().unwrap_or(0) + 1;
        let new_correct = c[2].parse::<i32>().unwrap_or(0) + if is_correct { 1 } else { 0 };
        let new_rate = if new_total > 0 {
            ((new_correct as f64 / new_total as f64) * 100.0).round() as i32
        } else {
            0
        };
        text = total_re
            .replace(&text, format!("${{1}}{}${{3}}", new_total))
            .to_string();
        text = correct_re
            .replace(&text, format!("${{1}}{}${{3}}", new_correct))
            .to_string();
        text = rate_re
            .replace(&text, format!("${{1}}{}${{3}}", new_rate))
            .to_string();
    }

    let topic_pat = format!(
        r"(\|\s*{}\s*\|\s*)(\d+)(\s*\|\s*)(\d+)(\s*\|\s*)([\d—-]+%?)(\s*\|)",
        regex::escape(topic)
    );
    let topic_re = Regex::new(&topic_pat)?;
    if let Some(cap) = topic_re.captures(&text) {
        let na = cap[2].parse::<i32>().unwrap_or(0) + 1;
        let nc = cap[4].parse::<i32>().unwrap_or(0) + if is_correct { 1 } else { 0 };
        let nr = if na > 0 {
            ((nc as f64 / na as f64) * 100.0).round() as i32
        } else {
            0
        };
        let replacement = format!("${{1}}{}${{3}}{}${{5}}{}%${{7}}", na, nc, nr);
        text = topic_re.replace(&text, replacement).to_string();
    }

    let history_line = format!(
        "| {} | {} | {} | (recorded via rai) |",
        now_hkt().format("%Y-%m-%d"),
        topic,
        rating.result_str()
    );

    let mut lines: Vec<String> = text.lines().map(|s| s.to_string()).collect();
    let mut last_idx: Option<usize> = None;
    let mut in_history = false;
    for (i, line) in lines.iter().enumerate() {
        if line.contains("## History") {
            in_history = true;
        } else if in_history && line.starts_with("## ") {
            break;
        } else if in_history
            && line.starts_with('|')
            && !line.contains("Date")
            && !line.contains("---")
        {
            last_idx = Some(i);
        }
    }

    if let Some(i) = last_idx {
        lines.insert(i + 1, history_line);
    } else if in_history {
        for i in 0..lines.len() {
            if lines[i].contains("## History") {
                lines.insert(i + 1, history_line);
                break;
            }
        }
    }

    let out = lines.join("\n");
    atomic_write(&path, &out)
}

fn topics_with_drills() -> Result<HashSet<String>> {
    let path = drills_path()?;
    if !path.exists() {
        return Ok(HashSet::new());
    }
    let text = fs::read_to_string(path)?;
    let re = Regex::new(r"\((M\d-[\w-]+)")?;
    let mut out = HashSet::new();
    for line in text.lines() {
        if line.starts_with("## ") {
            if let Some(cap) = re.captures(line) {
                out.insert(cap[1].to_string());
            }
        }
    }
    Ok(out)
}

fn search_terms(topic: &str) -> Option<&'static [&'static str]> {
    match topic {
        "M1-classical-ai" => Some(&["Classical AI", "GOFAI", "Limits of Classical"]),
        "M1-ml-types" => Some(&["Types of Machine Learning", "Four Types"]),
        "M1-ai-risks" => Some(&["Risks of Inscrutability", "Risks of Over-Reliance"]),
        "M2-intro-tools" => Some(&["Machine Learning, Classical Statistics"]),
        "M2-data-prep" => Some(&["Data Scaling", "normalization", "standardization"]),
        "M2-clustering" => Some(&["K-means", "Hierarchical Clustering", "DBSCAN"]),
        "M2-econometric" => Some(&["Econometric", "Stepwise", "Variable Selection"]),
        "M2-regression-classification" => Some(&[
            "Decision Tree",
            "Random Forest",
            "SVM",
            "Logistic Regression",
        ]),
        "M2-semi-supervised" => Some(&[
            "Semi-supervised Learning Assumptions",
            "Self-Training",
            "Co-Training",
        ]),
        "M2-neural-networks" => Some(&["Neural Net", "Deep Learning", "Overfitting", "Dropout"]),
        "M2-semi-rl" => Some(&[
            "Reinforcement Learning",
            "Q-learning",
            "TD Learning",
            "Monte Carlo",
        ]),
        "M2-model-estimation" => Some(&["Regularization", "Ridge", "LASSO", "Elastic Net"]),
        "M2-model-eval" => Some(&["Model Evaluation", "Precision", "Recall", "AUC", "ROC"]),
        "M2-nlp-traditional" => Some(&["Tokenization", "Stemming", "Lemmatization", "TF-IDF"]),
        "M2-nlp-genai" => Some(&["Transformer", "BERT", "GPT", "Attention Mechanism"]),
        "M3-bias-unfairness" => Some(&[
            "Sources of Unfairness",
            "Algorithmic Bias",
            "Historical Bias",
        ]),
        "M3-fairness-measures" => Some(&[
            "Group Fairness",
            "Demographic Parity",
            "Equal Opportunity",
            "Equalized Odds",
        ]),
        "M3-xai" => Some(&[
            "Explainability",
            "Interpretability",
            "LIME",
            "SHAP",
            "LUCID",
        ]),
        "M3-autonomy-safety" => {
            Some(&["Autonomy", "Manipulation", "Automation Bias", "Well-Being"])
        }
        "M3-reputational-existential" => Some(&["Reputational Risk", "Existential Risk"]),
        "M3-genai-risks" => Some(&["GenAI", "Generative AI", "Hallucination", "Deepfake"]),
        "M4-ethical-frameworks" => Some(&[
            "Ethical Framework",
            "Consequentialism",
            "Deontology",
            "Virtue Ethics",
        ]),
        "M4-ethics-principles" => Some(&[
            "Ethics Principles",
            "Beneficence",
            "Justice",
            "Non-maleficence",
        ]),
        "M4-bias-discrimination" => Some(&[
            "Bias, Discrimination",
            "Problematic Biases",
            "When Does Bias",
        ]),
        "M4-privacy-cybersecurity" => Some(&["Privacy", "Cybersecurity", "Data Minimization"]),
        "M4-governance-challenges" => Some(&["Governance Challenges", "Power Asymmetries"]),
        "M4-regulatory" => Some(&["GDPR", "EU AI Act", "Regulatory", "AI Office"]),
        "M5-data-governance" => Some(&["Data Governance", "Data Quality", "Alternative Data"]),
        "M5-model-governance" => {
            Some(&["Model Governance", "Model Landscape", "Interdependencies"])
        }
        "M5-model-risk-roles" => Some(&["Three Lines", "Model Risk Management", "First Line"]),
        "M5-model-dev-testing" => Some(&["Model Development", "Model Testing"]),
        "M5-model-validation" => Some(&["Model Validation", "Validation Framework"]),
        "M5-model-changes-review" => Some(&["Model Changes", "Model Review", "Ongoing Monitoring"]),
        "M5-genai-governance" => Some(&[
            "GenAI Governance",
            "Stochasticity",
            "Third-Party",
            "Provider",
        ]),
        _ => None,
    }
}

fn find_source_location(topic: &str) -> Result<Option<String>> {
    let module_char = topic.chars().nth(1);
    let Some(module_char) = module_char else {
        return Ok(None);
    };
    let module_file = module_path(module_char)?;
    if !module_file.exists() {
        return Ok(None);
    }

    let terms: Vec<String> = if let Some(mapped) = search_terms(topic) {
        mapped.iter().map(|s| s.to_string()).collect()
    } else {
        let suffix = topic.split_once('-').map(|x| x.1).unwrap_or(topic);
        suffix
            .split('-')
            .filter(|w| w.len() > 2)
            .map(|w| {
                let mut chars = w.chars();
                match chars.next() {
                    Some(c) => format!("{}{}", c.to_ascii_uppercase(), chars.as_str()),
                    None => String::new(),
                }
            })
            .collect()
    };

    let file_text = fs::read_to_string(&module_file)?;
    let lines: Vec<&str> = file_text.lines().collect();

    let mut seen = HashSet::new();
    let mut hits = Vec::new();

    for (i, line) in lines.iter().enumerate() {
        if line.starts_with("##") {
            for term in &terms {
                if line.to_lowercase().contains(&term.to_lowercase()) {
                    let h = line.trim().to_string();
                    if seen.insert(h) {
                        hits.push(i);
                    }
                    break;
                }
            }
        }
    }

    if hits.is_empty() {
        for (i, line) in lines.iter().enumerate() {
            let mut matched = false;
            for term in &terms {
                if line.to_lowercase().contains(&term.to_lowercase()) {
                    let upper_bound = usize::min(i + 6, lines.len());
                    let long_nearby = (i + 1..upper_bound).any(|j| lines[j].len() > 80);
                    if long_nearby {
                        hits.push(i.saturating_sub(2));
                    }
                    matched = true;
                    break;
                }
            }
            if matched && hits.len() >= 2 {
                break;
            }
        }
    }

    if hits.is_empty() {
        return Ok(None);
    }

    let start = hits[0];
    let mut end = usize::min(start + 80, lines.len());
    for i in start + 4..end {
        if lines[i].starts_with("## ") {
            end = i;
            break;
        }
    }

    Ok(Some(format!(
        "{}:{}-{}",
        module_file.display(),
        start + 1,
        end
    )))
}

fn is_tty() -> bool {
    std::io::stdout().is_terminal()
}

fn print_panel(title: &str) {
    if is_tty() {
        let w = title.chars().count() + 2;
        println!();
        println!("╭{}╮", "─".repeat(w));
        println!("│ {} │", title.bold());
        println!("╰{}╯", "─".repeat(w));
    } else {
        println!("## {title}");
    }
}

fn normalize(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .flat_map(|c| c.to_lowercase())
        .collect()
}

fn resolve_topic(input: &str, tracker: &Tracker) -> Option<String> {
    if tracker.topics.contains_key(input) {
        return Some(input.to_string());
    }

    let q = input.to_lowercase();

    for t in tracker.topics.keys() {
        if t.to_lowercase() == q {
            println!("{}", format!("Matched: {}", t).dimmed());
            return Some(t.clone());
        }
    }

    let mut alias_map: HashMap<String, HashSet<String>> = HashMap::new();
    for topic in tracker.topics.keys() {
        let mut aliases = vec![topic.to_lowercase()];
        if let Some((_, suffix)) = topic.split_once('-') {
            aliases.push(suffix.to_lowercase());
            aliases.push(suffix.replace('-', " ").to_lowercase());
            aliases.push(normalize(suffix));
        }
        aliases.push(normalize(topic));

        if let Some(terms) = search_terms(topic) {
            for t in terms {
                aliases.push(t.to_lowercase());
                aliases.push(normalize(t));
            }
        }

        for a in aliases {
            alias_map.entry(a).or_default().insert(topic.clone());
        }
    }

    let mut matches: HashSet<String> = HashSet::new();

    if let Some(s) = alias_map.get(&q) {
        for m in s {
            matches.insert(m.clone());
        }
    }
    let qn = normalize(input);
    if let Some(s) = alias_map.get(&qn) {
        for m in s {
            matches.insert(m.clone());
        }
    }

    if matches.is_empty() {
        for t in tracker.topics.keys() {
            let tl = t.to_lowercase();
            let ts = t.split_once('-').map(|x| x.1).unwrap_or(t).to_lowercase();
            if tl.contains(&q) || ts.contains(&q) || q.contains(&ts) {
                matches.insert(t.clone());
            }
        }
    }

    if matches.len() == 1 {
        let m = matches.iter().next().cloned().unwrap();
        println!("{}", format!("Matched: {}", m).dimmed());
        return Some(m);
    }

    if !matches.is_empty() {
        println!("{}", "Ambiguous:".yellow());
        let mut v: Vec<_> = matches.into_iter().collect();
        v.sort();
        for m in v {
            println!("  - {}", m);
        }
        return None;
    }

    println!("{}", format!("Unknown topic: {}", input).red());
    None
}

fn rating_from_str(s: &str) -> Option<RatingKind> {
    match s.to_lowercase().as_str() {
        "again" | "miss" => Some(RatingKind::Again),
        "hard" | "guess" => Some(RatingKind::Hard),
        "good" | "ok" => Some(RatingKind::Good),
        "easy" | "confident" => Some(RatingKind::Easy),
        _ => None,
    }
}

fn state_name(state: i32) -> &'static str {
    match state {
        1 => "learning",
        2 => "review",
        3 => "relearning",
        _ => "new",
    }
}

fn schedule_card(
    mut card: PyCard,
    rating: RatingKind,
    now: DateTime<FixedOffset>,
) -> Result<PyCard> {
    let fsrs = FSRS::new(Some(&DEFAULT_PARAMETERS)).map_err(|e| anyhow::anyhow!("{:?}", e))?;
    let prev_memory = if card.stability > 0.0 && card.difficulty > 0.0 {
        Some(MemoryState {
            stability: card.stability as f32,
            difficulty: card.difficulty as f32,
        })
    } else {
        None
    };

    let elapsed_days = card_last_review(&card)
        .map(|dt| now.signed_duration_since(dt).num_days().max(0) as u32)
        .unwrap_or(0);

    let next = fsrs
        .next_states(prev_memory, DESIRED_RETENTION, elapsed_days)
        .map_err(|e| anyhow!("FSRS scheduling failed: {e}"))?;

    let item = match rating {
        RatingKind::Again => next.again,
        RatingKind::Hard => next.hard,
        RatingKind::Good => next.good,
        RatingKind::Easy => next.easy,
    };

    let interval_days = item.interval.max(1.0);
    let due = now + Duration::seconds((interval_days as f64 * 86_400.0).round() as i64);

    let was_new = prev_memory.is_none();
    let (new_state, step) = if rating == RatingKind::Again {
        if was_new {
            (1, Some(0))
        } else {
            (3, Some(0))
        }
    } else {
        (2, None)
    };

    card.state = new_state;
    card.step = step;
    card.stability = item.memory.stability as f64;
    card.difficulty = item.memory.difficulty as f64;
    card.last_review = now.with_timezone(&Utc).to_rfc3339();
    card.due = due.with_timezone(&Utc).to_rfc3339();

    if card.card_id == 0 {
        card.card_id = Utc::now().timestamp_millis();
    }

    Ok(card)
}

fn get_today_reviews(state: &State) -> Vec<ReviewEntry> {
    let today = today_hkt().to_string();
    state
        .review_log
        .iter()
        .filter(|e| e.date.starts_with(&today))
        .cloned()
        .collect()
}

fn cmd_session(count: Option<usize>) -> Result<()> {
    let state = load_state()?;
    let tracker = parse_tracker()?;
    let now = now_hkt();
    let days_left = days_until_exam();
    let (phase_num, phase_name) = get_phase();
    let n = count.unwrap_or_else(default_count);

    if n < 1 {
        return Err(anyhow!("Session count must be positive"));
    }

    let today_reviews = get_today_reviews(&state);
    let tested_today: HashSet<String> = today_reviews.iter().map(|e| e.topic.clone()).collect();

    let q_per_session = daily_quota();
    if today_reviews.len() >= q_per_session {
        println!();
        println!(
            "  {}",
            format!(
                "✓ Already done {} questions today ({} topics). Quota met.",
                today_reviews.len(),
                tested_today.len()
            )
            .green()
        );
        println!("  {}", "Continuing with unreviewed topics...".dimmed());
        println!();
    }

    let mut due: Vec<(String, TopicInfo, i64)> = Vec::new();

    for (topic, info) in &tracker.topics {
        if tested_today.contains(topic) {
            continue;
        }
        if let Some(card) = state.cards.get(topic) {
            if let Some(due_dt) = card_due_hkt(card) {
                if due_dt <= now {
                    let overdue = now.signed_duration_since(due_dt).num_days();
                    due.push((topic.clone(), info.clone(), overdue));
                }
            } else {
                due.push((topic.clone(), info.clone(), 999));
            }
        } else {
            due.push((topic.clone(), info.clone(), 999));
        }
    }

    due.sort_by(|a, b| {
        b.2.cmp(&a.2)
            .then_with(|| a.1.rate.partial_cmp(&b.1.rate).unwrap_or(Ordering::Equal))
    });

    let weak: Vec<_> = due
        .iter()
        .filter(|(_, i, _)| i.rate < 0.60)
        .cloned()
        .collect();
    let strong: Vec<_> = due
        .iter()
        .filter(|(_, i, _)| i.rate >= 0.60)
        .cloned()
        .collect();

    let max_weak = usize::min(weak.len(), usize::max(1, ((n as f64) * 0.6) as usize));
    let mut selected: Vec<(String, TopicInfo, i64)> = weak[..max_weak].to_vec();

    let need = n.saturating_sub(selected.len());
    selected.extend(strong.into_iter().take(need));

    if selected.len() < n {
        let used: HashSet<_> = selected.iter().map(|x| x.0.clone()).collect();
        for item in &due {
            if selected.len() >= n {
                break;
            }
            if !used.contains(&item.0) {
                selected.push(item.clone());
            }
        }
    }
    selected.truncate(n);

    let mut interleaved: Vec<(String, TopicInfo, i64)> = Vec::new();
    let mut remaining = selected;

    while !remaining.is_empty() {
        if interleaved.len() >= 2 {
            let last_mod = &interleaved[interleaved.len() - 1].0[..2];
            let prev_mod = &interleaved[interleaved.len() - 2].0[..2];
            if last_mod == prev_mod {
                if let Some(pos) = remaining.iter().position(|x| &x.0[..2] != last_mod) {
                    interleaved.push(remaining.remove(pos));
                } else {
                    interleaved.push(remaining.remove(0));
                }
                continue;
            }
        }
        interleaved.push(remaining.remove(0));
    }

    let summary = &tracker.summary;
    print_panel(&format!(
        "Session Plan | Phase {} ({}) | {} days to exam",
        phase_num, phase_name, days_left
    ));
    println!(
        "  Overall: {}/{} ({}%)  |  {} sessions",
        summary.correct, summary.total, summary.rate, summary.sessions
    );

    let m12 = interleaved
        .iter()
        .filter(|(t, _, _): &&(String, TopicInfo, i64)| t.starts_with("M1-") || t.starts_with("M2-"))
        .count();
    if !interleaved.is_empty() && (m12 as f64 / interleaved.len() as f64) < 0.30 {
        println!(
            "  {}",
            format!("M1/M2 quota: {}/{} (target ≥30%)", m12, interleaved.len()).yellow()
        );
    }
    println!();

    if !tracker.recent_misses.is_empty() {
        println!("{}", "Recent misses:".bold());
        for m in tracker.recent_misses.iter().rev().take(5).rev() {
            println!("  - {} ({}) [{}]", m.concept, m.date, m.topic);
        }
        println!();
    }

    let drilled = topics_with_drills()?;
    println!("{}", format!("Questions ({}):", interleaved.len()).bold());
    println!();

    for (idx, (topic, info, overdue)) in interleaved.iter().enumerate() {
        let mode = get_mode(info.rate);
        let colored_topic: colored::ColoredString = match mode {
            "drill" => topic.red().bold(),
            "free-recall" => topic.yellow().bold(),
            _ => topic.green().bold(),
        };
        let drill_tag = if drilled.contains(topic) {
            format!(" {}", "[drill]".cyan())
        } else {
            String::new()
        };
        let source = find_source_location(topic)?.unwrap_or_else(|| "not found".to_string());
        println!(
            "  Q{}: {}  |  {} ({:.0}%)  |  overdue {}d{}",
            idx + 1,
            colored_topic,
            mode,
            info.rate * 100.0,
            overdue,
            drill_tag
        );
        println!("      {}", source.dimmed());
    }
    println!();

    Ok(())
}

fn cmd_record(topic_input: &str, rating_str: &str) -> Result<()> {
    let mut rating = match rating_from_str(rating_str) {
        Some(r) => r,
        None => {
            println!(
                "{}",
                format!(
                    "Unknown rating: {}. Valid: again/miss, hard/guess, good/ok, easy/confident",
                    rating_str
                )
                .red()
            );
            return Ok(());
        }
    };

    let mut state = load_state()?;
    let tracker = parse_tracker()?;

    let Some(topic) = resolve_topic(topic_input, &tracker) else {
        return Ok(());
    };

    let topic_info = tracker.topics.get(&topic).cloned().unwrap_or_default();
    let topic_rate = topic_info.rate;
    if topic_rate < 0.60 && (rating == RatingKind::Good || rating == RatingKind::Easy) {
        println!(
            "  {}",
            format!(
                "Acquisition cap: {} at {:.0}% — capping {} → hard",
                topic,
                topic_rate * 100.0,
                rating_str
            )
            .yellow()
        );
        rating = RatingKind::Hard;
    }

    let now = now_hkt();
    let card = state
        .cards
        .get(&topic)
        .cloned()
        .unwrap_or_else(|| new_card(now));
    let card = schedule_card(card, rating, now)?;

    state.cards.insert(topic.clone(), card.clone());
    state.review_log.push(ReviewEntry {
        topic: topic.clone(),
        rating: rating.log_name().to_string(),
        date: now.to_rfc3339(),
    });

    save_state(&state)?;
    update_tracker_record(&topic, rating)?;

    let due_hkt = card_due_hkt(&card).unwrap_or(now);
    let days = due_hkt.signed_duration_since(now_hkt()).num_days();
    let display = match rating {
        RatingKind::Again => rating.display_str().red(),
        RatingKind::Hard => rating.display_str().yellow(),
        RatingKind::Good => rating.display_str().green(),
        RatingKind::Easy => rating.display_str().bright_green(),
    };

    println!();
    println!("  {}  {}", display, topic.bold());
    println!(
        "  Next: {} ({:+}d)  |  {}",
        due_hkt.format("%b %d").to_string().cyan(),
        days,
        state_name(card.state)
    );
    println!();

    Ok(())
}

fn cmd_stats() -> Result<()> {
    let tracker = parse_tracker()?;
    let summary = &tracker.summary;
    let (phase_num, phase_name) = get_phase();
    let days_left = days_until_exam();
    let drilled = topics_with_drills()?;

    print_panel(&format!(
        "Stats | Phase {} ({}) | {} days to exam",
        phase_num, phase_name, days_left
    ));
    println!(
        "  Total: {} questions across {} sessions",
        summary.total, summary.sessions
    );
    println!(
        "  Rate: {}%  ({}/{})",
        summary.rate, summary.correct, summary.total
    );
    println!(
        "  Drill coverage: {} topics have Definition Drills entries",
        drilled.len()
    );
    println!();

    let mut weak: Vec<_> = tracker
        .topics
        .iter()
        .filter(|(_, i)| i.rate < 0.60)
        .map(|(t, i)| (t.clone(), i.clone()))
        .collect();
    weak.sort_by(|a, b| a.1.rate.partial_cmp(&b.1.rate).unwrap_or(Ordering::Equal));

    if !weak.is_empty() {
        println!("{}", "Weak topics (<60%):".bold());
        for (t, i) in weak {
            let tag = if drilled.contains(&t) {
                "[drill]".cyan().to_string()
            } else {
                "[no drill]".dimmed().to_string()
            };
            println!(
                "  {}: {:.0}% ({}/{}) {}",
                t,
                i.rate * 100.0,
                i.correct,
                i.attempts,
                tag
            );
        }
        println!();
    }

    Ok(())
}

fn cmd_topics() -> Result<()> {
    let tracker = parse_tracker()?;
    let drilled = topics_with_drills()?;
    let mut topics: Vec<_> = tracker.topics.into_iter().collect();
    topics.sort_by(|a, b| a.1.rate.partial_cmp(&b.1.rate).unwrap_or(Ordering::Equal));

    println!();
    println!("{}", "All topics:".bold());
    println!();

    for (t, i) in topics {
        let rate_str = if i.attempts > 0 {
            format!("{:.0}%", i.rate * 100.0)
        } else {
            "—".to_string()
        };
        let tag = if drilled.contains(&t) {
            format!(" {}", "[drill]".cyan())
        } else {
            String::new()
        };
        let line = format!("{}: {} ({}/{}){}", t, rate_str, i.correct, i.attempts, tag);
        if i.rate < 0.60 {
            println!("  {}", line.red());
        } else if i.rate < 0.70 {
            println!("  {}", line.yellow());
        } else {
            println!("  {}", line.green());
        }
    }
    println!();

    Ok(())
}

fn cmd_due() -> Result<()> {
    let state = load_state()?;
    let tracker = parse_tracker()?;
    let now = now_hkt();

    let mut due_topics = Vec::new();
    for topic in tracker.topics.keys() {
        if let Some(card) = state.cards.get(topic) {
            if let Some(due_dt) = card_due_hkt(card) {
                if due_dt <= now {
                    let overdue = now.signed_duration_since(due_dt).num_days();
                    due_topics.push((topic.clone(), overdue, state_name(card.state).to_string()));
                }
            } else {
                due_topics.push((topic.clone(), 999, "new".to_string()));
            }
        } else {
            due_topics.push((topic.clone(), 999, "new".to_string()));
        }
    }

    due_topics.sort_by(|a, b| b.1.cmp(&a.1));

    println!();
    println!("{}", format!("{} topics due:", due_topics.len()).bold());
    println!();
    for (t, overdue, st) in due_topics {
        let line = format!("{}: overdue {}d ({})", t, overdue, st);
        if overdue > 0 {
            println!("  {}", line.red());
        } else {
            println!("  {}", line.yellow());
        }
    }
    println!();

    Ok(())
}

fn cmd_today() -> Result<()> {
    let state = load_state()?;
    let (phase_num, phase_name) = get_phase();
    let today_reviews = get_today_reviews(&state);
    let q_per_session = daily_quota();

    let mut topics_today = HashSet::new();
    let mut correct_today = 0;
    let mut miss_today = 0;

    for r in &today_reviews {
        topics_today.insert(r.topic.clone());
        let rl = r.rating.to_lowercase();
        if ["good", "ok", "easy", "confident"].contains(&rl.as_str()) {
            correct_today += 1;
        } else if ["again", "miss"].contains(&rl.as_str()) {
            miss_today += 1;
        }
    }

    let total_today = today_reviews.len();

    let mut sessions_today = 0;
    if !today_reviews.is_empty() {
        sessions_today = 1;
        let mut sorted = today_reviews.clone();
        sorted.sort_by(|a, b| a.date.cmp(&b.date));
        for pair in sorted.windows(2) {
            if let (Some(prev), Some(curr)) = (
                parse_datetime_any(&pair[0].date),
                parse_datetime_any(&pair[1].date),
            ) {
                if curr.signed_duration_since(prev).num_seconds() > 1800 {
                    sessions_today += 1;
                }
            }
        }
    }

    let quota_met = total_today >= q_per_session;

    print_panel(&format!(
        "Today | Phase {} ({}) | {} days to exam",
        phase_num,
        phase_name,
        days_until_exam()
    ));

    if total_today == 0 {
        println!("  {}", "No reviews today.".dimmed());
    } else {
        let rate = ((correct_today as f64 / total_today as f64) * 100.0).round() as i32;
        println!(
            "  Questions: {}  |  Correct: {}  |  Missed: {}  |  Rate: {}%",
            total_today, correct_today, miss_today, rate
        );
        println!(
            "  Sessions: ~{}  |  Topics: {}",
            sessions_today,
            topics_today.len()
        );

        if quota_met {
            println!(
                "  {}",
                format!("✓ Daily quota met ({}+ questions)", q_per_session).green()
            );
        } else {
            let remaining = q_per_session - total_today;
            println!(
                "  {}",
                format!(
                    "◯ {} more questions to meet daily quota ({})",
                    remaining, q_per_session
                )
                .yellow()
            );
        }
    }

    if !today_reviews.is_empty() {
        println!();
        println!("{}", "Topics reviewed today:".bold());
        let mut topic_results: HashMap<String, Vec<String>> = HashMap::new();
        for r in today_reviews {
            topic_results
                .entry(r.topic)
                .or_default()
                .push(r.rating.to_lowercase());
        }
        let mut ks: Vec<_> = topic_results.keys().cloned().collect();
        ks.sort();
        for t in ks {
            let ratings = topic_results.get(&t).unwrap().join(", ");
            println!("  {}: {}", t, ratings);
        }
    }

    println!();
    Ok(())
}

fn cmd_end_session() -> Result<()> {
    let path = tracker_path()?;
    if !path.exists() {
        println!("{}", "Tracker not found".red());
        return Ok(());
    }

    let text = fs::read_to_string(&path)?;
    let re = Regex::new(r"(\|\s*Sessions\s*\|\s*)(\d+)(\s*\|)")?;
    let Some(cap) = re.captures(&text) else {
        println!("{}", "Sessions row not found in tracker".red());
        return Ok(());
    };

    let old = cap[2].parse::<i32>().unwrap_or(0);
    let new = old + 1;
    let out = re
        .replace(&text, format!("${{1}}{}${{3}}", new))
        .to_string();
    atomic_write(&path, &out)?;

    println!();
    println!(
        "  Session {} recorded (was {})",
        new.to_string().bold(),
        old
    );
    println!();

    Ok(())
}

fn cmd_reconcile() -> Result<()> {
    let path = tracker_path()?;
    if !path.exists() {
        println!("{}", "Tracker not found".red());
        return Ok(());
    }

    let tracker = parse_tracker()?;
    let topics = &tracker.topics;

    if topics.len() < 10 {
        println!(
            "  {}",
            format!(
                "Abort: only {} topics parsed (expected ~34). Check tracker format.",
                topics.len()
            )
            .red()
        );
        return Ok(());
    }

    let actual_total: i32 = topics.values().map(|t| t.attempts).sum();
    let actual_correct: i32 = topics.values().map(|t| t.correct).sum();
    let actual_rate = if actual_total > 0 {
        ((actual_correct as f64 / actual_total as f64) * 100.0).round() as i32
    } else {
        0
    };

    let old_total = tracker.summary.total;
    let old_correct = tracker.summary.correct;

    if old_total == actual_total && old_correct == actual_correct {
        println!();
        println!("  {}", "Summary is in sync. No changes needed.".green());
        println!();
        return Ok(());
    }

    let mut text = fs::read_to_string(&path)?;
    let total_re = Regex::new(r"(\|\s*Total Questions\s*\|\s*)\d+(\s*\|)")?;
    let correct_re = Regex::new(r"(\|\s*Correct\s*\|\s*)\d+(\s*\|)")?;
    let rate_re = Regex::new(r"(\|\s*Rate\s*\|\s*)\d+(%\s*\|)")?;

    text = total_re
        .replace(&text, format!("${{1}}{}${{2}}", actual_total))
        .to_string();
    text = correct_re
        .replace(&text, format!("${{1}}{}${{2}}", actual_correct))
        .to_string();
    text = rate_re
        .replace(&text, format!("${{1}}{}${{2}}", actual_rate))
        .to_string();

    atomic_write(&path, &text)?;

    println!();
    println!("  {}", "Reconciled:".yellow());
    println!("    Total: {} -> {}", old_total, actual_total);
    println!("    Correct: {} -> {}", old_correct, actual_correct);
    println!("    Rate: {}% -> {}%", tracker.summary.rate, actual_rate);
    println!();

    Ok(())
}

fn cmd_coverage() -> Result<()> {
    let tracker = parse_tracker()?;
    let tracked = &tracker.topics;

    let tracked_set: HashSet<String> = tracked.keys().cloned().collect();
    let syllabus_set: HashSet<String> = GARP_RAI_SYLLABUS.iter().map(|s| s.to_string()).collect();

    let mut missing: Vec<String> = syllabus_set.difference(&tracked_set).cloned().collect();
    missing.sort();

    let mut never_attempted: Vec<String> = tracked
        .iter()
        .filter(|(_, i)| i.attempts == 0)
        .map(|(t, _)| t.clone())
        .collect();
    never_attempted.sort();

    let mut low_coverage: Vec<String> = tracked
        .iter()
        .filter(|(_, i)| i.attempts > 0 && i.attempts < 3)
        .map(|(t, _)| t.clone())
        .collect();
    low_coverage.sort_by_key(|t| tracked.get(t).map(|i| i.attempts).unwrap_or(0));

    let tracked_in_syllabus = tracked_set.intersection(&syllabus_set).count();
    let coverage_pct = if !GARP_RAI_SYLLABUS.is_empty() {
        (tracked_in_syllabus as f64 / GARP_RAI_SYLLABUS.len() as f64) * 100.0
    } else {
        0.0
    };

    print_panel(&format!(
        "Coverage Report | {} syllabus topics",
        GARP_RAI_SYLLABUS.len()
    ));
    println!(
        "  Tracked: {}/{} ({:.0}%)",
        tracked_in_syllabus,
        GARP_RAI_SYLLABUS.len(),
        coverage_pct
    );

    if !missing.is_empty() {
        println!();
        println!(
            "{} {}",
            format!("MISSING ({}):", missing.len()).red().bold(),
            "in syllabus but not in tracker".dimmed()
        );
        for t in &missing {
            println!("  {}", t);
        }
    }

    if !never_attempted.is_empty() {
        println!();
        println!(
            "{} {}",
            format!("NEVER ATTEMPTED ({}):", never_attempted.len())
                .yellow()
                .bold(),
            "in tracker but 0 attempts".dimmed()
        );
        for t in &never_attempted {
            println!("  {}", t);
        }
    }

    if !low_coverage.is_empty() {
        println!();
        println!(
            "{} {}",
            format!("LOW COVERAGE ({}):", low_coverage.len())
                .cyan()
                .bold(),
            "<3 attempts".dimmed()
        );
        for t in &low_coverage {
            if let Some(i) = tracked.get(t) {
                println!("  {}: {} attempts ({:.0}%)", t, i.attempts, i.rate * 100.0);
            }
        }
    }

    if missing.is_empty() && never_attempted.is_empty() && low_coverage.is_empty() {
        println!();
        println!(
            "  {}",
            "All syllabus topics covered with adequate attempts.".green()
        );
    }

    println!();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_thresholds() {
        assert_eq!(get_mode(0.59), "drill");
        assert_eq!(get_mode(0.60), "free-recall");
        assert_eq!(get_mode(0.85), "MCQ");
    }

    #[test]
    fn parse_py_card_json_string() {
        let raw = r#"{\"card_id\": 1, \"state\": 2, \"step\": null, \"stability\": 1.2, \"difficulty\": 3.4, \"due\": \"2026-02-01T00:00:00+00:00\", \"last_review\": \"2026-01-01T00:00:00+00:00\"}"#;
        let card: PyCard = serde_json::from_str(raw).unwrap();
        assert_eq!(card.state, 2);
        assert!(card.step.is_none());
    }

    #[test]
    fn rating_aliases() {
        assert_eq!(rating_from_str("miss"), Some(RatingKind::Again));
        assert_eq!(rating_from_str("guess"), Some(RatingKind::Hard));
        assert_eq!(rating_from_str("ok"), Some(RatingKind::Good));
        assert_eq!(rating_from_str("confident"), Some(RatingKind::Easy));
    }
}
