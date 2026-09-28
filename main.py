from future import annotations

import argparse
import json
import logging
import re
import statistics
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Iterator, Protocol, Sequence

LOGGER = logging.getLogger("fndd")

DEFAULT_STOPWORDS: frozenset[str] = frozenset(
  {
    "the", "a", "an", "and", "or", "but", "if", "then", "than",
    "so", "as", "at", "by", "for", "from", "in", "into", "of",
    "on", "onto", "to", "with", "is", "are", "was", "were",
    "be", "been", "being", "it", "its", "this", "that", "these",
    "those", "i", "you", "he", "she", "we", "they", "them",
    "his", "her", "our", "their", "not", "no", "yes", "do",
    "does", "did", "has", "have", "had", "will", "would", "can",
    "could", "should", "may", "might", "must", "about", "after",
    "before", "over", "under", "again", "more", "most", "some",
    "such", "only", "own", "same", "too", "very", "just",
  }
)

POSITIVE_WORDS: frozenset[str] = frozenset(
  {
    "win", "wins", "growth", "success", "successful", "improve",
    "improved", "improvement", "gain", "gains", "boost", "record",
    "best", "great", "strong", "positive", "progress", "rally",
    "recover", "recovery", "profit", "profits", "up", "high",
  }
)

NEGATIVE_WORDS: frozenset[str] = frozenset(
  {
    "loss", "losses", "crisis", "fail", "failed", "failure",
    "decline", "declined", "risk", "risks", "weak", "worst",
    "negative", "crash", "fall", "falls", "drop", "dropped",
    "threat", "threatens", "warning", "warn", "down", "low",
  }
)

WORD_PATTERN = re.compile(r"[A-Za-z][A-Za-z'-]*")
SENTENCE_PATTERN = re.compile(r"[.!?]+")

WORDS_PER_MINUTE = 200

class Source(Protocol):
  """Protocol describing a news data source."""

def fetch(self) -> Iterable[dict[str, str]]:
  """Yield raw records as dictionaries."""
  ...

@dataclass(frozen=True)
class RawRecord:
  """Represents a raw, unvalidated news record."""

source: str
payload: dict[str, str]

@dataclass(frozen=True)
class NewsItem:
  """Immutable, validated representation of a news article."""

title: str
source: str
body: str
published_at: datetime

def word_count(self) -> int:
  """Return the number of words in the article body."""
  return len(WORD_PATTERN.findall(self.body))

def reading_minutes(self) -> float:
  """Estimate reading time in minutes."""
  return round(self.word_count() / WORDS_PER_MINUTE, 2)

@dataclass(frozen=True)
class Analysis:
  """Derived analytics for a single news item."""

title: str
source: str
word_count: int
reading_minutes: float
keywords: tuple[str, ...]
sentiment: str
sentiment_score: float

def to_dict(self) -> dict[str, object]:
  """Serialize the analysis to a JSON-friendly dictionary."""
  return asdict(self)

@dataclass
class PipelineStats:
  """Mutable counters describing a pipeline run."""

fetched: int = 0
parsed: int = 0
failed: int = 0
analysed: int = 0

def as_dict(self) -> dict[str, int]:
  """Return the statistics as a plain dictionary."""
  return asdict(self)

def log_summary(self) -> None:
  """Log a human-readable summary of the run."""
  LOGGER.info(
  "Pipeline finished: fetched=%d parsed=%d analysed=%d failed=%d",
  self.fetched,
  self.parsed,
  self.analysed,
  self.failed,
  )

class ParseError(ValueError):
  """Raised when a raw record cannot be parsed into a NewsItem."""

def normalize_whitespace(text: str) -> str:
  """Collapse runs of whitespace into single spaces and strip the result."""
  return " ".join(text.split())

def parse_timestamp(value: str) -> datetime:
  """Parse an ISO-8601 timestamp into a timezone-aware datetime."""
  try:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
  except ValueError as exc: # pragma: no cover - defensive branch
  raise ParseError(f"invalid timestamp: {value!r}") from exc
  if parsed.tzinfo is None:
    parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed

def parse_record(record: RawRecord) -> NewsItem:
  """Validate and convert a RawRecord into a NewsItem."""
  payload = record.payload
  missing = [key for key in ("title", "body", "published_at") if not payload.get(key)]
  if missing:
    raise ParseError(f"missing required field(s): {', '.join(missing)}")

return NewsItem(
  title=normalize_whitespace(payload["title"]),
  source=record.source,
  body=normalize_whitespace(payload["body"]),
  published_at=parse_timestamp(payload["published_at"]),
)

def tokenize(text: str) -> list[str]:
  """Lowercase tokenize a piece of text into words."""
  return [match.group(0).lower() for match in WORD_PATTERN.finditer(text)]

def extract_keywords(text: str, limit: int = 5) -> tuple[str, ...]:
  """Return the most common non-stopword tokens in the text."""
  tokens = [token for token in tokenize(text) if token not in DEFAULT_STOPWORDS and len(token) > 2]
  if not tokens:
    return ()
    counts = Counter(tokens)
    return tuple(word for word, _ in counts.most_common(limit))

def score_sentiment(text: str) -> tuple[str, float]:
  """Compute a heuristic sentiment label and score for the text.

  The score is normalised to the range [-1.0, 1.0].
  """
  tokens = tokenize(text)
  if not tokens:
    return "neutral", 0.0

positive = sum(1 for token in tokens if token in POSITIVE_WORDS)
negative = sum(1 for token in tokens if token in NEGATIVE_WORDS)

raw = positive - negative
score = raw / len(tokens)
score = max(-1.0, min(1.0, score * 10))

if score > 0.05:
  return "positive", round(score, 4)
  if score < -0.05:
    return "negative", round(score, 4)
    return "neutral", round(score, 4)

def analyse_item(item: NewsItem) -> Analysis:
  """Enrich a NewsItem with keyword and sentiment analytics."""
  keywords = extract_keywords(f"{item.title} {item.body}")
  sentiment, score = score_sentiment(f"{item.title} {item.body}")
  return Analysis(
  title=item.title,
  source=item.source,
  word_count=item.word_count(),
  reading_minutes=item.reading_minutes(),
  keywords=keywords,
  sentiment=sentiment,
  sentiment_score=score,
  )

def split_sentences(text: str) -> list[str]:
  """Split text into sentences using a simple punctuation heuristic."""
  return [segment.strip() for segment in SENTENCE_PATTERN.split(text) if segment.strip()]

class InMemorySource:
  """A simple in-memory news source used for demonstrations and tests."""

def init(self, records: Sequence[dict[str, str]], name: str = "memory") -> None:
  self._records = list(records)
  self._name = name

@property
def name(self) -> str:
  """Return the source name."""
  return self._name

def fetch(self) -> Iterator[dict[str, str]]:
  """Yield each stored record."""
  for record in self._records:
    yield dict(record)

def run_pipeline(
  sources: Sequence[Source],
  ,
limit: int = 0,
  analyser: Callable[[NewsItem], Analysis] = analyse_item,
) -> tuple[list[Analysis], PipelineStats]:
  """Execute the full fetch, parse, and analyse pipeline."""
  stats = PipelineStats()
  results: list[Analysis] = []

for source in sources:
  LOGGER.info("Fetching from source: %s", getattr(source, "name", source))
  for payload in source.fetch():
    if limit and stats.fetched >= limit:
      LOGGER.info("Reached limit of %d item(s); stopping", limit)
      stats.log_summary()
      return results, stats

stats.fetched += 1
record = RawRecord(
  source=getattr(source, "name", "unknown"),
  payload=dict(payload),
)
try:
  item = parse_record(record)
except ParseError as exc:
  stats.failed += 1
  LOGGER.warning("Skipping invalid record: %s", exc)
  continue

stats.parsed += 1
analysis = analyser(item)
stats.analysed += 1
results.append(analysis)

stats.log_summary()
return results, stats

def export_json(records: Sequence[Analysis], path: Path) -> None:
  """Persist analysis results to disk as JSON."""
  path.write_text(
  json.dumps([record.to_dict() for record in records], indent=2),
  encoding="utf-8",
  )
  LOGGER.info("Exported %d record(s) to %s", len(records), path)

def build_demo_source() -> InMemorySource:
  """Build a small in-memory source used for the CLI demo."""
  return InMemorySource(
  name="demo",
  records=[
  {
  "title": "Markets rally on strong growth data",
  "body": (
  "Investors welcomed strong growth and a steady gain in exports. "
  "Analysts reported record profit across several sectors."
  ),
  "published_at": "2026-01-05T09:30:00Z",
  },
  {
  "title": "Supply chain crisis deepens",
  "body": (
  "A deepening crisis and rising risk threaten global trade. "
  "Manufacturers warn of further decline and potential loss."
  ),
  "published_at": "2026-01-06T14:15:00Z",
  },
  {
  "title": "Local team secures narrow win",
  "body": (
  "The home side secured a narrow win after a tense final. "
  "Supporters celebrated the success well into the night."
  ),
  "published_at": "2026-01-07T21:05:00Z",
  },
  {
  "title": "Incomplete record",
  "body": "",
  "published_at": "2026-01-08T00:00:00Z",
  },
  ],
  )

def summarize(results: Sequence[Analysis]) -> dict[str, object]:
  """Compute aggregate statistics over the analysed results."""
  if not results:
    return {"count": 0}

word_counts = [record.word_count for record in results]
sentiments = Counter(record.sentiment for record in results)
scores = [record.sentiment_score for record in results]

return {
  "count": len(results),
  "avg_words": round(statistics.mean(word_counts), 2),
  "median_words": statistics.median(word_counts),
  "avg_sentiment": round(statistics.mean(scores), 4),
  "sentiment_breakdown": dict(sentiments),
}

def print_report(results: Sequence[Analysis]) -> None:
  """Print a human-readable summary of the analysed results."""
  print("=" * 60)
  print("fndd - News Data Analysis Report")
  print("=" * 60)
  for record in results:
    print(f"- {record.title}")
    print(f" source : {record.source}")
    print(f" words : {record.word_count}")
    print(f" reading : {record.reading_minutes} min")
    print(f" sentiment : {record.sentiment} ({record.sentiment_score})")
    print(f" keywords : {', '.join(record.keywords) or 'n/a'}")
    print("-" * 60)
    summary = summarize(results)
    for key, value in summary.items():
      print(f"{key:>20}: {value}")
      print("=" * 60)

def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
  """Parse command-line arguments for the fndd CLI."""
  parser = argparse.ArgumentParser(
  prog="fndd",
  description="fndd - fast news data detection toolkit.",
  )
  parser.add_argument(
  "command",
  nargs="?",
  default="run",
  choices=("run", "summary"),
  help="Command to execute (default: run).",
  )
  parser.add_argument(
  "--export",
  type=Path,
  default=None,
  help="Optional path to write the JSON export.",
  )
  parser.add_argument(
  "--limit",
  type=int,
  default=0,
  help="Maximum number of items to process (0 = all).",
  )
  parser.add_argument(
  "--verbose",
  action="store_true",
  help="Enable debug logging.",
  )
  return parser.parse_args(argv)

def configure_logging(verbose: bool) -> None:
  """Configure application logging."""
  logging.basicConfig(
  level=logging.DEBUG if verbose else logging.INFO,
  format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
  )

def main(argv: Sequence[str] | None = None) -> int:
  """Application entry point."""
  args = parse_args(argv)
  configure_logging(args.verbose)

sources: list[Source] = [build_demo_source()]
results, stats = run_pipeline(sources, limit=args.limit)

if args.command == "summary":
  print(summarize(results))
else:
  print_report(results)

if args.export is not None:
  export_json(results, args.export)

if stats.failed:
  LOGGER.warning("%d record(s) failed to parse", stats.failed)

return 0

if name == "main":
  raise SystemExit(main())
  
