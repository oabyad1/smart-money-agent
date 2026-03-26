"""Individual pass implementations for the 6-pass analysis pipeline."""
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import anthropic
from analysis.prompts import (
    PASS_1_ENTITY_EXTRACTION, PASS_2_QUOTE_EXTRACTION, PASS_3_SENTIMENT_HEDGE,
    PASS_4_ABSENCE_REPORT, PASS_5_13F_CROSSREFERENCE, PASS_6_RED_TEAM,
)

logger = logging.getLogger(__name__)

HAIKU = "claude-haiku-4-5-20251001"
SONNET = "claude-sonnet-4-6"
MAX_TOKENS = 2000

# Passes 1-4 are mechanical extraction/classification → cheap Haiku.
# Passes 5-6 require cross-referencing and adversarial reasoning → Sonnet.
_PASS_MODEL = {
    1: HAIKU,
    2: HAIKU,
    3: HAIKU,
    4: HAIKU,
    5: SONNET,
    6: SONNET,
}


def _call_claude(client: anthropic.Anthropic, prompt: str, pass_num: int,
                 model: str | None = None) -> str:
    """Call Claude API and return the text response."""
    message = client.messages.create(
        model=model or _PASS_MODEL.get(pass_num, SONNET),
        max_tokens=MAX_TOKENS,
        temperature=0,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text


def _parse_json_response(raw: str, pass_num: int, client: anthropic.Anthropic) -> any:
    """Parse JSON from Claude response. Retries once with explicit instruction if needed."""
    text = raw.strip()

    # Strip markdown code fences if present
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        logger.warning("Pass %d: invalid JSON, retrying. Raw: %s", pass_num, raw[:200])
        retry_prompt = (
            f"Your previous response was not valid JSON. "
            f"Return only the JSON with no other text.\n\n{raw}"
        )
        retry_raw = _call_claude(client, retry_prompt, pass_num, model=HAIKU)
        retry_text = retry_raw.strip()
        if retry_text.startswith("```"):
            lines = retry_text.splitlines()
            retry_text = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])
        try:
            return json.loads(retry_text)
        except json.JSONDecodeError as e:
            logger.error("Pass %d: retry also failed. Raw: %s. Error: %s",
                        pass_num, retry_raw[:300], e)
            raise


def run_pass_1(client: anthropic.Anthropic, document_text: str, manager_name: str) -> dict:
    """Pass 1: Entity extraction."""
    prompt = PASS_1_ENTITY_EXTRACTION.format(
        manager_name=manager_name,
        document_text=document_text[:15000],  # truncate for token budget
    )
    raw = _call_claude(client, prompt, 1)
    return _parse_json_response(raw, 1, client)


def run_pass_2(client: anthropic.Anthropic, document_text: str, manager_name: str,
               entities: dict) -> list:
    """Pass 2: Verbatim quote extraction."""
    entities_str = ", ".join(entities.get("tickers", []) + entities.get("companies", []))
    prompt = PASS_2_QUOTE_EXTRACTION.format(
        manager_name=manager_name,
        entities=entities_str,
        document_text=document_text[:15000],
    )
    raw = _call_claude(client, prompt, 2)
    return _parse_json_response(raw, 2, client)


def run_pass_3(client: anthropic.Anthropic, quotes: list, manager_name: str) -> list:
    """Pass 3: Sentiment and hedge classification."""
    prompt = PASS_3_SENTIMENT_HEDGE.format(
        manager_name=manager_name,
        quotes_json=json.dumps(quotes, indent=2),
    )
    raw = _call_claude(client, prompt, 3)
    return _parse_json_response(raw, 3, client)


def run_pass_4(client: anthropic.Anthropic, document_text: str, manager_name: str,
               known_positions: list, mentioned_tickers: list) -> dict:
    """Pass 4: Absence/silence report."""
    positions_str = json.dumps([
        {"ticker": p["ticker"], "value_usd": p.get("value_usd"), "shares": p.get("shares")}
        for p in known_positions[:50]  # top 50 positions
    ], indent=2)

    prompt = PASS_4_ABSENCE_REPORT.format(
        manager_name=manager_name,
        known_positions=positions_str,
        mentioned_tickers=json.dumps(mentioned_tickers),
    )
    raw = _call_claude(client, prompt, 4)
    return _parse_json_response(raw, 4, client)


def run_pass_5(client: anthropic.Anthropic, manager_name: str, statements: list,
               position_changes: list, silence_flags: list) -> list:
    """Pass 5: 13F cross-reference and signal generation."""
    prompt = PASS_5_13F_CROSSREFERENCE.format(
        manager_name=manager_name,
        statements_json=json.dumps(statements, indent=2),
        position_changes_json=json.dumps(position_changes[:30], indent=2),
        silence_flags=json.dumps(silence_flags, indent=2),
    )
    raw = _call_claude(client, prompt, 5)
    return _parse_json_response(raw, 5, client)


def run_pass_6(client: anthropic.Anthropic, manager_name: str, signals: list,
               document_text: str) -> dict:
    """Pass 6: Red team review."""
    prompt = PASS_6_RED_TEAM.format(
        manager_name=manager_name,
        signals_json=json.dumps(signals, indent=2),
        document_text=document_text[:8000],  # shorter for this pass
    )
    raw = _call_claude(client, prompt, 6)
    return _parse_json_response(raw, 6, client)
