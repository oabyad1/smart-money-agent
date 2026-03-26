"""All LLM prompt templates for the 6-pass analysis pipeline."""

PASS_1_ENTITY_EXTRACTION = """You are analysing a document about fund manager {manager_name}.

Your task is ONLY to extract every company name and stock ticker mentioned
in the document. Nothing else.

Document:
{document_text}

Return a JSON object with this exact structure:
{{
  "tickers": ["AAPL", "MSFT"],
  "companies": ["Apple Inc", "Microsoft Corp"],
  "count": 2
}}

Return only the JSON. No explanation. No preamble."""


PASS_2_QUOTE_EXTRACTION = """You are analysing a document about fund manager {manager_name}.

These companies/tickers were identified in this document: {entities}

For each entity, find every sentence where {manager_name} expresses a view.
Extract the EXACT verbatim quote. Do not paraphrase. Do not summarise.
If you cannot find a direct quote, do not invent one — omit that entity.

Document:
{document_text}

Return a JSON array with this exact structure:
[
  {{
    "ticker": "AAPL",
    "company": "Apple Inc",
    "quotes": [
      "exact verbatim quote from the document",
      "another exact verbatim quote if present"
    ]
  }}
]

Return only the JSON. No explanation. No preamble."""


PASS_3_SENTIMENT_HEDGE = """You are analysing statements made by fund manager {manager_name}.

For each quote below, classify sentiment and detect hedges.

Quotes:
{quotes_json}

For each quote:
- sentiment: "bullish" | "bearish" | "neutral" | "hedged"
- conviction_level: "high" | "medium" | "low" | "unclear"
- hedge_words: list any hedge words present (but, however, although, except,
  if, unless, until, could, might, may, somewhat, partially, cautious,
  concerns, risks, uncertain, etc.)
- is_hypothetical: true if the statement is a hypothetical or condition,
  not a current view

Return a JSON array matching the input structure, adding these fields to
each quote object.

Return only the JSON. No explanation. No preamble."""


PASS_4_ABSENCE_REPORT = """You are analysing a document about fund manager {manager_name}.

Their currently known positions (as of the most recent 13F available
before today) are:
{known_positions}

The following tickers were mentioned in the document: {mentioned_tickers}

Your task:
1. List every known position that was NOT mentioned at all in this document
2. For any previously mentioned position that appears LESS enthusiastic than
   in prior statements, flag it
3. Note if any position was mentioned with noticeably different language than
   before (more cautious, less specific, hedged where previously unhedged)

Return a JSON object:
{{
  "not_mentioned": ["TICKER1", "TICKER2"],
  "tone_shift_flags": [
    {{
      "ticker": "AAPL",
      "observation": "Previously called a core holding, now only mentioned briefly"
    }}
  ],
  "accumulation_candidates": ["TICKER3"]
}}

Return only the JSON. No explanation. No preamble."""


PASS_5_13F_CROSSREFERENCE = """You are computing signal types for fund manager {manager_name}.

Public statements extracted from document (with sentiment):
{statements_json}

Known position changes from most recent 13F filing:
{position_changes_json}

Silence flags (large positions not mentioned):
{silence_flags}

For each ticker where you can match a statement to a position change,
classify the signal:

- "distribution": bullish/positive public statement + position was reduced
  or exited in the 13F
- "accumulation": no or minimal public mention + position was added to or
  initiated in 13F
- "contrarian": bearish/negative public statement + position was initiated
  or added in 13F

Only fire a signal if you have both a statement AND a position change to
match. Do not speculate.

Return a JSON array:
[
  {{
    "ticker": "AAPL",
    "signal_type": "distribution",
    "statement_sentiment": "bullish",
    "position_direction": "reduced_38pct",
    "raw_score": 0.85,
    "reasoning": "Strong bullish language, position reduced by 38% in same quarter"
  }}
]

Return only the JSON. No explanation. No preamble."""


PASS_6_RED_TEAM = """You are a skeptical senior analyst reviewing an analysis of {manager_name}.

The analysis produced these signals:
{signals_json}

The original document is:
{document_text}

Your job is adversarial: find what was missed or over-interpreted.

Specifically check:
1. Are there important statements in the document NOT captured in the signals?
2. Were any quotes taken out of context in a way that changes their meaning?
3. Are there hedges or qualifications that should lower confidence?
4. Are there any signals that seem like over-interpretation of vague language?
5. Are there absence patterns (positions not mentioned) that suggest
   accumulation that were missed?

Return a JSON object:
{{
  "missed_signals": [],
  "overinterpretations": [],
  "confidence_reductions": [
    {{"ticker": "AAPL", "reduce_by": 0.15, "reason": "Statement was clearly hypothetical"}}
  ],
  "additional_absence_flags": []
}}

Return only the JSON. No explanation. No preamble."""
