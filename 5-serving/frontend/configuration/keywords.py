# The five supply-chain questions that replace the old risk-category multiselect.
# Each answer is a list of keyword strings; the processing layer matches them
# against article URLs / titles / keywords (processor.build_keyword_clause), and
# the question keys are the ones stored under the profile's "keywords" field.

KEYWORD_QUESTIONS = [
    ("sourcing",      "What are you sourcing?"),
    ("manufacturing", "What are you shipping for manufacturing?"),
    ("storage",       "What are you shipping for storage?"),
    ("delivery",      "What are you shipping for delivery?"),
    ("companies",     "Please list the names of all companies involved."),
]

MAX_KEYWORDS_PER_QUESTION = 100
