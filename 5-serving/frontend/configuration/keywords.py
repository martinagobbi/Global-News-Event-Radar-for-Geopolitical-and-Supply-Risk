# The five supply-chain questions that replace the old risk-category multiselect.
# Each answer is a list of SINGLE-WORD keywords; the processing layer splits each
# into tokens and matches them against the article URL, title and extracted
# keywords (processor.build_keyword_clause). The question keys are the ones stored
# under the profile's "keywords" field.
#
# Single words are enforced by the entry form: a multi-word phrase would have to
# appear with all of its words in the same article, which is a far narrower filter
# than users expect. "silicon" and "wafers" are added as two separate items.

KEYWORD_QUESTIONS = [
    ("sourcing",      "What are you sourcing?"),
    ("manufacturing", "What are you shipping for manufacturing?"),
    ("storage",       "What are you shipping for storage?"),
    ("delivery",      "What are you shipping for delivery?"),
    ("companies",     "Please list the names of all companies involved."),
]

MAX_KEYWORDS_PER_QUESTION = 1000

# Above this many entries the list is folded into an expander: rendering a
# remove button per item is what makes a long list slow, not holding the words.
KEYWORDS_SHOWN_BEFORE_FOLD = 50
