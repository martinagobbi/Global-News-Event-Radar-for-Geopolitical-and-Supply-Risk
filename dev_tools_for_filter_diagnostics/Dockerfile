# Filter diagnostics: explains why each gold article reached a user's pool.
# The build context is the REPOSITORY ROOT, so the image can reuse the real
# filter code rather than duplicating it — the same approach bootstrap/ takes:
#   * 2-parsing/parser.py     -> the bronze->silver criteria (F1, F2, F3)
#   * 4-processing/           -> territory codes and keyword tokenisation
FROM python:3.11-slim

WORKDIR /app

COPY 4-processing/requirements.txt ./req-processing.txt
RUN pip install --no-cache-dir -r req-processing.txt

COPY 2-parsing/parser.py                              /app/2-parsing/parser.py
COPY 4-processing/processor.py                        /app/4-processing/processor.py
COPY 4-processing/countries.py                        /app/4-processing/countries.py
COPY 4-processing/country_codes.json                  /app/4-processing/country_codes.json
COPY 4-processing/territories_other_than_sovereign_countries.txt \
                                                      /app/4-processing/
COPY dev_tools_for_filter_diagnostics/explain_gold.py /app/dev_tools_for_filter_diagnostics/

CMD ["python", "dev_tools_for_filter_diagnostics/explain_gold.py"]
