FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src/

# cda_etl uses flat imports (its own directory on the path); cda_expander is a
# proper package imported as cda_expander.*, so /app/src is on the path too.
# The two never import each other - see src/cda_expander/__init__.py.
ENV PYTHONPATH=/app/src/cda_etl:/app/src

# Default entry point is the ETL itself. The expander is a second entry point
# in the same image, run before the ETL to regenerate the literal-id config:
#     python -m cda_expander --base <config> --templates <templates> --out <generated>
CMD ["python", "src/cda_etl/main.py"]
