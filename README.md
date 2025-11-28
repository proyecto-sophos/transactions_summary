# Transactions Summary

Este proyecto procesa un archivo JSON/JSONL de transacciones y genera un resumen en formato Parquet.

## Instalación
```bash
pip install -r requirements.txt
```

## Uso
```bash
python src/process_transactions.py --input tests/sample.jsonl --output output/summary.parquet
```

## Estructura
- src/ → Script principal
- tests/ → Dataset de prueba
- output/ → Archivo de salida
