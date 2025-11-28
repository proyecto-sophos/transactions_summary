import argparse
import pandas as pd
import os

def parse_args():
    parser = argparse.ArgumentParser(description="Resumen de transacciones por método de pago y día.")
    parser.add_argument("--input", required=True, help="Archivo JSONL de entrada.")
    parser.add_argument("--output", required=True, help="Ruta del archivo Parquet de salida.")
    return parser.parse_args()

def read_json_auto(path):
    return pd.read_json(path, lines=True)

def normalize(df):
    df = df[df["status"] == "approved"].copy()
    df["payment_method_type"] = df["payment_method_type"].astype(str)
    df["amount"] = pd.to_numeric(df["amount_in_cents"], errors="coerce").fillna(0)
    df["day"] = pd.to_datetime(df["created_at"], utc=True).dt.date.astype(str)
    return df

def summarize(df):
    return (
        df.groupby(["payment_method_type", "day"], as_index=False)
        .agg(
            approved_tx_count=("status", "count"),
            approved_amount_total=("amount", "sum"),
        )
        .sort_values(["payment_method_type", "day"])
        .reset_index(drop=True)
    )

def main():
    args = parse_args()
    df = read_json_auto(args.input)
    df = normalize(df)
    summary = summarize(df)
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    summary.to_parquet(args.output, index=False)
    print(f" Resumen generado: {args.output}")

if __name__ == "__main__":
    main()
