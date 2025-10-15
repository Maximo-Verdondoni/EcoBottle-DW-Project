# main.py
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent))
from etl.load.load import run_pipeline

def main():
    print("Iniciando pipeline de Data Warehouse...")
    run_pipeline()
    print("✅ Pipeline completada con éxito!")

if __name__ == "__main__":
    main()
