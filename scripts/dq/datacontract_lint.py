import os
from datacontract.data_contract import DataContract

# Run lint check
def check_datacontract(out_path):
    try:
        result = DataContract(out_path).lint()
        for r in result.checks:
            print(f"{r.name:<40} {r.result}")
    except Exception as e:
        print(f"❌ Linting failed for {out_path}: {e}")



def main():
    #parser = argparse.ArgumentParser(description="Generate YAML data contracts from Excel models.")


    input_path = os.getcwd() +"/dataquality/datacontracts/"

    yaml = [f for f in os.listdir(input_path) if f.lower().endswith(".yaml")]
    if not yaml:
        print(f"⚠️ No .xlsx files found in {input_path}")
        return

    for fname in yaml:
        path = os.path.join(input_path, fname)
        try:
            check_datacontract(path)
        except Exception as e:
            print(f"❌ Error processing {fname}: {e}")


if __name__ == "__main__":
    main()
