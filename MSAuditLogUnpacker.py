import argparse
import pandas as pd
import json


def expand_json(data, json_cols):
    for json_col in json_cols:
        # Convert the json strings into dictionaries
        data[json_col] = data[json_col].apply(lambda x: json.loads(x) if x is not None else {})
        # Normalize the dictionaries into dataframes and join them with the original dataframe
        # Specify suffixes to differentiate between overlapping columns
        data = data.join(pd.json_normalize(data[json_col]), lsuffix="AuditData.")
        # Drop the original json columns
        data = data.drop(columns=json_col)
    return data


def parse_csv_file(file_path):
    df = pd.read_csv(file_path)

    # Identify columns that contain JSON data
    json_cols = [col for col in df.columns if df[col].apply(is_json).any()]

    # Expand those columns
    df = expand_json(df, json_cols)

    # Save the expanded dataframe
    output_path = file_path.replace(".csv", "_unpacked.csv")
    df.to_csv(output_path, index=False)
    print(f"Unpacked data saved to {output_path}.")


def is_json(x):
    if not x or type(x) != str:
        return False
    try:
        json.loads(x)
        return True
    except:
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", required=True, help="The path to the csv file to be processed.")
    args = parser.parse_args()

    parse_csv_file(args.file)


if __name__ == "__main__":
    main()
