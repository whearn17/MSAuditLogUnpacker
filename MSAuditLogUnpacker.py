import pandas as pd
import json
import argparse
import os


# Function to expand JSON column
def expand_json(data, json_cols, prefix=''):
    for json_col in json_cols:
        # Extract column
        col = data[json_col]
        # If the data type of the column is string, load it as JSON, else just use the dictionary
        if data[json_col].dtype == object:
            col = col.apply(lambda x: x if isinstance(x, dict) else json.loads(x))
        # Get keys for JSON
        keys = set().union(*col.apply(lambda x: list(x.keys())))
        # Create new columns for each key
        for key in keys:
            data[prefix + key] = col.apply(lambda x: x.get(key))
        data.drop(json_col, axis=1, inplace=True)
        # Check if any new JSON columns
        new_json_cols = [prefix + key for key in keys if isinstance(data[prefix + key].dropna().values[0], dict)]
        for new_json_col in new_json_cols:
            # Generate the new prefix using the current key
            new_prefix = prefix + new_json_col.split('.')[-1] + '.'
            data = expand_json(data, [new_json_col], prefix=new_prefix)
    return data


def parse_csv_file(file_path, unique_column, json_column):
    df = pd.read_csv(file_path)
    unique_values = df[unique_column].unique()

    output_dir = os.path.splitext(file_path)[0] + "_parsed"
    os.makedirs(output_dir, exist_ok=True)

    for unique_value in unique_values:
        df_event = df[df[unique_column] == unique_value].copy()
        df_event = expand_json(df_event, [json_column])
        df_event.to_csv(os.path.join(output_dir, f"{unique_value}.csv"), index=False)


# Main function
def main():
    parser = argparse.ArgumentParser(description="Parse CSV logs with JSON fields")
    parser.add_argument('-c', type=str, required=True, help="The column to filter by for event types")
    parser.add_argument('-j', type=str, required=True, help="The column containing JSON objects to parse")
    parser.add_argument('-f', type=str, required=True, help="The CSV file to parse")
    args = parser.parse_args()
    parse_csv_file(args.f, args.c, args.j)


if __name__ == "__main__":
    main()