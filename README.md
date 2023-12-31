﻿# Microsoft Audit Log Unpacker

Microsoft Audit Log Unpacker is a Python utility that helps unpack nested JSON objects within CSV files. This is particularly useful when dealing with Microsoft Audit Log exports, which are often formatted as CSVs with nested JSON.

## Requirements

To use Microsoft Audit Log Unpacker, you need to have Python 3.6+ installed on your system.

You will also need the pandas library. You can install it using pip:

```
pip install pandas
```

## Usage

To use the Microsoft Audit Log Unpacker, navigate to the directory containing the script, and then run the script using the -f argument followed by the path to your CSV file:

```
python MSAuditLogUnpacker.py -f <path_to_your_CSV_file>
```

This will create a new CSV file in the same directory as your original file, with the nested JSON objects unpacked.

## Contributing

Contributions are welcome! Please feel free to submit a pull request or create an issue.

## License

This project is licensed under the MIT License.
