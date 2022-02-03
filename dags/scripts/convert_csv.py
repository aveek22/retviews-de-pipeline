import pandas as pd
import os


def encode_csv_utf8():
    try:
        # remove_raw_file()
        print(f'Working directory: {os.getcwd()}')

        input_file = 'data/superstore_source.csv'
        output_file = 'output/superstore_raw.csv'

        print(f'Reading from CSV file: {input_file}')
        df = pd.read_csv(input_file, encoding='ISO-8859-1')

        print(f'Exporting CSV file with UTF-8 encoding: {output_file}')
        df.to_csv(output_file, encoding='utf-8', index=False)
        print(f'Encoding Successful.')
    except Exception as error:
        print(f'Error in encoding: {str(error)}')


# def remove_raw_file():
#     if os.path.exists(output_file) and os.path.isfile(output_file):
#         os.remove(output_file)
#         print(f'File Deleted')
#     else:
#         print(f'File not found')


if __name__ == '__main__':
    encode_csv_utf8()
