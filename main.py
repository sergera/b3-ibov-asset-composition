import pandas as pd
import os
import requests

def fetch_file_data(file_name):
    with open(file_name, 'r', encoding='cp1252') as file:
        content = file.readlines()
    return content

def preprocess_file_data(lines):
    modified_lines = lines[2:-2]  # Remove the first two and the last two lines
    modified_content = ''.join([line.replace(',', '.') for line in modified_lines])  # Replace commas with dots
    return modified_content

def save_to_temp_file(content, temp_file_name):
    with open(temp_file_name, 'w', encoding='cp1252') as temp_file:
        temp_file.write(content)

def process_temp_file_data(file_name):
    # Read the entire line as a single column, explicitly skipping the header row from the CSV
    df = pd.read_csv(file_name, header=None, encoding='cp1252')

    # Split the single column into multiple columns based on the semicolon delimiter
    df = df[0].str.split(';', n=4, expand=True)

    # Manually set column names (since we skipped the original header row)
    columns = ['Código', 'Ação', 'Tipo', 'Qtde. Teórica', 'Part. (%)']
    df.columns = columns

    # Since we manually set the column names, no row in 'df' should now be the original header row

    # Handle the 'Part. (%)' column
    df['Part. (%)'] = df['Part. (%)'].str.rstrip(';').astype(float)

    # Sort the DataFrame based on 'Part. (%)' column in descending order
    sorted_df = df.sort_values(by='Part. (%)', ascending=False)

    # Reset the index to reflect the new order and add the 'rank' column
    sorted_df.reset_index(drop=True, inplace=True)
    sorted_df.index += 1  # Make the index start from 1
    sorted_df.insert(0, 'Rank', sorted_df.index)  # Insert the index as the 'Rank' column

    return sorted_df


def fetch_api_data():
    url = "https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/eyJsYW5ndWFnZSI6ImVuLXVzIiwicGFnZU51bWJlciI6MSwicGFnZVNpemUiOjEyMCwiaW5kZXgiOiJJQk9WIiwic2VnbWVudCI6IjEifQ=="
    response = requests.get(url)
    data = response.json()['results']
    return data


def process_api_data(data):
    # Convert the data into a DataFrame
    df = pd.DataFrame(data)

    # Rename columns to match the original CSV format
    df.rename(columns={'cod': 'Código', 'asset': 'Ação', 'type': 'Tipo', 'theoricalQty': 'Qtde. Teórica',
                       'part': 'Part. (%)'}, inplace=True)

    # Convert 'Part. (%)' to float
    df['Part. (%)'] = df['Part. (%)'].astype(float)

    # Sort the DataFrame by 'Part. (%)' in descending order
    sorted_df = df.sort_values(by='Part. (%)', ascending=False)

    # Reset index to get a ranking column starting from 1
    sorted_df.reset_index(drop=True, inplace=True)
    sorted_df.index += 1
    sorted_df.insert(0, 'Rank', sorted_df.index)

    # Reorder columns to keep 'Part. (%)' as the last column
    columns_order = ['Rank', 'Código', 'Ação', 'Tipo', 'Qtde. Teórica', 'Part. (%)']
    sorted_df = sorted_df[columns_order]

    return sorted_df


def save_sorted_df_to_files(sorted_df):
    sorted_df.to_csv('sorted_ibov_stocks.csv', index=False, sep=';')
    sorted_df.to_excel('sorted_ibov_stocks.xlsx', index=False)
    print("The files 'sorted_ibov_stocks.csv' and 'sorted_ibov_stocks.xlsx' have been created.")


if __name__ == '__main__':
    file_basename = 'ibov_stocks'
    original_file_name = f'{file_basename}.csv'
    temp_file_name = f'{file_basename}_temp.csv'
    if not os.path.exists(original_file_name):
        data = fetch_api_data()
        sorted_df = process_api_data(data)
    else:
        lines = fetch_file_data(original_file_name)
        preprocessed_data = preprocess_file_data(lines)
        save_to_temp_file(preprocessed_data, temp_file_name)
        sorted_df = process_temp_file_data(temp_file_name)
        os.remove(temp_file_name)

    save_sorted_df_to_files(sorted_df)

