import os

import pandas as pd


def get_df_resolver(*filepaths):
    df_resolver = {}

    for filepath in filepaths:
        df = pd.read_json(filepath)
        filename = get_filename_from_path(filepath)
        df_resolver.update({filename: df})

    return df_resolver


def get_filename_from_path(filepath):
    filename, _ = os.path.basename(filepath).split('.')
    return filename


def get_df_with_split_column(df, col_name, *new_col_names):
    temp_df = df[col_name].str.split(expand=True)
    for idx, new_col_name in enumerate(new_col_names):
        df[new_col_name] = temp_df[idx]
    df.drop(columns=[col_name], inplace=True)

    return df


def load_df_to_excel(output_filepath, df_resolver):
    with pd.ExcelWriter(output_filepath) as writer:
        for sheet_name, df in df_resolver.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)


persons_small_fpath = os.path.join('source', 'small_data_persons.json')
persons_big_fpath = os.path.join('source', 'big_data_persons.json')
# contacts_small_fpath = os.path.join('source', 'small_data_contracts.json')
# contacts_big_fpath = os.path.join('source', 'big_data_contracts.json')

df_resolver = get_df_resolver(
    persons_small_fpath, persons_big_fpath,
    # contacts_small_fpath, contacts_big_fpath
)

# Split small_data_persons column 'Name' and update df_resolver
small_data_df = df_resolver['small_data_persons']
small_data_df_split = get_df_with_split_column(
    small_data_df, 'Name', 'LastName', 'FirstName'
)
df_resolver.update({'small_data_persons': small_data_df_split})

# Split big_data_persons column 'Name' and update df_resolver
big_data_df = df_resolver['big_data_persons']
big_data_df_split = get_df_with_split_column(
    big_data_df, 'Name', 'LastName', 'FirstName'
)
df_resolver.update({'big_data_persons': big_data_df_split})

df_resolver['small_data_persons'].sort_values(by=['LastName'], inplace=True)
df_resolver['big_data_persons'].sort_values(by=['FirstName'], inplace=True)

unique_data_persons_by_last_name_df = (
    df_resolver['small_data_persons'].merge(
        df_resolver['big_data_persons'], how='outer', indicator=True,
        on=['LastName'], suffixes=['', '_']
        # on=['Age', 'FirstName', 'LastName'], suffixes=['', '_']
    )
    .query("_merge=='left_only'")
    .dropna(how='all', axis=1)
    .drop(['_merge'], axis=1)
)

unique_data_persons_df = (
    df_resolver['small_data_persons'].merge(
        df_resolver['big_data_persons'], how='outer', indicator=True,
        on=['Age', 'FirstName', 'LastName'], suffixes=['', '_']
    )
    .query("_merge=='left_only'")
    .dropna(how='all', axis=1)
    .drop(['_merge'], axis=1)
)

df_resolver.update({
    'unique_persons_by_last_name': unique_data_persons_by_last_name_df,
    'unique_persons': unique_data_persons_df,
})

all_persons_df = pd.concat([
    df_resolver['big_data_persons'], df_resolver['unique_persons']
])

age_mask = all_persons_df.groupby(['LastName'])['Age'].transform(
    lambda ages: [any(abs(age - ages) == 10) for age in ages]
)
namesakes_df = all_persons_df.loc[age_mask].sort_values(by=['LastName'])

df_resolver.update({
    'namesakes_with_age_diff': namesakes_df,
})

english_chars_mask = (
    all_persons_df['LastName'].str.contains('[a-zA-Z]', regex=True) |
    all_persons_df['FirstName'].str.contains('[a-zA-Z]', regex=True)
)
persons_with_bad_names_df = all_persons_df.loc[english_chars_mask]

df_resolver.update({
    'persons_with_bad_names': persons_with_bad_names_df,
})

# load data to excel
if not os.path.exists('result'):
    os.makedirs('result')

output_filepath = os.path.join('result', 'data_persons.xlsx')
load_df_to_excel(output_filepath, df_resolver)
