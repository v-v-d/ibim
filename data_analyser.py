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


def get_timedelta(raw_from_date, raw_to_date):
    from_date = pd.to_datetime(raw_from_date, format='%d.%m.%Y %H:%M:%S')
    to_date = pd.to_datetime(raw_to_date, format='%d.%m.%Y %H:%M:%S')
    return to_date - from_date


def load_df_to_excel(output_filepath, df_resolver):
    with pd.ExcelWriter(output_filepath) as writer:
        for sheet_name, df in df_resolver.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)


if __name__ == '__main__':
    persons_small_fpath = os.path.join('source', 'small_data_persons.json')
    persons_big_fpath = os.path.join('source', 'big_data_persons.json')

    # Make dataframe resolver with key as sheet name and value as dataframe
    df_resolver = get_df_resolver(
        persons_small_fpath, persons_big_fpath,
    )

    # Split small_data_persons and big_data_persons column 'Name'
    small_data_df = df_resolver['small_data_persons']
    small_data_df_split = get_df_with_split_column(
        small_data_df, 'Name', 'LastName', 'FirstName'
    )
    df_resolver.update({'small_data_persons': small_data_df_split})

    big_data_df = df_resolver['big_data_persons']
    big_data_df_split = get_df_with_split_column(
        big_data_df, 'Name', 'LastName', 'FirstName'
    )
    df_resolver.update({'big_data_persons': big_data_df_split})

    # Sort data by Name
    df_resolver['small_data_persons'].sort_values(
        by=['LastName'], inplace=True
    )
    df_resolver['big_data_persons'].sort_values(
        by=['FirstName'], inplace=True
    )

    # Find unique persons in the small_data_persons only by LastName
    unique_data_persons_by_last_name_df = (
        df_resolver['small_data_persons'].merge(
            df_resolver['big_data_persons'], how='outer', indicator=True,
            on=['LastName'], suffixes=['', '_']
        )
        .query("_merge=='left_only'")
        .dropna(how='all', axis=1)
        .drop(['_merge'], axis=1)
    )

    # Find absolutely unique persons in the small_data_persons
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

    # Make all persons dataframe
    all_persons_df = pd.concat([
        df_resolver['big_data_persons'], df_resolver['unique_persons']
    ])

    # Find namesakes by age rule
    age_mask = all_persons_df.groupby(['LastName'])['Age'].transform(
        lambda ages: [any(abs(age - ages) == 10) for age in ages]
    )
    namesakes_df = all_persons_df.loc[age_mask].sort_values(by=['LastName'])

    df_resolver.update({
        'namesakes_with_age_diff': namesakes_df,
    })

    # Find persons with bad names
    english_chars_mask = (
        all_persons_df['LastName'].str.contains('[a-zA-Z]', regex=True) |
        all_persons_df['FirstName'].str.contains('[a-zA-Z]', regex=True)
    )
    persons_with_bad_names_df = all_persons_df.loc[english_chars_mask]

    df_resolver.update({
        'persons_with_bad_names': persons_with_bad_names_df,
    })

    # Sort persons by contacts count
    contacts_big_fpath = os.path.join('source', 'big_data_contracts.json')
    contacts_df = pd.read_json(contacts_big_fpath)

    filtered_by_time_df = contacts_df.groupby(['Member1_ID'], group_keys=False).apply(
        lambda group: group[get_timedelta(group['From'], group['To']) > pd.Timedelta(minutes=5)]
    )

    filtered_and_sorted_df = (
        filtered_by_time_df.groupby(['Member1_ID'])
        .size()
        .reset_index(name='ContactsCount')
        .sort_values(['ContactsCount'], ascending=False)
    )

    joined_sorted_df = pd.merge(
        filtered_and_sorted_df, all_persons_df, how='left',
        left_on=['Member1_ID'], right_on=['ID']
    ).drop(['Member1_ID', 'ID', 'Age'], axis=1)

    df_resolver.update({
        'contacts_count': joined_sorted_df,
    })

    # Sort by total contacts duration
    total_times_series = (
        contacts_df.groupby(['Member1_ID'])
        .apply(
            lambda group: sum(
                [get_timedelta(row['From'], row['To']) for _, row in group.iterrows()],
                pd.Timedelta(0)
            )
        )
        .sort_values(ascending=False)
    )

    total_times_df = pd.DataFrame({
        'Member1_ID': total_times_series.index,
        'Time': total_times_series.values
    })

    joined_times_df = pd.merge(
        total_times_df, all_persons_df, how='left',
        left_on=['Member1_ID'], right_on=['ID']
    ).drop(['Member1_ID', 'ID', 'Age'], axis=1)

    df_resolver.update({
        'total_contacts_duration': joined_times_df,
    })

    # Find age group with most common contacts
    # Assume that the most common contact is from 0.75max to max.
    filtered_and_sorted_df = (
        pd.merge(
            filtered_and_sorted_df, all_persons_df, how='left',
            left_on=['Member1_ID'], right_on=['ID']
        )
        .drop(['Member1_ID', 'ID'], axis=1)
    )

    bins = [13, 17, 20, 55, 75, 110]
    labels = ['Подросток', 'Юноша', 'Зрелый', 'Пожилой', 'Старческий']
    filtered_and_sorted_df['AgeGroup'] = pd.cut(
        filtered_and_sorted_df['Age'], bins=bins, labels=labels, right=False
    )

    filtered_by_contacts_df = (
        filtered_and_sorted_df.groupby(['ContactsCount'], group_keys=False)
        .apply(lambda group: group[
            group['ContactsCount'] >= filtered_and_sorted_df['ContactsCount'].max() * .75
        ])
    )

    age_group_df = pd.DataFrame({
        'AgeGroup': filtered_by_contacts_df['AgeGroup'].mode(),
    })

    df_resolver.update({
        'most_common_contacts_age_group': age_group_df,
    })

    # load data to excel
    if not os.path.exists('result'):
        os.makedirs('result')

    output_filepath = os.path.join('result', 'data_persons.xlsx')
    load_df_to_excel(output_filepath, df_resolver)
