from collections import Counter
from nltk.util import ngrams
import re
import pandas as pd
import itertools
import numpy as np


def parse_file(file_name):
    with open(file_name, 'r') as file:
        names_list = []
        for line in file:
            line = line.replace('\n', '')
            if re.match(r"^.*,.*$", line):
                names_list.append(line)

        cleaned_strings_list = ' '.join(str(name) for name in names_list)
        return cleaned_strings_list


def count_authors(cleaned_strings_list):
    n_gram = 2
    n_grams_list = ngrams(cleaned_strings_list.split(), n_gram)
    full_names_list = []
    for bigram in n_grams_list:
        full_names_list.append(' '.join(bigram))

    last_authors_list = []
    full_names_list_new = []
    not_last_authors_list = []
    for full_name in full_names_list:
        if not re.match(r".*, .*", full_name):
            if full_name.endswith(","):
                not_last_authors_list.append(full_name)
            else:
                last_authors_list.append(full_name.strip(":"))
            full_names_list_new.append(re.sub(r"[:,]", r"", full_name))

    return Counter(full_names_list_new), Counter(last_authors_list)


def make_df(total_full_names, last_authors_count, conference_dict):
    df = pd.DataFrame(
        columns=['name', 'full_count', 'search_term' 'conf_str', 'last_authors_count', 'CVPR2017', 'CVPR2018', 'CVPR2019', 'ICML2017',
                 'ICML2018', 'ICML2019', 'NIPS2017', 'NIPS2018', 'NIPS2019'])

    for name in total_full_names:
        #print(name)
        df = df.append({
            "name": name,
            "full_count": total_full_names[name],
            'conf_str': conference_dict[name],
            "last_authors_count": last_authors_count.get(name, 0)

        }, ignore_index=True)
    df['search_term'] = df['name'].astype(str) + ' scholar'
    df['CVPR2017'] = np.where(df['conf_str'].str.contains('CVPR2017', case=False, na=False), 'y', 'n')
    df['CVPR2018'] = np.where(df['conf_str'].str.contains('CVPR2018', case=False, na=False), 'y', 'n')
    df['CVPR2019'] = np.where(df['conf_str'].str.contains('CVPR2019', case=False, na=False), 'y', 'n')
    df['ICML2017'] = np.where(df['conf_str'].str.contains('ICML2017', case=False, na=False), 'y', 'n')
    df['ICML2018'] = np.where(df['conf_str'].str.contains('ICML2018', case=False, na=False), 'y', 'n')
    df['ICML2019'] = np.where(df['conf_str'].str.contains('ICML2019', case=False, na=False), 'y', 'n')
    df['NIPS2017'] = np.where(df['conf_str'].str.contains('NIPS2017', case=False, na=False), 'y', 'n')
    df['NIPS2018'] = np.where(df['conf_str'].str.contains('NIPS2018', case=False, na=False), 'y', 'n')
    df['NIPS2019'] = np.where(df['conf_str'].str.contains('NIPS2019', case=False, na=False), 'y', 'n')

    del df['conf_str']

    return df


if __name__ == "__main__":
    LOCAL_PATH = '/Users/samski/PycharmProjects/nameCounter/'
    OUT_FILE = '/Users/samski/PycharmProjects/nameCounter/final_df.csv'
    TOTAL_AUTH_MIN = 15

    conf_list = ((LOCAL_PATH + 'CVPR2017.txt', 'CVPR2017'), (LOCAL_PATH + 'CVPR2018.txt', ' CVPR2018'),
                 (LOCAL_PATH + 'CVPR2019.txt', ' CVPR2019'), (LOCAL_PATH + 'ICML2017.txt', ' ICML2017'),
                 (LOCAL_PATH + 'ICML2018.txt', ' ICML2018'), (LOCAL_PATH + 'ICML2019.txt', ' ICML2019'),
                 (LOCAL_PATH + 'NIPS2017.txt', ' NIPS2017'), (LOCAL_PATH + 'NIPS2018.txt', ' NIPS2018'),
                 (LOCAL_PATH + 'NIPS2019.txt', ' NIPS2019'))
    path = conf_list[0][0]

    total_full_names = Counter()
    total_last_auth = Counter()
    conf_per_auth_dict = dict()

    for file_str_set in conf_list:
        cleaned_strings_list = parse_file(file_str_set[0])
        full_names_count, last_authors_count = count_authors(cleaned_strings_list)

        for author in list(full_names_count.elements()):
            if author in conf_per_auth_dict:
                conf_per_auth_dict[author] += file_str_set[1]
            else:
                conf_per_auth_dict[author] = file_str_set[1]

        total_full_names += full_names_count
        total_last_auth += last_authors_count


    high_publish_auth_count = Counter(
        el for el in total_full_names.elements() if total_full_names[el] >= TOTAL_AUTH_MIN)

    result_df = make_df(high_publish_auth_count, total_last_auth, conf_per_auth_dict)
    print(result_df.head(5))

    result_df.to_csv(OUT_FILE)


