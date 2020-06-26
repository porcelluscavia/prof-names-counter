#Inspired by Sebastian Gehrmann's dblp-pub scraper:  https://github.com/sebastianGehrmann/dblp-pub

from bs4 import BeautifulSoup
import pandas as pd
import requests
import re
import scholarly


STRINGS_FOR_TEST = ["venue:NeurIPS: year:2019: "]
DBLP_BASE_URL = 'https://dblp.uni-trier.de'
PUB_SEARCH_URL = DBLP_BASE_URL + "/search/publ/api"
JOURN_SEARCH_URL = DBLP_BASE_URL + "/db/journals/"
JOURN_API_URL = PUB_SEARCH_URL + '?q=toc%3Adb/journals/'
APPENDAGE_STR = '.bht%3A&h=1000&format=xml'
HITS_NUMBER = 1000

# confs and start years
# CONFERENCES = {'NeurIPS': 2019, 'CVPR': 2019, 'NIPS': 2017}
CONFERENCES = {'NeurIPS': 2018, 'NIPS': 1987, 'ICLR': 2013, 'CVPR': 1988, 'ECCV': 1990, 'ICCV':1988, 'ICRA': 1984, 'IROS': 1988, 'IJCAI': 1969, 'KDD': 1994, 'ICML': 1988}
END_YEAR = 2021

JOURNALS = {'TPAMI': 1979, 'TKDE': 1989, 'JMLR': 2003} #tpami = pami?
JOURNAL_NAMES_1 = ['TPAMI', 'TKDE', 'JMLR']
JOURNAL_NAMES = {'IEEE Trans. Pattern Anal. Mach. Intell.', 'IEEE Trans. Knowl. Data Eng.', 'J. Mach. Learn. Res.'}
JOURNALS_DICT = {'IEEE Trans. Pattern Anal. Mach. Intell.': 'TPAMI', 'IEEE Trans. Knowl. Data Eng.': 'TKDE', 'J. Mach. Learn. Res.':'JMLR'}



def query_db(pub_string=STRINGS_FOR_TEST, first_hit=0):
    '''
    Returns the BeautifulSoup object of a query to DBLP.

    :param pub_string: A list of strings of keywords
    :return: BeautifulSoup: A BeautifulSoup Object
    '''
    resp = requests.get(PUB_SEARCH_URL, params={'q': pub_string, 'h': HITS_NUMBER, 'f': first_hit})
    print(resp.url)
    soup = BeautifulSoup(resp.content, features="lxml")
    # print(soup.prettify())
    return soup


def get_volume_nums_by_year(journals=JOURNALS, journal_names = JOURNAL_NAMES_1):
    '''
    For journal (!! not conference) lookup: loop through all years a journal was published and extract all authors+info into a dataframe.

    :param journals: A dictionary of desired {journals: start year} pairs
    :param journal_names: A list of journal names as they should appear in the df columns; sometimes different from what is presented on DBPL
    :return: A pandas dataframe populated with all the authors in all the years of the journals

    todo:: Needs error handling very badly- can break easily with a bad URL
    '''

    main_df = setup_main_df(journal_names)
    for journal, start_year in journals.items():
        journal = journal.lower()
        resp = requests.get(JOURN_SEARCH_URL + journal)
        soup = BeautifulSoup(resp.content, features="lxml")
        for pub in soup.find_all('li'):
            if pub.string:
                for year in range(start_year, END_YEAR):
                    if str(year) in pub.string:
                        number = re.search('\d+', pub.string)
                        version = number.group()
                        url = make_full_journal_url(journal, version)
                        resp = requests.get(url)
                        soup = BeautifulSoup(resp.content, features="lxml")
                        new_df = search(soup, journals=True)
                        # new_df = add_counter_column(new_df)
                        print(new_df.head())
                        print(len(new_df))
                        main_df = pd.concat([main_df, new_df], ignore_index=True)

    main_df = main_df.sort_values('Author')
    return main_df



def search(soup, journals = False, journals_dict = JOURNALS_DICT):
    '''
    Makes separate dataframe entries for each author.
    :param journals: A boolean to differentiate from when we are pulling from conferences or journals (organized differently on DBPL)
    :param journals_dict: A dictionary of desired {journals: start year} pairs
    :return: A pandas dataframe populated with all the authors the authors from one specific venue

    todo:: Condense into count here too? Yeah, let's just do a total count'''

    author_name = 'n'
    where = 'n'
    year = 0

    pub_list_data = []

    for hit in soup.find_all('hit'):
        for auth in hit.find_all('author'):
            author_name = auth.string
            where = hit.venue.string
            if journals:
                if where in journals_dict:
                    where = journals_dict.get(where)
            year = where + hit.year.string  # future thoughts: these don't have to be accessed every time
            pub_data = {'Author': author_name, 'Year': year, where: 1}  # one-hotting for conference
            pub_list_data.append(pub_data)
    # here: condense names, add a count column <-- this will have to be updated when adding new conferences; check if it exists first; otherwise add it
    return pd.DataFrame(pub_list_data)


def year_conf_looper(confs=CONFERENCES):
    '''
    For conference (!! not journal) lookup: loop through all years of a conference and extract all authors+info into a dataframe.

    :param confs: A dictionary of desired {conference: start year} pairs

    :return: A pandas dataframe populated with all the authors in all the years of the journals

    notes:: sister method to get_volume_nums_by_year, but for conferences
    notes:: inclusive of end year (?)
    '''

    main_df = setup_main_df(confs)
    print(main_df.columns)
    for conf, start_year in confs.items():
        for year in range(start_year, END_YEAR):
            print('venue:' + conf + ':' + ' year:' + str(year) + ": ")
            new_df = find_all_results_per_conf(['venue:' + conf + ':' + 'year:' + str(year) + ": "])
            main_df = pd.concat([main_df, new_df], ignore_index=True)
    main_df = main_df.sort_values('Author')
    return main_df


def find_all_results_per_conf(search_string=STRINGS_FOR_TEST):
    '''
    Solves the issue of the 1000-hit cap on the API for bandwidth purposes from DBLP. If there are more than 1000 hits, start from 1001.

    :param search_string: What to search for in the search bar of DBLP
    :return: A pandas dataframe populated with all authors from a single year at a conference.

    notes:: Does not account for conferences with > 2000 entries (so far, don't exist). Eventually generalize to > 2000 with a modulo/division while loop
    '''

    final_df_1 = pd.DataFrame()
    soup = query_db(search_string)
    hits_tag = soup.hits
    hits_number = int(hits_tag['total'])
    print(hits_number)
    if hits_number > 0:
        final_df_1 = search(soup)
        print(final_df_1.head())
        if hits_number > 1000:
            new_soup = query_db(search_string, first_hit=1001)
            next_df = search(new_soup)
            frames = [final_df_1, next_df]
            final_df_1 = pd.concat(frames)
        final_df_1 = add_counter_column(final_df_1) #does this work as intended? before the final merged df of venues and jours
    return final_df_1

def postprocess_df(df, journals = False, journals_dict = JOURNALS_DICT):
    '''
    Change floats to ints, combine all years active into one string, add sum over publication count.

    :param journals: A boolean to differentiate from when we are pulling from conferences or journals (organized differently on DBPL)
    :param journal_names: A list of journal names as they should appear in the df columns; sometimes different from what is presented on DBPL
    :return: A postprocessed pandas dataframe

    notes:: Where are we condensing authors?
    '''

    columns_list = ['count']

    df = df.fillna(0)
    #extract all columns except year and author
    columns_list = df.columns.difference(['Year', 'Author']).values

    df[columns_list] = df[columns_list].astype(int)
    df['Year'] = df['Year'].astype(str)
    if journals:
        df.rename(columns=journals_dict, inplace=True)
    df['Year'] = df[df.columns.tolist()].groupby(['Author'])['Year'].transform(lambda x: ','.join(x))


    # df['count'] = df[df.columns.tolist()].groupby(['Author'])['count'].transform('sum')
    # ODO here: groupby and agg to condense one hotting into one author entry each
    df['count'] = df.Author.map(df.Author.value_counts())  # kind of slow; try other suggestions?
    #df = df.sort_values(['count', 'Author'], ascending=False)

    return df



def join_frames(confs_frame, journal_frame, confs=CONFERENCES, journs = JOURNAL_NAMES_1):
    '''
    Joins main conference and journal frames, as they had to be scraped differently.

    :param search_string: What to search for in the search bar of DBPL
    :return: A pandas dataframe populated with all authors from a single year at a conference.

    notes:: Does not account for conferences with > 2000 entries (so far, don't exist). Eventually generalize to > 2000 with a modulo/division while loop
    '''

    joined = pd.concat([confs_frame, journal_frame], ignore_index=True)
    joined = joined.fillna(0)
    joined['Year'] = joined[joined.columns.tolist()].groupby(['Author'])['Year'].transform(lambda x: ','.join(x))
    columns_list = journs #MAKE THIS WHOLE THING INTO HELPER FUNCTION
    for venue in confs.keys():
        columns_list.append(venue)
    print(columns_list)
    max_dict = {} #make this also into helper function
    for venue in columns_list:
        max_dict[venue] = 'max'
    max_dict['count'] = 'sum'
    venue_list = list(max_dict.keys())
    joined = joined.groupby(['Author', 'Year']).agg(max_dict).reset_index()


    joined[venue_list] = joined[venue_list].astype(int)

    return joined


def make_full_journal_url(journal_name, version):
    '''
    Formats URLS to access journals on DBLP.
    '''
    full_url = JOURN_API_URL + journal_name + '/' + journal_name + str(version) + APPENDAGE_STR
    return full_url


def setup_main_df(confs=CONFERENCES):
    '''
    Create an empty dataframe with column names corresponding to conferences, author, year, total count.

    :param confs: A dictionary of desired {conferences: start year} pairs
    :return: An empty pandas dataframe
    '''
    ''''''
    columns = ['Author', 'Year', 'count']
    for conf in confs:
        columns.append(conf)
    empty_df = pd.DataFrame(columns=columns)

    return empty_df



def add_counter_column(df):
    df.groupby(df.columns.tolist()).size().reset_index(
        name='count')
    return df



def df_as_file_saver(df, file_name='/Users/samski/Downloads/hi4.csv'):
    '''export as pandas df or csv?'''
    df.to_csv(file_name, encoding='utf-8', index=False)
    return



def consolidate_names(df, venues=JOURNALS):
    '''should this be stuck into postprocess?'''
    columns_list = []
    for venue in venues.keys():
        columns_list.append(venue)
    df = df.groupby(['Author', 'count', 'Year'])[columns_list].sum().reset_index()
    return df

def add_single_new_conf(conf_dict):
    '''capability to add conf data to dataframe'''
    return


def google_schol_getter():
    '''Extract what is returned by scholarly'''

    return

def schol_looper(df):
    '''For each author, search Google Scholar/scholarly entry, enter into columns hindex, interests, affilation/email?'''
    for i in df.index:
        auth = df.get_value(i,'Author')
        print(auth)
        print(next(scholarly.search_author(auth)))


    return

def main():
    '''Proper pipeline:
    Confs:
        conf_df = year_conf_looper()
        conf_df = postprocess_df(conf_df)
        conf_df = consolidate_names(conf_df, journals=False)


    Journs:
        journs_df = get_volume_nums_by_year()
        journs_df = postprocess_df(journs_df, journals=True)
        journs_df = consolidate_names(journs_df, venues=JOURNALS)

    Consolidation:
        joined_df = join_frames(conf_df, journs_df)
        df_as_file_saver(joined_df, full_venues_file)

            '''

    df_file = '/Users/samski/prof-names-counter/full_database_maker/data/full_nips_fixed.csv'

    df = pd.read_csv(df_file)

    dft = df.head()
    schol_looper(dft)


    print(author)
    # print(dft)
    # dft.Author.apply(schol_looper)


    #
    # full_confs_file_1 = '/Users/samski/Downloads/full_confs_1.csv'
    # full_confs_file = '/Users/samski/Downloads/full_confs.csv'
    # full_journs_file = '/Users/samski/Downloads/full_journs.csv'
    # #full_venues_file = '/Users/samski/Downloads/all_venues.csv'
    # test_file = '/Users/samski/Downloads/test_confs.csv'
    # test_file_c = '/Users/samski/Downloads/test_c.csv'
    # test_file_j = '/Users/samski/Downloads/test_j.csv'
    # both_file = '/Users/samski/Downloads/both.csv'
    # full_file = '/Users/samski/Downloads/full.csv'
    # cond_file = '/Users/samski/Downloads/cond.csv'
    # cond_file_2 = '/Users/samski/Downloads/cond_2.csv'
    # #
    # # test_j = read_in_df_from_file(test_file_j)
    # # test_c = read_in_df_from_file(test_file_c)
    # #
    # # both = join_frames(test_c, test_j)
    # # #
    # # df_as_file_saver(both, both_file)
    #
    # #TODO REMAKE CONFS WITH KKD FROM BEGINNING OF TIME
    #
    # conf_df = read_in_df_from_file(full_confs_file_1)
    # journs_df = read_in_df_from_file(full_journs_file)
    #
    # # #test_df = test_df.groupby(['Author'])[['NIPS']].agg('sum').reset_index()
    # # conf_df = year_conf_looper()
    # # conf_df = postprocess_df(conf_df)
    # # conf_df = consolidate_names(conf_df)
    # # df_as_file_saver(conf_df, full_confs_file_1)
    # #test_df = test_df.groupby(test_df.columns, axis=1).sum()
    # #test_df = test_df.groupby(['Author']).agg({'NIPS': 'sum'})
    # #conf_df['count'] = conf_df.Author.map(conf_df.Author.value_counts())  # kind of slow; try other suggestions?
    #
    #
    # full_df = read_in_df_from_file(cond_file_2)
    # #full_df = read_in_df_from_file(full_venues_file_1)
    # # ICML, NeurIPS, ICLR
    # #full_df['cond'] = full_df['ICML'] + full_df['NeurIPS'] + full_df['ICLR'] + full_df['NIPS']
    # #ICRA, IROS, KDD, IJCAI
    # #full_df['cond_2'] = full_df['ICRA'] + full_df['IROS'] + full_df['IJCAI']
    # #cond_df = full_df.drop(full_df[full_df.cond < 5].index)
    # # cols = cond_df.columns.tolist()
    # # cols = cols[-1:] + cols[:-1]
    # # cond_df = cond_df[cols]
    # cond_df_2 = full_df.drop(full_df[(full_df.cond_2 < 5)].index)
    #
    # # cols = cond_df_2.columns.tolist()
    # # print(cols)
    #
    # cond_df_2 = cond_df_2[['Author','count', 'cond', 'cond_2', 'TPAMI', 'TKDE', 'JMLR', 'NeurIPS', 'NIPS', 'ICLR', 'CVPR', 'ECCV', 'ICCV', 'ICRA', 'IROS', 'IJCAI', 'KDD', 'ICML', 'Year']]
    # #df_as_file_saver(cond_df, cond_file)
    # df_as_file_saver(cond_df_2, cond_file_2)




    # journs_df = consolidate_names(journs_df, JOURNALS)



    # journs_df = get_volume_nums_by_year()
    # journs_df = postprocess_df(journs_df, journals=True)
    # journs_df = consolidate_names(journs_df, JOURNALS)
    # df_as_file_saver(journs_df, full_journs_file)
    #
    # joined_df = join_frames(conf_df, journs_df)
    # df_as_file_saver(joined_df, full_venues_file_1)

    #concatenate
    #combine years string again
    #make years string into confyear, confyear
    #do count




    # full_df = read_in_df_from_file(file_name_j)
    # full_df = full_df.sort_values(['count', 'Author'], ascending=False)
    # df_as_file_saver(full_df, file_name_j)
    #
    # full_df1 = read_in_df_from_file(file_name_c)
    # full_df1 = full_df1.sort_values(['count', 'Author'], ascending=False)
    # df_as_file_saver(full_df1, file_name_c)



if __name__ == "__main__":
    main()
