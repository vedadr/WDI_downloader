import wbdata
import datetime
import pandas as pd
import logging
import time
import pyodbc
import pickle
import yaml

start_time = time.time()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# create a file handler
handler = logging.FileHandler('log.log')
handler.setLevel(logging.ERROR)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)

with open('config.yml','r') as conf:
    settings = yaml.load(conf)

if settings['debug']:
    limit_test = settings['limit_test']
else:
    limit_test = 99999

sleep_time = settings['sleep_time']
sleep_interval = settings['sleep_interval']
cache_indicators = settings['cache_indicators']

def main():
    sources = wbdata.get_source()
    res_data = []
    indicator_list = []
    fips_codes = get_FIPS_codes()
    if not cache_indicators:
        get_indicators(indicator_list, sources)
        output = open('data.pkl', 'wb')
        pickle.dump(indicator_list, output)
    else:
        pkl_file = open('data.pkl', 'rb')
        indicator_list = pickle.load(pkl_file)
    preselected_indicators = get_preselected_indicators();
    indicator_list = [ind for ind in indicator_list if ind['id'] in preselected_indicators]
    df = get_data(indicator_list, res_data)

    df = df.merge(fips_codes, how='inner', left_on='Place', right_on='Name')
    df.value = df.Value.str.replace('None', '')
    df.Value.astype(float)
    df = df.pivot_table(columns = 'Indicator', values='Value', index=['FIPS', 'Place','Name','qName'] , aggfunc=max)
    df.to_csv("data_from_py.csv")

def get_preselected_indicators():
    preselected_indicators = []
    try:
        preselected_indicators = pd.read_csv(settings['indicators_loc'])
    except ValueError:
        logger.info("There's been an error while trying to open preselected indicators: {}".format(ValueError))
        return preselected_indicators
    return list(preselected_indicators['indicatorid'])

def get_FIPS_codes():
    # Create connection
    con = pyodbc.connect(driver="{SQL Server}", server=settings['s'], database=settings['db'], uid=settings['u'], pwd=settings['p'])
    cur = con.cursor()
    db_cmd = settings['query']
    cur.execute(db_cmd)
    res = [list(i) for i in cur.fetchall()]
    res_df = pd.DataFrame(data = res, columns=['Name', 'qName', 'FIPS'])
    return res_df

def get_data(indicator_list, res_data):
    max_ind = 0
    for i, indicator in enumerate(indicator_list):
        try:
            logger.info('Downloading ' + indicator['id'])
            missing_data = wbdata.get_data(indicator['id'], data_date=datetime.datetime(2015, 1, 1))
        except:
            logger.error('Something is wrong, skipping ' + indicator['id'])
            continue
        max_ind += 1
        if max_ind == limit_test:
            break
        for el in missing_data:
            res_data.append([indicator['id'], el['country']['value'], el['value']])
        if i % sleep_interval == 0:
            logger.warn('Sleeping for {} seconds...'.format(sleep_time))
            time.sleep(sleep_time)
    df = pd.DataFrame(data=res_data, columns=['Indicator', 'Place', 'Value'])
    return df


def get_indicators(indicator_list, sources):
    logger.info('Downloading indicators ...')
    for i, el in enumerate(sources):
        if i == limit_test:
            break
        indicator_list.extend(wbdata.get_indicator(source=int(el['id'])))
    logger.info('Done!')

if __name__ == '__main__':
    main()
    logger.info('Program ended in {} seconds.'.format(time.time()-start_time))