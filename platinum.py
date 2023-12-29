import pymysql

print(1)
print(2)
print(3)

import pandas as pd
import requests
from tqdm import tqdm
import pandas as pd
import random
import time
tqdm.pandas()
api_key='RGAPI-2ada0acf-3e59-436b-81a4-9b9c18582719'
SEOUL_api_key = '564851697a776a7337314e7a415251'


def get_df(url):
    url_re = url.replace('(인증키)', SEOUL_api_key).replace('xml', 'json').replace('/5/', '/1000')
    res = requests.get(url_re).json()
    key=list(res.keys())[0]
    df = pd.DataFrame(res[key]['row'])
    return df


def connect_mysql(db='lol_data'):
    conn = pymysql.connect(host='svc.sel4.cloudtype.app', port=32233, user='takealook', password='tmddk0908', db=db)
    return conn


def sql_execute(conn,query):
    cursor = conn.cursor()
    cursor.execute(query)
    result=cursor.fetchall()
    return result

def sql_execute_dict(conn,query):
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    cursor.execute(query)
    result=cursor.fetchall()
    return result

def get_match_id(puuid,num):
    url = f'https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?type=ranked&tart=0&count={num}&api_key={api_key}'
    match_list = requests.get(url).json()
    return match_list

def get_matches_timelines(matchid):
    url1 = f'https://asia.api.riotgames.com/lol/match/v5/matches/{matchid}?api_key={api_key}'
    url2 = f'https://asia.api.riotgames.com/lol/match/v5/matches/{matchid}/timeline?api_key={api_key}'
    matches = requests.get(url1).json()
    timelines = requests.get(url2).json()
    return matches,timelines

def get_puuid(nickname,tag):
    url = f'https://asia.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{nickname}/{tag}?api_key={api_key}'
    print(nickname,tag)
    res = requests.get(url).json()
    print(res)
    puuid = res['puuid']
    return puuid

def get_match_timeline_df(df):
    # df를 한개로 만들기
    df_creater = []
    print('소환사 스텟 생성중.....')
    for i in tqdm(range(len(df))):
        # matches 관련된 데이터
        try:
            if df.iloc[i].matches['info']['gameDuration'] > 900:   # 게임시간이 15분이 안넘을경우에는 패스하기
                for j in range(10):
                    tmp = []
                    tmp.append(df.iloc[i].match_id)
                    tmp.append(df.iloc[i].matches['info']['gameDuration'])
                    tmp.append(df.iloc[i].matches['info']['gameVersion'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['summonerName'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['summonerLevel'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['participantId'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['championName'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['champExperience'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['teamPosition'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['teamId'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['win'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['kills'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['deaths'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['assists'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['totalDamageDealtToChampions'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['totalDamageTaken'])
            #timeline 관련된 데이터
                    for k in range(5,26):
                        try:
                            tmp.append(df.iloc[i].timelines['info']['frames'][k]['participantFrames'][str(j+1)]['totalGold'])
                        except:
                            tmp.append(0)
                    df_creater.append(tmp)
        except:
            print(i)
            continue
    columns = ['gameId','gameDuration','gameVersion','summonerName','summonerLevel','participantId','championName','champExperience',
    'teamPosition','teamId','win','kills','deaths','assists','totalDamageDealtToChampions','totalDamageTaken','g_5','g_6','g_7','g_8','g_9','g_10','g_11','g_12','g_13','g_14','g_15','g_16','g_17',
    'g_18','g_19','g_20','g_21','g_22','g_23','g_24','g_25']
    df = pd.DataFrame(df_creater,columns = columns).drop_duplicates()
    print('df 제작이 완료되었습니다. 현재 df의 수는 %d 입니다'%len(df))
    return df
def get_rawdata(tier):
    division_list = ['I', 'II', 'III', 'IV']
    lst = []
    IdLst = []
    sid = []
    pid = []
    mid = []
    dl = []
    i = 0
    page = random.randrange(1, 20)
    print('get summonerId...')
    for division in tqdm(division_list):
        url1 = f'https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{tier}/{division}?page={page}&api_key={api_key}'
        res = requests.get(url1).json()
        #   if isinstance(res, list):
        lst += random.sample(res, 2)
    for i in range(len(lst)):
        sid.append(lst[i]['summonerId'])
    print('get puuid')
    for i in tqdm(range(len(lst))):
        url2 = f'https://kr.api.riotgames.com/lol/summoner/v4/summoners/{sid[i]}?api_key={api_key}'
        pid.append(requests.get(url2).json()['puuid'])
    print('get matchid')
    for i in tqdm(range(len(pid))):
        url3 = f'https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{pid[i]}/ids?start=0&count=20&api_key={api_key}'
        res3 = requests.get(url3).json()
        # if isinstance(res3, list):
        mid.append(random.sample(res3, 4))
    print(mid)
    print('get data')
    for i in tqdm(range(len(mid))):
        for j in range(len(mid[i])):
            url4 = f'https://asia.api.riotgames.com/lol/match/v5/matches/{mid[i][j]}?api_key={api_key}'
            url5 = f'https://asia.api.riotgames.com/lol/match/v5/matches/{mid[i][j]}/timeline?api_key={api_key}'
            matches = requests.get(url4).json()
            timelines = requests.get(url5).json()
            dl.append([mid[i][j], matches, timelines])

    df = pd.DataFrame(dl, columns=['match_id', 'matches', 'timelines'])
    print('작업완료')
    print(df)
    return df

def insert_matches_timelines_mysql(row, conn):

    # lambda를 이용해서 progress_apply를 통해 insert할 구문 만들기
    query = (
        f'insert into lol_platinum(gameId, gameDuration, gameVersion, summonerName, summonerLevel, participantId,'
        f'championName, champExperience, teamPosition, teamId, win, kills, deaths, assists,'
        f'totalDamageDealtToChampions, totalDamageTaken, g_5, g_6, g_7, g_8, g_9, g_10, g_11, g_12 ,g_13,g_14,'
        f'g_15, g_16, g_17, g_18, g_19, g_20, g_21, g_22, g_23, g_24, g_25)'
        f'values(\'{row.gameId}\',{row.gameDuration}, \'{row.gameVersion}\', \'{row.summonerName}\','
        f'{row.summonerLevel}, {row.participantId},\'{row.championName}\',{row.champExperience},'
        f'\'{row.teamPosition}\', {row.teamId}, \'{row.win}\', {row.kills}, {row.deaths}, {row.assists},'
        f'{row.totalDamageDealtToChampions},{row.totalDamageTaken},{row.g_5},{row.g_6},{row.g_7},{row.g_8},'
        f'{row.g_9},{row.g_10},{row.g_11},{row.g_12},{row.g_13},{row.g_14},{row.g_15},{row.g_16},{row.g_17},'
        f'{row.g_18},{row.g_19},{row.g_20},{row.g_21},{row.g_22},{row.g_23},{row.g_24},{row.g_25})'
        f'ON DUPLICATE KEY UPDATE '
        f'gameId = \'{row.gameId}\', gameDuration = {row.gameDuration}, gameVersion = \'{row.gameVersion}\', summonerName= \'{row.summonerName}\','
        f'summonerLevel = {row.summonerLevel},participantId = {row.participantId},championName = \'{row.championName}\','
        f'champExperience = {row.champExperience}, teamPosition = \'{row.teamPosition}\', teamId = {row.teamId},win = \'{row.win}\','
        f'kills = {row.kills}, deaths = {row.deaths}, assists = {row.assists}, totalDamageDealtToChampions = {row.totalDamageDealtToChampions},'
        f'totalDamageTaken = {row.totalDamageTaken},g_5 = {row.g_5},g_6 = {row.g_6},g_7 = {row.g_7},g_8 = {row.g_8},g_9 = {row.g_9},'
        f'g_10 = {row.g_10},g_11 = {row.g_11},g_12 = {row.g_12},g_13 = {row.g_13},g_14 = {row.g_14},g_15 = {row.g_15},g_16 = {row.g_16},g_17 = {row.g_17},'
        f'g_18 = {row.g_18},g_19 = {row.g_19},g_20 = {row.g_20},g_21 = {row.g_21},g_22 = {row.g_22},g_23 = {row.g_23},g_24 = {row.g_24},g_25 = {row.g_25}'
    )
    sql_execute(conn, query)
    return query
tier='BRONZE'
for i in tqdm(range(1000)):
    try:
        raw_data=get_rawdata(tier)
        df=get_match_timeline_df(raw_data)
        conn=connect_mysql()
        df.progress_apply(lambda x:insert_matches_timelines_mysql(x,conn),axis=1)
        conn.commit()
        conn.close()
        print(f'{i}번째 완료')
        time.sleep(90)
    except Exception as e:
        print(f'{e}의 원인으로 insert 실패')
        time.sleep(90)

print('insert complet')