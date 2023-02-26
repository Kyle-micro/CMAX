
import pandas as pd
import numpy as np
import os
pd.set_option('display.max_columns',None)

fn = [ i for i in os.listdir(r'C:\Users\user\Downloads\產學案\價量資料')]

All = pd.DataFrame()
for f in fn:
    a = pd.read_csv('價量資料\\'+f) 
    
    # 前N天最高價
    CN=30
    # 前N天平均扣去前N天標準差
    BN=50
    # 扣去前N天標準差的倍數
    SN=2 #1,2-5
    # CMAX平均值分佈天數
    DN=40
    # Crash結束的臨界值(CMAX平均值分佈的10分位數)
    EN=25
    # 崩盤起始日=前幾天最高價
    MN=10
    
    # 將時間設定為索引
    a['snapped_at']=pd.to_datetime(a['snapped_at'])
    a = a[a['snapped_at'] <= pd.to_datetime("2022/07/31",utc=True)]
    
    a = a.iloc[:,0:2]
    
    # 計算CMAX
    a['CMAX']= a['price'] / a.rolling(CN)['price'].max()
    a['CMAX_AVG']= a['CMAX'].rolling(BN).mean()
    a['CMAX_STD']= SN*(a['CMAX'].rolling(BN).std())
    a['BenchMark']= a['CMAX_AVG']-a['CMAX_STD']
    
    # 崩盤結束門檻值 不rolling
    pr_limitlist=[]
    length = len(a['CMAX_AVG'][a['CMAX_AVG'].notna()])
    for pr in [a['CMAX_AVG'][a['CMAX_AVG'].notna()][i:i+DN] if DN+i < length else a['CMAX_AVG'][a['CMAX_AVG'].notna()][i:] for i in range(length)[::DN] ]:
        pr_limit=np.percentile(pr,EN)
        pr_limitlist.append(pr_limit)
   
    # 崩盤結束定義
    btable=pd.DataFrame() 
    b = [a[['price','CMAX','CMAX_AVG']][a['CMAX_AVG'].notna()][i:i+DN] if DN+i < length else a[['price','CMAX','CMAX_AVG']][a['CMAX_AVG'].notna()][i:] for i in range(length)[::DN] ]
    for n in range(len(pr_limitlist)):
        b[n]['Crash_over'] = np.where( b[n]['CMAX'] > pr_limitlist[n],b[n]['price'],np.nan)
        btable = pd.concat([btable,b[n]]) 
        
    # 得到Crash_over後併回原本表格
    Nona = a.dropna()
    na = a.iloc[:a.isna().sum()[-1],:]
    if Nona.shape[0] != btable.shape[0]:
        print('False')
    Nona.insert(Nona.shape[1],'Crash_over',btable['Crash_over'])
    a = pd.concat([na,Nona])
    
    # 崩盤存在定義
    a['Crash']=np.where(a['CMAX']<a['BenchMark'],a['price'],np.nan)
    
    # 跑一個迴圈，分別抓出第一個崩盤開始訊號和崩盤結束訊號。
    x_range=[]
    index = 0
    st=0
    en=0
    for i in range(a.shape[0]):
        a.loc[a.index[i],'g']=0
        # 偵測到崩盤
        if index == 0 and not np.isnan(a.loc[a.index[i],'Crash']):
            print(1)
            index = 1
            a.loc[a.index[i],'g']=1
        # 崩盤結束日 
        elif index == 1 and not np.isnan(a.loc[a.index[i],'Crash_over']):
            a.loc[a.index[i],'g']=1
            index =0
        else:
            continue
            
    # 崩盤起始日
    Genesis =  pd.DataFrame()
    for c in a[a['g']==1].index[::2]:
        aa = a.loc[c-MN:c,:]
        ge = aa[aa['price']==aa['price'].max()]
        if ge.shape[0] >1:
            ge = ge[ge.index==ge.index[-1]] 
        Genesis = pd.concat([Genesis,ge])
        
    G = Genesis.iloc[:,0:2]
    G.index = np.arange(0,G.shape[0])
    A = a.iloc[a[a['g']==1].index[1::2],0:2]
    A.index = np.arange(0,A.shape[0])
    Event = pd.concat([G,A],axis=1)   
    Event.columns = ['start time', 'p0', 'end time', 'p1']
    
    #合併起始日在前一個崩盤的事件為同一個崩盤
    merge = Event.shape[0]-1
    mer=0
    for i in range(merge):
        if Event.loc[i+1,'start time']<=Event.loc[i,'end time'] and Event.loc[i+1,'start time']>=Event.loc[i,'start time']:
            l = pd.DataFrame([Event.loc[i,'start time'],Event.loc[i,'p0'],Event.loc[i+1,'end time'],Event.loc[i+1,'p1']]).T
            l.columns = ['start time', 'p0', 'end time', 'p1']
            l.index = [i+1]
            Event = Event.drop([i,i+1],axis=0)
            Event = pd.concat([Event.loc[:i,:],l,Event.loc[i+2:,]])
            mer+=1
        else:
            continue
    Event.index = np.arange(Event.shape[0])
    Event[['p0','p1']] = Event[['p0','p1']].astype('float')
    
        
    # 價格最低之日
    Pmint = pd.DataFrame()    
    for i in range(Event.shape[0]):    
        tem = a[ (Event['end time'][i] >= a['snapped_at']) & (a['snapped_at'] >= Event['start time'][i]) ]
        Pmin = tem[tem['price'] == tem['price'].min()]
        if Pmin.shape[0] >1:
            Pmin = Pmin[Pmin.index==Pmin.index[0]]
        Pmint = pd.concat([Pmint,Pmin]) 
        
    P = Pmint.iloc[:,0:2]
    P.index = np.arange(0,P.shape[0])
    Event = pd.concat([Event,P],axis=1)  
    Event.columns = ['start time', 'p0', 'end time', 'p1', 'min time', 'minp']
    Event['持續期間'] = (Event['end time']-Event['start time']).astype('timedelta64[D]')
    
    aa = (Event['min time']- Event['start time']).astype('timedelta64[D]')
    Event['開始-最低價時長'] = np.where(aa>=1,aa,1) 
    Event['最大跌幅(%)'] = (Event['p0']-Event['minp'])/Event['p0']*100
    Event['下跌速度(%)'] = Event['最大跌幅(%)']/np.where(aa>=1,aa,1)
            
    #敘述統計
    Event.describe().to_csv( "敘述統計表格//" + f[:-12].upper() + "_大.csv", encoding='cp950')
    Event.to_csv('崩盤事件表//'+ f[:-12] + "_大.csv", encoding='cp950')
    Event['幣種'] = f[:-12]
    All = pd.concat([All,Event])
All