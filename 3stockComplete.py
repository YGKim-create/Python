#!/usr/bin/env python
# coding: utf-8

# In[69]:


import pandas as pd
import pandas_datareader.data as web
from datetime import datetime
import numpy as np
import pandas as pd
from datetime import datetime
import pymysql
import matplotlib.gridspec as gridspec
import matplotlib
import pylab
from mpl_finance import candlestick_ohlc
from mpl_finance import candlestick2_ohlc
import matplotlib.pyplot as plt
#ipython magic code disabled in this code
import matplotlib.ticker as ticker
import numpy as np
import mpl_finance as mpf
import pandas_datareader.data as web
from mpl_finance import candlestick2_ohlc
import matplotlib.pyplot as plt
#ipython magic code disabled in this code
import matplotlib.ticker as ticker
import numpy as np


# In[177]:


df = pd.read_csv('hite.csv')


# In[178]:


df


# In[179]:


date = df.date.astype('str')


# In[180]:


MA5 = df['close'].rolling(window=5).mean()
MA20 = df['close'].rolling(window=20).mean()
MA50 = df['close'].rolling(window=50).mean()


# In[181]:


MA5


# In[182]:


def get_macd(df, short=12, long=26, t=9): # 입력받은 값이 dataframe이라는 것을 정의해줌 
    df = pd.DataFrame(df) 
    
    # MACD 관련 수식 
    ma_12 = df.close.ewm(span=12).mean() # 단기(12) EMA(지수이동평균) 
    ma_26 = df.close.ewm(span=26).mean() # 장기(26) EMA 
    macd = ma_12 - ma_26     # MACD 
    macds = macd.ewm(span=9).mean() # Signal 
    macdo = macd - macds # Oscillator 
    df = df.assign(MACD=macd, Signal=macds, Oscillator=macdo).dropna() 
    
    return df


# In[183]:


df = get_macd(df)


# In[184]:


df.head()


# In[185]:


ax1 = plt.subplot2grid((3, 3), (0, 0))
ax2 = plt.subplot2grid((3, 3), (0, 1), colspan=2)
ax3 = plt.subplot2grid((3, 3), (1, 0), colspan=2, rowspan=2)
ax4 = plt.subplot2grid((3, 3), (1, 2), rowspan=2)

plt.tight_layout()


# In[186]:


# 차트 레이아웃을 설정합니다.
fig = plt.figure(figsize=(12,10))
ax_main = plt.subplot2grid((5, 1), (0, 0), rowspan=3)
ax_sub = plt.subplot2grid((5, 1), (3, 0))
ax_sub2 = plt.subplot2grid((5, 1), (4, 0))


# In[192]:


#메인차트

fig = plt.figure(figsize=(12,10))
ax_main = plt.subplot2grid((5, 1), (0, 0), rowspan=3)
ax_sub = plt.subplot2grid((5, 1), (3, 0))
ax_sub2 = plt.subplot2grid((5, 1), (4, 0))

#ax = fig.add_subplot(1, 1, 1)
ax_main.set_xticks(range(0, len(df.date), 17))
ax_main.set_xticklabels(df.date[::17])

ax_sub.set_xticks(range(0, len(df.date), 20))
ax_sub.set_xticklabels(df.date[::20])

ax_sub2.set_xticks(range(0, len(df.date), 17))
ax_sub2.set_xticklabels(df.date[::17])


ax_main.set_title('HiteJinro 2019 Stock price',fontsize=20)
ax_main.plot(date, MA5, label='MA5')
ax_main.plot(date, MA20, label='MA20')
ax_main.plot(date, MA50, label = 'MA50')
mpf.candlestick2_ochl(ax_main, df['open'], df['close'], df['high'],
      df['low'], width=0.6, colorup='r', colordown='b', alpha=0.5)

ax_main.legend(loc=5);


def mydate(x,pos):
    try:
        return date[int(x-0.5)]
    except IndexError:
        return ''
    
    
ax_sub.set_title('MACD',fontsize=15)
df['MACD'].iloc[0] = 0
ax_sub.plot(date,df['MACD'], label='MACD', color = '#009900')
ax_sub.plot(date,df['Signal'], label='MACD Signal', color = '#cc9900')
ax_sub.legend(loc=2)

ax_sub2.set_title('MACD Oscillator',fontsize=15)
oscillator = df['Oscillator']
oscillator.iloc[0] = 1e-16
ax_sub2.bar(list(date),list(oscillator.where(oscillator > 0)), 0.7, color='#cc3333')
ax_sub2.bar(list(date),list(oscillator.where(oscillator < 0)), 0.7)

plt.tight_layout()
#plt.show()
plt.savefig('hite2.png')


# In[ ]:




