#!/usr/bin/env python
# coding: utf-8

# In[1]:


from ApploComposer import *


# In[2]:


import plotly.graph_objects as go
import plotly
from plotly.graph_objs import Candlestick, Layout
from plotly import __version__
from plotly.offline import download_plotlyjs,init_notebook_mode,plot,iplot
import pandas as pd
from datetime import datetime
from plotly import tools 
import plotly.offline as offline
import numpy as np
import chart_studio.plotly as py


# In[3]:


df = pd.read_csv('samsung.csv')


# In[4]:


df


# In[5]:


list(df.columns)


# In[6]:


plotly.offline.init_notebook_mode()


# In[7]:


INCREASING_COLOR = '#FF0000'
DECREASING_COLOR = '#0000FF'


# In[8]:


INCREASING_COLOR2 = '#993333'
DECREASING_COLOR2 = '#9999CC'


# In[22]:


data = [ dict(
    type = 'candlestick',
    open = df.open,
    high = df.high,
    low = df.low,
    close = df.close,
    x = df.date1,
    yaxis = 'y2',
    name = 'SAMSUNG',
    increasing = dict( line = dict( color = INCREASING_COLOR ) ),
    decreasing = dict( line = dict( color = DECREASING_COLOR ) ),
) ]


# In[23]:


layout = dict()


# In[24]:


fig = dict(data=data,layout=layout)


# In[35]:


fig['layout'] = dict()
fig['layout']['plot_bgcolor'] = 'rgb(250, 250, 250)'
fig['layout']['xaxis'] = dict( rangeselector = dict( visible = True))
fig['layout']['yaxis'] = dict( domain = [0, 0.2], showticklabels = False )
fig['layout']['yaxis2'] = dict( domain = [0.2, 0.8] )
fig['layout']['legend'] = dict( orientation = 'h', y=0.9, x=0.3, yanchor='bottom' )
fig['layout']['margin'] = dict( t=40, b=40, r=40, l=40 )


# In[36]:


#기간 선택 버튼

rangeselector=dict(
    visibe = True,
    x = 0, y = 0.9,
    bgcolor = 'rgba(150, 200, 250, 0.4)',
    font = dict( size = 13 ),
    buttons=list([
        dict(count=1,
             label='reset',
             step='all'),
        dict(count=1,
             label='1yr',
             step='year',
             stepmode='backward'),
        dict(count=3,
            label='3 mo',
            step='month',
            stepmode='backward'),
        dict(count=1,
            label='1 mo',
            step='month',
            stepmode='backward'),
        dict(step='all')
    ]))
    
fig['layout']['xaxis']['rangeselector'] = rangeselector


# In[37]:


def movingaverage(interval, window_size=10):
    window = np.ones(int(window_size))/float(window_size)
    return np.convolve(interval, window, 'same')


# In[38]:


mv_y = movingaverage(df.close)
mv_x = list(df.date1)

# Clip the ends
mv_x = mv_x[5:-5]
mv_y = mv_y[5:-5]

fig['data'].append( dict( x=mv_x, y=mv_y, type='scatter', mode='lines', 
                         line = dict( width = 1 ),
                         marker = dict( color = '#FF9933' ),
                         yaxis = 'y2', name='이동평균선' ) )


# In[39]:


colors = []

for i in range(len(df.close)):
    if i != 0:
        if df.close[i] > df.close[i-1]:
            colors.append(INCREASING_COLOR)
        else:
            colors.append(DECREASING_COLOR)
    else:
        colors.append(DECREASING_COLOR)


# In[40]:


fig['data'].append( dict( x=df.date1, y=df.volume,                         
                         marker=dict( color='#9999cc' ),
                         type='bar', yaxis='y', name='거래량' ) )


# In[41]:


def bbands(price, window_size=10, num_of_std=5):
    rolling_mean = price.rolling(window=window_size).mean()
    rolling_std  = price.rolling(window=window_size).std()
    upper_band = rolling_mean + (rolling_std*num_of_std)
    lower_band = rolling_mean - (rolling_std*num_of_std)
    return rolling_mean, upper_band, lower_band

bb_avg, bb_upper, bb_lower = bbands(df.close)

fig['data'].append( dict( x=df.date1, y=bb_upper, type='scatter', yaxis='y2', 
                         line = dict( width = 1 ),
                         marker=dict(color='#ccc'), hoverinfo='none', 
                         legendgroup='Bollinger Bands', name='볼린저 밴드') )

fig['data'].append( dict( x=df.date1, y=bb_lower, type='scatter', yaxis='y2',
                         line = dict( width = 1 ),
                         marker=dict(color='#ccc'), hoverinfo='none',
                         legendgroup='볼린저 밴드', showlegend=False ) )


# In[42]:


offline.plot(fig,validate = False)


# In[43]:


plot_file_name=offline.plot( fig, validate = False )


# In[44]:


#파일 이름
print(plot_file_name)

#파일을 열어서
with open(plot_file_name) as f:
    #내용을 받아와서
    file_content = f.read()
    
    #내용이 HTML이므로 ipython HTML객체 변경한다
    html_converted = HTML(file_content)
    
    #화면에 보여준다
    display(html_converted)


# In[45]:


from IPython.display import IFrame


# In[47]:


IFrame(src="https://dash-simple-apps.plotly.host/dash-candlestickplot/", width="100%", height ="750px", frameBoarder="0")


# In[ ]:





# In[ ]:




