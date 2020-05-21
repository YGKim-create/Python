#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# -*- coding: UTF-8 -*-


# In[ ]:



#import System   
try:
    import dateparser
except:    
    get_ipython().system('pip install dateparser')
    import dateparser
    print("dateparser installed")

import traceback
import calendar   
import pytz
import datetime
import decimal
import pandas as pd
import re
import numpy as np
#from System import *
import uuid
from threading import RLock
_nldate_mutex = RLock()
global re_nldateformatter
global re_nldateformatter2
re_nldateformatter = re.compile('([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2})\:([0-9]{2})\:([0-9]{2})\.([0-9]{4,})([\+\-])([0-9]{2})\:([0-9]{2})')
re_nldateformatter2 = re.compile('([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2})\:([0-9]{2})\:([0-9]{2})\.([0-9]{4,})')
decimal.getcontext().traps[decimal.InvalidOperation] = False


# In[ ]:


def CustomSlack(message):
    import requests
    import json
    return CustomSendSlackFull("#applo_log", "ApploEditor", "[%s] (type:%s) is invalid type"%(s, type(s)))
    
def CustomSendSlackFull(channel, name, message, icon = ':robot:'):
    data = {"channel":channel,"username":name,"text":message,"icon_emoji":icon}
    res = requests.post("https://hooks.slack.com/services/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", {"payload":json.dumps(data)} )
    return res
    


# In[ ]:




class NLDUtil:
    @classmethod
    def is_number_tryexcept(cls, s):
        try:
            float(s)
            return True
        except ValueError:
            return False
        
    @classmethod
    def DatetimeStr(cls, dt):        
        
        if dt == None:
            return "None"

        if type(dt) != Date:
            if type(dt) == str:            
                dt = Date(dt)
            elif type(dt) == datetime.datetime:
                dt = Date("%s년 %s월 %s일 %s시 %s분 %s초"%((dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)))
            elif type(dt) == type(null_datetime()):
                return "None"
                
        return "%s년 %s월 %s일 %s요일 %s시 %s분 %s초"%(dt.year, dt.month, dt.day, dt.요일, dt.hour, dt.minute, dt.second)
    
    @classmethod
    def get_tz_diff(cls, tz):
        utc_now = datetime.datetime.now(datetime.timezone.utc)
        current_now = utc_now.astimezone(tz)
        td = datetime.datetime(current_now.year, current_now.month, current_now.day, current_now.hour, current_now.minute, current_now.second) - datetime.datetime(utc_now.year, utc_now.month, utc_now.day, utc_now.hour, utc_now.minute, utc_now.second)
        ts = td.seconds
        pm = ""
        if ts >= 0:
            pm = "+"
        else:
            pm = "-"
        left_sec = ts % 60
        mins = ts / 60
        hours = mins / 60
        left_min = mins%60
        return "%s%02d%02d"%(pm, hours, left_min)

from math import ceil


#datetime이 immutable이므로 __new__를 이용 cls를 쓴다.
class null_datetime(datetime.datetime):
    def __new__(cls, *args, **kwargs):
        #super(null_datetime,self).__init__(1970,1,1,0,0,0)
        nn = decimal.Decimal('nan')
        cls.nn = nn
        
        cls.year = nn
        cls.month = nn
        cls.day = nn
        cls.hour = nn
        cls.minute = nn
        cls.second = nn
        cls.microsecond = nn
        return cls
    
    def weekday():
        return decimal.Decimal('nan')
    
    def replace(**kwargs):
        return null_datetime()
    
    def __hash__(cls):
        return hash(pd.NaT)
__nd = null_datetime()

def week_of_month(dt):
    """ Returns the week of the month for the specified date.
    """
    
    global __nd
    
    if dt == __nd:
        return decimal.Decimal('nan')
    
    first_day = dt.replace(day=1)

    dom = dt.day
    adjusted_dom = dom + first_day.weekday()

    return int(ceil(adjusted_dom/7.0))


# In[ ]:


global nldate_compare_matched_last
nldate_compare_matched_last = True 
global nldate_compare_set
#global nldate_compare_cache
nldate_compare_set = []
#nldate_compare_cache = []
def init_once():
    year1 = '%Y'
    year2 = '%y'
    month = '%m'
    day = '%d'
    hour = '%H'
    minu = '%M'
    sec = '%S'

    sep = ['',' ','/','.','-',':']
    ptns = []
    #순서 중요하다
    ptns.append(year1)
    ptns.append(year2)

    for s in sep:
        ptns.append(year1+s+month)
        ptns.append(year2+s+month)                

    for s in sep:
        ptns.append(year1+s+month+s+day)
        ptns.append(year2+s+month+s+day)                

    for s in sep:
        for s1 in sep:                    
            ptns.append(year1+s+month+s+day+s1+hour)
            ptns.append(year2+s+month+s+day+s1+hour)

    for s in sep:
        for s1 in sep:
            for s2 in sep:
                ptns.append(year1+s+month+s+day+s1+hour+s2+minu)
                ptns.append(year2+s+month+s+day+s1+hour+s2+minu)
    for s in sep:
        for s1 in sep:
            for s2 in sep:
                for s3 in sep:
                    ptns.append(year1+s+month+s+day+s1+hour+s2+minu+s3+sec)
                    ptns.append(year2+s+month+s+day+s1+hour+s2+minu+s3+sec)
    return ptns

nldate_compare_set = init_once()


# In[ ]:


def sortlist(ls, **kwargs):
    ls2 = ls[:]
    ls2.sort(key=kwargs.get("key",lambda x : x), reverse=kwargs.get("reverse", False))
    return ls2

class Date:
    def __hash__(self):
        return self.datetime.__hash__()
    
    def __repr__(self):
        return NLDUtil.DatetimeStr(self.datetime)
    
    def Clone(self):
        return Date(self.datetime)
        
    def CloneFrom(self, from_nldate):
        if type(from_nldate) == Date:
            self.__init__(from_nldate.datetime)
            return self
        else:
            self.__init__(from_nldate)
            return self
    
    def __getitem__(self, key):
        try:
            return self.__dict__[key]
        except:
            return None
    
    def __init__(self, string, basetime = None, **kwargs):
        
        self.rawdata = string
        if type(string) == Date:
            self.CloneFrom(string)
            return
        if type(string) == type(None):
            string = ''
            
        global _dateParser
        if _dateParser == None:
            _dateParser = DateParser()
        self._dateParser = kwargs.get('dateparser', _dateParser)
        dt = self._dateParser.Parse(string, basetime, **kwargs)        
                
        self.isNull = (dt == None) or (dt == null_datetime())
        
        if self.isNull:
            print("#applo_log", "ApploEditor", "[ERROR] Can't parse date string [%s]"%string)
            dt = null_datetime()
            
        self.datetime = dt
        
        self.year = dt.year
        self.month = dt.month
        self.day = dt.day
        self.hour = dt.hour
        self.minute = dt.minute
        self.second = dt.second
        self.microsecond = dt.microsecond
        self.년 = dt.year
        self.연 = dt.year
        self.월 = dt.month
        self.일 = dt.day
        self.시 = dt.hour
        self.시24 = dt.hour        
        
        self.년월 = f"{dt.year:0>4}-{dt.month:0>2}"
        self.년월일 = f"{dt.year:0>4}-{dt.month:0>2}-{dt.day:0>2}"
        self.년월일시 = f"{dt.year:0>4}-{dt.month:0>2}-{dt.day:0>2} {dt.hour:0>2}"
        self.년월일시분 = f"{dt.year:0>4}-{dt.month:0>2}-{dt.day:0>2} {dt.hour:0>2}:{dt.minute:0>2}"
        self.년월일시분초 = f"{dt.year:0>4}-{dt.month:0>2}-{dt.day:0>2} {dt.hour:0>2}:{dt.minute:0>2}:{dt.second:0>2}"
        self.연월 = f"{dt.year:0>4}-{dt.month:0>2}"
        self.연월일 = f"{dt.year:0>4}-{dt.month:0>2}-{dt.day:0>2}"
        self.연월일시 = f"{dt.year:0>4}-{dt.month:0>2}-{dt.day:0>2} {dt.hour:0>2}"
        self.연월일시분 = f"{dt.year:0>4}-{dt.month:0>2}-{dt.day:0>2} {dt.hour:0>2}:{dt.minute:0>2}"
        self.연월일시분초 = f"{dt.year:0>4}-{dt.month:0>2}-{dt.day:0>2} {dt.hour:0>2}:{dt.minute:0>2}:{dt.second:0>2}"                
        self.일시분 = f"{dt.day:0>2}:{dt.hour:0>2}:{dt.minute:0>2}"
        self.시분 = f"{dt.hour:0>2}:{dt.minute:0>2}"
        self.시분초 = f"{dt.hour:0>2}:{dt.minute:0>2}:{dt.second:0>2}"
        self.분초 = f"{dt.minute:0>2}:{dt.second:0>2}"
        
            
        try:
            if self.시24 < 12:
                self.오전오후 = '오전'
                self.시12 = self.시24
            else:
                self.오전오후 = '오후'
                self.시12 = self.시24 - 12
                
            if self.시24 == 0:
                self.시12 = 12
        except:
            self.시24 = self.시
            self.시12 = self.시
            self.오전오후 = '오전'
        
        
        
        #self.오전오후 = '오전' if dt.hour < 12 else '오후'        
        #self.시12 = self.시24 if self.오전오후 == '오전' else 
        self.분 = dt.minute
        self.초 = dt.second
        self.weekday_no = dt.weekday()
        if self.isNull:
            self.weekday = decimal.Decimal('nan')
        else:
            self.weekday = calendar.day_name[self.weekday_no]
        
        #요일 한글화
        if self.isNull:
            self.weekday_kor = self.요일 = "NaN"
        else:
            if self.weekday_no == 0:
                self.weekday_kor = self.요일 = "월"
            elif self.weekday_no == 1:
                self.weekday_kor = self.요일 = "화"
            elif self.weekday_no == 2:
                self.weekday_kor = self.요일 = "수"
            elif self.weekday_no == 3:
                self.weekday_kor = self.요일 = "목"
            elif self.weekday_no == 4:
                self.weekday_kor = self.요일 = "금"
            elif self.weekday_no == 5:
                self.weekday_kor = self.요일 = "토"
            elif self.weekday_no == 6:
                self.weekday_kor = self.요일 = "일"
        self.week_month = week_of_month(dt)
        self.주 = self.week_month        
    
    def __str__(self):
        return str(self.datetime)
    
    def __add__(self, other):
        if type(other) == Date:
            return self.datetime + other.datetime
        return self.datetime + other
    
    def __sub__(self, other):
        if type(other) == Date:
            return self.datetime - other.datetime
        return self.datetime - other
    
    def __lt__(self, other):
        if type(other) == Date:
            return self.datetime < other.datetime
        return self.datetime < other
    
    def __le__(self, other):
        if type(other) == Date:
            return self.datetime <= other.datetime
        return self.datetime <= other
    
    def __eq__(self, other):
        if type(other) == Date:
            return self.datetime == other.datetime
        return self.datetime == other
    
    def __ne__(self, other):
        if type(other) == Date:
            return self.datetime != other.datetime
        return self.datetime != other
    
    def __gt__(self, other):
        if type(other) == Date:
            return self.datetime > other.datetime
        return self.datetime > other
    
    def __ge__(self, other):
        if type(other) == Date:
            return self.datetime >= other.datetime
        return self.datetime >= other

class DateParser:
    def now(self, **kwargs):
        tz = kwargs.get('tzinfo', self.tzinfo)
        return datetime.datetime.now(datetime.timezone.utc).astimezone(tz)
        
    def befaft(self):
        return self.bef + self.aft
    
    def makebt(self, bt, dic):
        if type(bt) != datetime.datetime:
            #can't operate
            return bt
    
    def __init__(self, tz=pytz.timezone('Etc/GMT-9')):
        
        self.tzinfo = tz
        self.bef = ['이전','전','앞']
        self.aft = ['이후','후','뒤']
        
        dic_amount_standard = {"hour":["시간","시"],"min":["분"],"sec":["초"],"year":["연","년"],"mon":["월","개월","달"],"day":["일"]}
        
        self.bef_aft_pattern = "("+"|".join(sortlist(self.bef+self.aft, key=len, reverse=True))+")"       
        self.amount_pattern = "(([+-]?[\d,]*(\.?\d*))(\s*)("+"|".join(sortlist(sum([v for k, v in dic_amount_standard.items()], []), key=len, reverse=True))+")(\s*))"
        #reverse self.dic_amount_standard
        ls_dic_reversed = sum([[(t, k) for t in v] for k, v in dic_amount_standard.items()],[])
        self.dic_amount_standard = {}
        for l in ls_dic_reversed: 
            self.dic_amount_standard[l[0]] = l[1]
            
    def GetDSTAddr(self):
        return NLDUtil.get_tz_diff(self.tzinfo)
    
    def replace_with_da(self, bt, dic, **kwargs):
        minsec0 = kwargs.get('minsec0', False)
        da = {}
        da['year'] = dic['year'] if 'year' in dic else bt.year
        da['mon'] = dic['mon'] if 'mon' in dic else bt.month
        da['day'] = dic['day'] if 'day' in dic else bt.day        
        
        satisfied = False        
        da['hour'] = dic['hour'] if 'hour' in dic else bt.hour
        satisfied = ('hour' in dic) or satisfied
        
        if minsec0 and satisfied and 'min' not in dic:
            da['min'] = 0
        else:
            da['min'] = dic['min'] if 'min' in dic else bt.minute
        satisfied = ('min' in dic) or satisfied
        
        if minsec0 and satisfied and 'sec' not in dic:
            da['sec'] = 0
        else:
            da['sec'] = dic['sec'] if 'sec' in dic else bt.second                    
        return da
    
    def add_with_da(self, bt, dic):
        da = {}
        da['year'] = bt.year + dic.get("year", 0)
        da['mon'] = bt.month + dic.get("mon", 0)
        da['day'] = bt.day + dic.get("day", 0)
        da['hour'] = bt.hour + dic.get("hour", 0)
        da['min'] = bt.minute + dic.get("min", 0)
        da['sec'] = bt.second + dic.get("sec", 0)
        return da
    
    def monthrange_from_da(self, da):
        #process month-year (for maxday, if negative month exists handle it)
        while da['mon'] <= 0 or da['mon'] > 12:
            av = abs(da['mon']/(12.0))
            year_amount = int(av) + (1 if int(av) != av else 0)
            if da['mon'] <= 0:
                da['mon'] += year_amount * 12
                da['year'] -= year_amount
            elif da['mon'] > 12:
                da['mon'] -= year_amount * 12
                da['year'] += year_amount    
                
        #process day-month        
        maxday = calendar.monthrange(da['year'], da['mon'])[1]
        return maxday
            
    def compat_da(self, da):
        #import ipdb;ipdb.set_trace()
        while da['sec'] < 0 or da['sec'] >= 60:
            av = abs(da['sec']/(60.0))
            min_amount = int(av) + (1 if int(av) != av else 0)
            if da['sec'] < 0:
                
                da['sec'] += min_amount * 60
                da['min'] -= min_amount
            elif da['sec'] >= 60:
                min_amount = int(abs(da['sec']/(60.0)) + 1)
                da['sec'] -= min_amount * 60
                da['min'] += min_amount
        #process min
        while da['min'] < 0 or da['min'] >= 60:
            av = abs(da['min']/(60.0))
            hour_amount = int(av) + (1 if int(av) != av else 0)
            if da['min'] < 0:
                da['min'] += hour_amount * 60
                da['hour'] -= hour_amount
            elif da['min'] >= 60:
                da['min'] -= hour_amount * 60
                da['hour'] += hour_amount
        #process hour-day
        while da['hour'] < 0 or da['hour'] >= 24:
            av = abs(da['hour']/(24.0))
            day_amount = int(av) + (1 if int(av) != av else 0)
            if da['hour'] < 0:
                da['hour'] += day_amount * 24
                da['day'] -= day_amount
            elif da['hour'] >= 24:
                da['hour'] -= day_amount * 24
                da['day'] += day_amount
       
        #process day-month      
        maxday = self.monthrange_from_da(da)
        #maxday = calendar.monthrange(da['year'], da['mon'])[1]
        while da['day'] <= 0 or da['day'] > maxday:
            if da['day'] <= 0:
                da['mon'] -= 1
                da['day'] += self.monthrange_from_da(da)
            elif da['day'] > maxday:
                da['mon'] += 1                        
                da['day'] -= self.monthrange_from_da(da)
            #re-calculate maxday per month
            maxday = self.monthrange_from_da(da)
        
        return da
            
    def parse_preprocessing(self, s, **kwargs):
        if type(s) == str:
            return s
        else:
            if type(s) == datetime.datetime:
                return s.strftime('%Y-%m-%d %H:%M:%S.%f')
            elif type(s) == pd.Timestamp:
                ds = datetime.datetime(s.year, s.month, s.day, s.hour, s.minute, s.second)
                return ds.strftime('%Y-%m-%d %H:%M:%S')
            else:
                CustomSlack("#applo_log", "ApploEditor", "[%s] (type:%s) is invalid type"%(s, type(s)))
                
                return None
        
    def Parse(self, s, basetime = None, **kwargs):
        if s == None or type(s) == type(pd.NaT):
            return null_datetime()
        
        #print("%s%s"%(s,"-"*100))
        uid = uuid.uuid1()
        log = kwargs.get("log",False)
        tz_using = kwargs.get("tzinfo", self.tzinfo)
        kwargs['tzinfo'] = tz_using
        #log = True
        if log:
            if type(s) != str:
                stacktrace = "\t".join(traceback.format_stack())
                CustomSlack(f"{uid} NLDATE input = {s}({type(s)}), stack=\n{stacktrace}")
        
        if basetime == None:
            basetime = self.now(tzinfo=tz_using)
        
        s_origin_include_type = s
        s_origin = s
        
        try:
            dtorg = type(s_origin)
            if dtorg == np.datetime64:
                if log:
                    CustomSlack(f"{uid} np.dt64 NLDATE input = {s}({type(s)}), s_org={s_origin}")
                s_origin = pd.Timestamp(s_origin)
                dtorg = type(s_origin)
                

            if dtorg == pd.Timestamp:
                if log:
                    CustomSlack(f"{uid} pd.ts NLDATE input = {s}({type(s)}), s_org={s_origin}")
                
                if s_origin == pd.NaT:
                    return null_datetime()
                
                s_origin = s_origin.to_pydatetime()
                dtorg = type(s_origin)
                
                

            if dtorg == datetime.datetime:
                if log:
                    CustomSlack(f"{uid} datetime NLDATE input = {s}({type(s)}), s_org={s_origin}")
                return s_origin

            if dtorg != str:
                if log:
                    CustomSlack(f"{uid} not str NLDATE input = {s}({type(s)}), s_org={s_origin}")
                s = str(s_origin)
        
            
            #CustomSlack(f"{uid} all after NLDATE input = {s}({type(s)}), s_org={s_origin}")
            s = self.parse_preprocessing(s)
            s_origin = s
            if s == None:
                raise
        except:
            CustomSlack(f'error occured raw:{s_origin_include_type}(type:{type(s_origin_include_type)}), parsed = {s}.\nstack={traceback.format_exc()}')
            return None
        
        
        global nldate_compare_matched_last
        with _nldate_mutex:
            if nldate_compare_matched_last:
                pt = matching_compare_set(s_origin, 2, **kwargs)
                if pt != None:
                    return pt
                
        #혹시 NLDate인지 체크한다
        nldate1 = re_nldateformatter.search(s_origin)
        if nldate1 != None:
            lsi = [int(nldate1[i+1]) for i in range(7)]
            s8 = nldate1[8]
            s8 = '+' if s8 == '-' else '-'
            tz = f"Etc/GMT{s8}{int(nldate1[9])}"
            d = datetime.datetime(lsi[0], lsi[1], lsi[2], lsi[3], lsi[4], lsi[5], lsi[6], tzinfo=pytz.timezone(tz) )
            #print("nldate1 succeeded!")
            return d
        nldate2 = re_nldateformatter2.search(s_origin)
        if nldate2 != None:
            lsi = [int(nldate2[i+1]) for i in range(7)]
            d = datetime.datetime(lsi[0], lsi[1], lsi[2], lsi[3], lsi[4], lsi[5], lsi[6])
            #print("nldate2 succeeded!")
            return d
        
        s = re.sub("(지금|현재)","",s)
        s = re.sub("(오늘)","",s)
        s = re.sub("(내일)","1일뒤",s)
        s = re.sub("(어제)","1일전",s)
        s = re.sub("(그제|그저께)","2일전",s)
        s = re.sub("(그그제|그그저께)","3일전",s)
        s = re.sub("(사흘)","3일",s)
        s = re.sub("(나흘)","3일",s)
        s = re.sub("(작년)","1년전",s)
        s = re.sub("(제작년)","2년전",s)
        s = re.sub("(내년|명년)","1년뒤",s)
        s = re.sub("(지난달|저번달)","1달전",s)
        s = re.sub("(다음달|내달)","1달후",s)
        #아무것도 없으면 현재
        if s.replace(" ","") == "":
            return basetime
        
        if log:
            print('tz_using = %s'%tz_using)
        
        bt = basetime
        
        s_splited = re.split(self.bef_aft_pattern, s)
        isin_prefix = False
        isin_postfix = False
        postfixes = self.befaft()
        prefix_str = ""
        postfix_str = ""
        for ss in s_splited:
            if isin_prefix:
                isin_postfix = True
                postfix_str += ss            
            else:
                if ss in postfixes:
                    isin_prefix = True
                    prefix_str += ss
                else:
                    prefix_str += ss
        matched_oneormore = False
        #전, 후 등이 있을 때 파싱
        if isin_prefix:            
            maxc = 10
            finded = None
            ptn = ""        
            for i in range(0, maxc):            
                ptn = self.amount_pattern * (maxc-i)
                finded = re.search(ptn, prefix_str)
                if finded != None:
                    break;
            if finded != None:
                matched_oneormore = True
                fs = re.findall(self.amount_pattern, finded[0])
                ff = re.findall(ptn + self.bef_aft_pattern, prefix_str)
                addtio = list(ff[0])[-1]
                ratio = 1
                if addtio in self.bef:
                    ratio = -1
                elif addtio in self.aft:
                    ratio = 1
                else:
                    CustomSlack("[%s] has no bef/aft"%(s_origin))
                dic = {}
                for p in fs:
                    num = int(float(p[1])) * ratio
                    standard = p[4]
                    key = self.dic_amount_standard.get(p[4], "")
                    #print("\t"+str(num)+key)
                    dic[key] = num
                #print("basetime = %s + (%s)"%(Util.DatetimeStr(bt), s))
                da = {}
                
                #for fast calc, using pydatetime
                if 'day' in dic != 0 or 'sec' in dic != 0:
                    td = datetime.timedelta(days=dic.get('day',0), seconds=dic.get('sec',0))
                    bt += td
                    dic['day'] = 0
                    dic['sec'] = 0
                    
                da = self.add_with_da(bt, dic)
                da = self.compat_da(da)
                bt = rtdt = datetime.datetime(da['year'], da['mon'], da['day'], da['hour'], da['min'], da['sec'], tzinfo=tz_using)
        
        #실제 날짜가 있을때 파싱
        if isin_postfix:
            strdata = postfix_str
        else:
            strdata = s
        maxc = 10
        finded = None
        ptn = ""        
        for i in range(0, maxc):            
            ptn = self.amount_pattern * (maxc-i)
            finded = re.search(ptn, strdata)
            if finded != None:
                break;
        if finded != None:
            matched_oneormore = True
            fs = re.findall(self.amount_pattern, finded[0])
            ratio = 1
            dic = {}
            for p in fs:
                num = int(float(p[1])) * ratio
                standard = p[4]
                key = self.dic_amount_standard.get(p[4], "")
                #print("\t"+str(num)+key)
                dic[key] = num
                da = self.replace_with_da(bt, dic, minsec0=True)
                da = self.compat_da(da)
                bt = rtdt = datetime.datetime(da['year'], da['mon'], da['day'], da['hour'], da['min'], da['sec'], tzinfo=tz_using)
                #print("result3 = %s"%(Util.DatetimeStr(rtdt)))                    
        #print("final result %s"%(Util.DatetimeStr(bt)))
        
        if not matched_oneormore:
            
            if log:
                print(f"no matched for {s_origin}. using strp with {nldate_compare_matched_last}")
            '''
            ptns = nldate_compare_set
            pt = None
            passed = False
            cutout = re.compile('[a-zA-Z]+')
            subded = re.sub('^\s+','',re.sub('\s+$','',cutout.sub(' ',s_origin)))
            
            
            lastp = ptns[0]
            for p in ptns:
                try:                    
                    pt = datetime.datetime.strptime(subded , p)
                    pt = datetime.datetime(pt.year, pt.month, pt.day, pt.hour, pt.minute, pt.second, pt.microsecond, tzinfo=tz_using)
                    passed = True
                    lastp = p
                    ptns.remove(p)
                    ptns.insert(0, p)
                    break
                except:
                    passed = False
            '''
            #kwargs['uid'] = uid 
            #kwargs['s_origin_include_type'] = s_origin_include_type
            
            
            #passed = False
            with _nldate_mutex:
                pt = matching_compare_set(s_origin, **kwargs)
                if pt != None:
                    bt = pt               
                
#                if nldate_compare_matched_last:
#                    pt = matching_compare_set(s_origin, **kwargs)
                    #passed = (pt != None)                
#                    if pt == None:
#                        pt = matching_dateparser(s_origin, self.GetDSTAddr(), **kwargs)
#                        bt = null_datetime() if not pt else pt
#                    else:
#                        bt = pt
                else:
                    #print(s_origin)
                    pt = matching_dateparser(s_origin, self.GetDSTAddr(), **kwargs)                
                    if not pt:
                        #print(f"pt = {pt}")
                        pt = matching_compare_set(s_origin, **kwargs)
                        #print(f"pt = {pt}")
                        #passed = (pt != None)                    
                        bt = null_datetime() if not pt else pt
                    else:
                        #final validateion
                        #가끔 dateparser가 20181002 같은걸 8100년으로 파싱한다... 이게 년월일이 뒤에 오는 방식으로 먼저 인식하는듯
                        #어쩔수없이 날짜 validation 해서 에러안나면 이걸로하기로함.
                        if pt.year <= 0 or pt.year >= 3000:
                            pt2 = matching_compare_set(s_origin, **kwargs)
                            if pt2 != None:
                                bt = pt2
                            else:
                                bt = pt
                        else:
                            bt = pt
        if bt == null_datetime():
            CustomSlack(f"{uid} ERROR date parsing with {s_origin}, using NaT, (origin input : {s_origin_include_type})")
        return bt
    
def matching_dateparser(s_origin, dstaddr, **kwargs):
    log = kwargs.get('log', False)    
    #uid = kwargs.get('uid', '')
    #s_origin_include_type = kwargs.get('s_origin_include_type','')
    if log:
        print("%s. using dateparser"%s_origin)
    
    d = dateparser.parse(s_origin +" "+dstaddr)
    
    if d == None:
        d = dateparser.parse(s_origin)
        
    return d
    
def matching_compare_set(s_origin, limit=1000000000, **kwargs):
    log = kwargs.get('log', False)
    tz_using = kwargs.get('tzinfo')
    if log:
        print(f"in strp {s_origin}")
    global nldate_compare_set        
    global nldate_compare_matched_last
    ptns = nldate_compare_set
    pt = None
    passed = False
    cutout = re.compile('[a-zA-Z]+')
    subded = re.sub('^\s+','',re.sub('\s+$','',cutout.sub(' ',s_origin)))
    #print(subded)    
    lastp = ptns[0]
    for _idx, p in enumerate(ptns[:limit]):
        #print("-----")
        try:                    
            pt = datetime.datetime.strptime(subded , p)
            pt = datetime.datetime(pt.year, pt.month, pt.day, pt.hour, pt.minute, pt.second, pt.microsecond, tzinfo=tz_using)
            passed = True
            lastp = p
            #print(f"succeed with pattern [{p}] in [{subded}]")
            if _idx != 0:
                ptns.remove(p)
                ptns.insert(0, p)
            break
        except:
            #print(f"failed with pattern [{p}] in [{subded}]")
            passed = False
            
    nldate_compare_matched_last = passed
    if passed:
        return pt
    else:
        return None

#DateParser().Parse("1일 1시간 전")

_dateParser = DateParser()


# In[ ]:


def GetReversePattern(nldate):
    '''
    NLDate를 입력받아, NLDate를 원래의 raw-string으로 돌리는 패턴을 찾아준다.
    다만 nldate가 일정 패턴 (YY-MM-DD 등)이 아니면 못찾는다.
    못 찾으면 ''(빈 string)을 리턴함
    '''
    s = nldate.rawdata
    global nldate_compare_set
    for ptn in nldate_compare_set:
        try:
            date = datetime.datetime.strptime(s, ptn)
            if date.year == nldate.year and date.month == nldate.month and date.day == nldate.day and date.hour == nldate.hour and date.minute == nldate.minute and date.second == nldate.second:
                return ptn
        except:
            pass
    return ''


# In[ ]:


def InitDateParser(dateparser = None):
    global _dateParser
    if _dateParser == None:        
        _dateParser = DateParser()
    else:
        _dateParser = dateparser
    return _dateParser


# In[ ]:





# In[ ]:


def DateTest(string, basetime = None):    
    #무한루프재현법
    global _dateParser
    if _dateParser == None:
        _dateParser = DateParser()
    basetime = _dateParser.now()
    #basetime = datetime.datetime(2019,7,9,15,57,16,tzinfo=_dateParser.tzinfo)    
    print("-"*100)    
    print("basetime : %s"%NLDUtil.DatetimeStr(basetime))
    print("input : %s"%string)
    print("result : %s"%NLDUtil.DatetimeStr(Date(string, basetime, log=True)))
          
if __name__ == "__main__":
    try:
        import ipdb        
    except:
        get_ipython().system('pip install ipdb')
        import ipdb
    print(f"지금utc = {Date('지금',tzinfo=pytz.timezone('utc'))}")
    
    DateTest('1000일 전 0시')
    
    DateTest('내일')
    DateTest('1시간 27분 뒤')
    
    #import ipdb; ipdb.set_trace()
    #Date('2019-12-15')
    DateTest('2019-12-15')
    
    DateTest('2019-12-16')
    DateTest('2019-12-17')
    DateTest('2019-12-18')
    #DateTest("어제 7시 16분")
    #DateTest("7년 2개월 40일 전 7시")
    
    DateTest("어제")
    DateTest("200분 200초 전")
    DateTest("200초 후")
    DateTest("7년 전")
    DateTest("3일")
    DateTest("3일 20시간전")
    DateTest("3일 20시간 23분전")    
    DateTest("다음달 3일 20시")
    DateTest("오늘 0시")
    DateTest("21:11:23")
    DateTest("하하하 이건 파싱 못하겠지 7일전")
    DateTest("20181002")
    
    print("-----")
    DateTest("2019-12-13T15:04:00Z")
    #ipdb.set_trace()   
    DateTest("2019-12-22 23:59:19.142593+09:00")
    DateTest("2019-12-22 23:59:19.142593")
    print("-----")
    print(Date("오늘").시24)
    print(Date("오늘").시12)
    print(Date("오늘").오전오후)
    print(Date("오늘 12시").시24)
    print(Date("오늘 12시").시12)
    print(Date("오늘 0시").시24)
    print(Date("오늘 0시").시12)
    print(Date("오늘 0시").오전오후)
    print("지금몇주? %s"%Date("지금").주)
    now = Date('')
    print(now)
    print(Date(now).Clone())
    print(Date('어제').CloneFrom(now))
    DateTest('7달 전 0시')
    DateTest('1000일 전 0시')
    


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




