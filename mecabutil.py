#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import mecab
import hgtk


# In[ ]:


#수제작 mecab 태그
mecab_tag_dic = { "NNG":"일반 명사",
"NNP":"고유 명사",
"NNB":"의존 명사",
"NNBC":"단위를 나타내는 명사",
"NR":"수사",
"NP":"대명사",
"VV":"동사",
"VA":"형용사",
"VX":"보조 용언",
"VCP":"긍정 지정사",
"VCN":"부정 지정사",
"MM":"관형사",
"MAG":"일반 부사",
"MAJ":"접속 부사",
"IC":"감탄사",
"JKS":"주격 조사",
"JKC":"보격 조사",
"JKG":"관형격 조사",
"JKO":"목적격 조사",
"JKB":"부사격 조사",
"JKV":"호격 조사",
"JKQ":"인용격 조사",
"JX":"보조사",
"JC":"접속 조사",
"EP":"선어말 어미",
"EF":"종결 어미",
"EC":"연결 어미",
"ETN":"명사형 전성 어미",
"ETM":"관형형 전성 어미",
"XPN":"체언 접두사",
"XSN":"명사 파생 접미사",
"XSV":"동사 파생 접미사",
"XSA":"형용사 파생 접미사",
"XR":"어근",
"SF":"마침표, 물음표, 느낌표",
"SE":"줄임표 …",
"SSO":"여는 괄호 (,[ ",
"SSC":"닫는 괄호 ),] ",
"SC":"구분자 , · / :",
"SY":"기호",
"SL":"외국어",
"SH":"한자",
"SN":"숫자",
"UNKNOWN":"분석불가",
"UNA":"ㅋ",
"NA":"없음",
"SPACE":"공백"
}


# In[ ]:


def Decompose(charac):
    try:
        return hgtk.letter.decompose(charac)
    except:
        return (charac, '', '')

def Compose(*args):
    try:
        if len(args) == 3:        
            return hgtk.letter.compose(args[0], args[1], args[2])
        elif len(args) == 2:
            return hgtk.letter.compose(args[0], args[1])
        else:
            return hgtk.letter.compose(args[0])
    except:
        return "".join(args)
    
def PosToKorean(posname) -> str:
    return "+".join([mecab_tag_dic[p] for p in posname.split('+')])

#밤새서 만든 pattern finder 로직
def PosWithSpace(sentence, mecab_ko = None, extend = False):
    if mecab_ko == None:
        mecab_ko = mecab.MeCab()
    
    #pos태그된곳에 띄어쓰기 추가 (태그는 SPACE)
    pos = mecab_ko.pos(sentence)
    sentence_sliced = sentence
    pos_with_space = []
    for p in pos:        
        while sentence_sliced[0] == ' ':
            sentence_sliced = sentence_sliced[1:]
            pos_with_space.append( (' ', 'SPACE') )        
        sentence_sliced = sentence_sliced[len(p[0]):]
        pos_with_space.append(p)
    
    if extend:
        pos_extended = []
        for p in pos_with_space:
            pos_extended += [('', pp) for pp in p[1].split('+')]
            pos_extended[len(pos_extended) - 1] = (p[0], pos_extended[len(pos_extended) - 1][1])    
        return pos_extended
    else:
        return pos_with_space

def ExtendedPosToNormalPos(posed_extended):
    posed_normal = []
    for idx, p in enumerate(posed_extended):
        if len(posed_normal) > 0:
            if posed_normal[-1][0] == '':
                posed_normal[-1] = (posed_normal[-1][0] + p[0], posed_normal[-1][1] + "+" + p[1])
            else:
                posed_normal.append(p)
        else:
            posed_normal.append(p)
    return posed_normal


# In[ ]:





# In[ ]:




