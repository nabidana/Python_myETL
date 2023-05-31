import pandas as pd
from sqlalchemy import create_engine, types
import cx_Oracle
import pymysql
import numpy as np
import re
import os

def SaveToOracle(oracleDBinfo, filenamesinfo) :
    """
    SaveToOracle함수.
    사용방법 :
    SaveToOracle( '오라클정보가 담긴 배열[id, pw, ip, port, dbname]'<String형 배열> , 'csv및 excel파일 이름 및 경로[]'<String형 배열>
    return값 : 없음
    """
    filecount = len(filenamesinfo);   #파일의 개수 파악하기. 리스트형식이기때문
    dbid = oracleDBinfo[0]
    dbpw = oracleDBinfo[1]
    dbip = oracleDBinfo[2]
    dbport = oracleDBinfo[3]
    dbname = oracleDBinfo[4]
    dbcategory = oracleDBinfo[5]
    engine;
    if(dbcategory == 'oracle') :
        engine = create_engine('oracle://{}:{}@{}:{}/{}'.format(dbid, dbpw, dbip, dbport, dbname))
    elif(dbcategory == 'mysql') :
        engine = create_engine('mysql+mysqldb://{}:{}@{}:{}/{}'.format(dbid, dbpw, dbip, dbport, dbname))
    else :
        print("ERROR. Can't read DB category. please check your files.")
        return
    
    #engine = create_engine('oracle://{}:{}@{}:{}/{}'.format(dbid, dbpw, dbip, dbport, dbname))
    for i in range(filecount) :
        whatfiletype = filenamesinfo[i].split('.')  #확장자명 파악하기위해서 문자열을 자름
        thisfiletype = whatfiletype[-1] #확장자명 뽑기.
        imsifilespt = whatfiletype[-2].split('/') #확장자명 앞에있는것을 가져와서 파일경로 문자'/'을 자름
        savetablename = imsifilespt[-1] #최종적으로 확장자뒤에있는것이 이름이기 때문.
        savetablename = savetablename.lower()
        
        if(thisfiletype == 'csv') : 
            #파일읽기 실패시 예외 처리.
            try :
                putdataframe = pd.read_csv(filenamesinfo[i])   #csv파일로 불러들이기
            except : print("ERROR. Can't read ",filenamesinfo[i]," please check it again.")
            objectcolumcount = list(putdataframe.columns[putdataframe.dtypes == 'object'])
            typeDict={}
            putdataframe.astype('object')
            for i in range(len(objectcolumcount)) :
                typeDict[ objectcolumcount[i] ] = types.VARCHAR(100)
            #오라클 저장 실패시 예외 처리
            try :
                putdataframe.to_sql(name=savetablename, con=engine, dtype=typeDict, if_exists='append', index=False)
            except : print("ERROR. Can't save to Oracle Database. please check your Database or Your file.")
        elif (thisfiletype == 'excel') :
            #파일읽기 실패시 예외 처리.
            try :
                putdataframe = pd.read_excel(filenamesinfo) #엑셀파일 불러들이기
            except : print("ERROR. Can't read ",filenamesinfo," please check it again.")
            objectcolumcount = list(putdataframe.columns[putdataframe.dtypes == 'object'])
            typeDict={}
            for i in range(len(objectcolumcount)) :
                typeDict[objectcolumcount[i] ] = types.VARCHAR(100)
            #오라클 저장 실패시 예외 처리
            try :
                putdataframe.to_sql(name=savetablename, con=engine, dtype=typeDict, if_exists='append', index=False)
            except : print("ERROR. Can't read ",filenamesinfo," please check it again.")
        else :
            print("It's not '.csv' or '.exel'. please check your files.")
        
def readoracleDBinfofile(filelinkpath) :
    """
    오라클DB정보가 담긴 파일을 읽어오는 함수
    사용방법 :
    readoracleDBinfofile( '파일경로.txt' <String>형>)
    파일은 TXT파일로 저장되어야함.
    파일내용은 같은 폴더의 sample.txt을 참조할것.
    return값 : 배열[]
    """
    whatfiletype = filelinkpath.split('.')[-1]
    returnlist = []
    if(whatfiletype == 'txt') :
        try :
            inputvalues = pd.read_table(filelinkpath)
        except : print("File read error. please check your oracledbinfo file.")
        #데이터무결성에 의거 파일을 훼손햇을경우
        if(inputvalues.columns[0] != "PutDBinfo") :
            print('ERROR Please check sample.txt (무결성 훼손)')
        else :
            #데이터프레임의 컬럼길이만큼 반복하여 배열에 담아서 return함
            #print(inputvalues)
            for i in range(len(inputvalues)) :
                returnlist.append(inputvalues.PutDBinfo[i])
                #print(inputvalues.PutDBinfo[i])
            return returnlist
    else :
        print("Please setting file path for '~~~~.txt' .")
    
    
def readallfilelist() :
    """
    같은 폴더내의 'filepath.txt'에 입력한 경로의 모든 파일을 읽어오는 함수
    사용방법 :
    같은 폴더내의 'filepath.txt' 경로를 정확하게 입력한다.
    return값 : 
    """
    readpathfile = pd.read_table('./filepath.txt')
    #데이터무결성에 의거 파일을 훼손햇을경우
    checkfile = readpathfile
    if(checkfile.columns[0] != "Inputfilepath") :
        print("ERROR Please Check filepath.txt (무결성 훼손)")
    else :
        setfilepath = checkfile.Inputfilepath[0]
        readallfilelist = os.listdir(setfilepath)
        for i in range(len(readallfilelist)) :
            readallfilelist[i] = setfilepath+"/"+readallfilelist[i]
    return readallfilelist

def readallfilelistinput(linkpath) :
    """
    폴더내의 모든 파일을 읽어오는 함수
    사용방법 :
    readallfilelist( '모든파일을 읽을 폴더 경로' <String형>)
    return값 : 
    """
    allfiles = os.listdir(linkpath)
    for i in range(len(allfiles)) :
        allfiles[i] = linkpath+"/"+allfiles[i]
    return allfiles
#vscode는 기본적으로 경로를 아무리 폴더를 만들어도 최초에 선택한 폴더를 root경로로 가지고 있음.

#테스트용
# dbinfo = readoracleDBinfofile('./Pyfiles/sample.txt')
# print('your db information is ',dbinfo)
# fileslist = readallfilelist('./TestFolder')
# print('your file list is',fileslist)
# asdf = readallfilelist()
# print(asdf)

# SaveToOracle(dbinfo, fileslist)
