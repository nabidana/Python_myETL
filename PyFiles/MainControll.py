import LJW_Middledef as mydef

#vscode는 어떤 폴더에 py파일을 넣어도 절대경로로 선택된 프로젝트 폴더를 root경로로 가지고 있다.
#따라서 테스트는 root경로에서 따라 들어가서 실행.

#제출용 메인 코드


#테스트용
try :
    dbinfo = mydef.readoracleDBinfofile('../TestFolder/OCinformation.txt')
    print('your db information is ',dbinfo)
except : print("ERROR")
# try :
#     fileslist = mydef.readallfilelistinput('../TestFolder')
#     print('your file list is',fileslist)
# except : print("ERROR")
try :
    fileslist = mydef.readallfilelist()
except : print("ERROR")
try :
    mydef.SaveToOracle(dbinfo, fileslist)
except : print("ERROR")
