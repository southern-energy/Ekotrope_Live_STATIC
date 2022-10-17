import time
import json
import requests
import ast
import os
import MySQLdb
import pandas as pd
import datetime as dt
from pandas import json_normalize
from warnings import filterwarnings
from sqlalchemy import create_engine
import urllib.request
import logging

urlString = 'http://pshmn.com/ppLnYNe' # Replace string with 'http://pshmn.com/p2PnYui' if you want Pushmon notifications.

start_time = time.perf_counter()

def flatten_json(y):
    """Takes complex layered jsons and flattens then to a spreadsheet format"""
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
                for a in x:
                    flatten(x[a], name + a + "_")
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + "_")
                i += 1
        else:
            out[name[:-1]] =  x
            
    flatten(y)
    return out
     
def renameColumns(df):
    """Shortens names of dataframe columns for export (find/replace)"""
    df.rename(columns=lambda x: x.replace("mechanicals", "mech"), inplace=True)
    df.rename(columns=lambda x: x.replace("mechanical", "mech"), inplace=True)
    df.rename(columns=lambda x: x.replace("Envelope", "Envel"), inplace=True)
    df.rename(columns=lambda x: x.replace("distribution", "dist"), inplace=True)
    df.rename(columns=lambda x: x.replace("type_", ""), inplace=True)
    df.rename(columns=lambda x: x.replace("Insulation", "Insul"), inplace=True)
    df.rename(columns=lambda x: x.replace("thermal", "therm"), inplace=True)
    #df.rename(columns=lambda x: x.replace("ufactor", "uFactor"), inplace=True)
    #df.rename(columns=lambda x: x.replace("SHGC", "shgc"), inplace=True)
    
def mysqlExport(mydb, file, table):
    """Exports generated csvs to the MySQL database"""
    path= os.getcwd()
    path = path.replace("\\", "/")
        
    file = ("%s/%s" % (path,file))
    sqlarg = "LOAD DATA LOCAL INFILE '%s' " % file
    sqlarg2 = sqlarg + ("REPLACE INTO TABLE %s FIELDS TERMINATED BY \',\' ignore 1 lines;" % table)
    cursor = mydb.cursor()
    cursor.execute(sqlarg2)
    mydb.commit()
    cursor.close()
    time.sleep(1)
    print("\tComplete!\n") 
    

def main(start_day, days_back_to_look):
    start_time = time.time()
    
    # Creates dataframes with defined columns so that it lines up with our MYSQL structure
    lastWeek_df_static = pd.read_csv("project_columns.csv", index_col=False)  
    projectMasterDF_static = pd.read_csv("project_details columns.csv", index_col=False)    
    housePlanMasterDF_static = pd.read_csv("houseplanM columns.csv", index_col=False)
    analysisAsModeled_static = pd.read_csv("eko_analysis columns.csv", index_col=False)
    analysiscompliance_static = pd.read_csv("eko_analysis_compliance columns.csv", index_col=False)
    mechEquipment_static = pd.read_csv("mech columns.csv", index_col=False)
    windows_static = pd.read_csv("window columns.csv", index_col=False)
    distsys_static = pd.read_csv("distsys columns.csv", index_col=False)
    
    # creates blank dataframes for filling up with data
    projectMasterDF = pd.DataFrame()
    housePlanMasterDF = pd.DataFrame()
    analysisAsModeled= pd.DataFrame()
    nc2012ref = pd.DataFrame()
    nchero2012ref = pd.DataFrame()
    EnergyStarV3 = pd.DataFrame()
    EnergyStarV31 = pd.DataFrame()
    TaxCredit45L = pd.DataFrame()
    mechEquipment = pd.DataFrame()
    windows = pd.DataFrame()
    distsys = pd.DataFrame()
    
    
    filterwarnings("ignore", category = MySQLdb.Warning)
    
    print("\n----Ekotrope Database Update----")
                
    # Connect to EkoTrope API for master project list
    print("\nAttempting to connect to Ekotrope API...\n")
    url = "http://app.ekotrope.com/api/v1/projects?status=SUBMITTED_TO_REGISTRY"
    
    payload = {}
    headers = {'authorization': 'Basic c2VtLWFwaS11c2VyOjh2JH4rOVda'}
    
    # Attempts connection 3 times before terminating program
    tries = 1
    while tries < 4:
        try:
            print("Attempt %s of 3..." % tries)    
            res = requests.request("GET", url, data=payload, headers=headers)
            
            # Turn json object into a dataframe object for managing
            ETcsv = pd.read_json(res.text, convert_axes=False)
            break
        except:
            print("\tUnexpected connection error occurred.")
            print("\tRe-attempting connection.\n")
            if tries == 3:
                print("\tUnable to connect. Please check your internet connectivity.")
                print("\tTerminating program.")
                exit()
            tries += 1
        
    # Convert last saved date/time to standard
    ETcsv["selfOrPlanLastSavedAt"] = pd.to_datetime(ETcsv["selfOrPlanLastSavedAt"])    
    
    # Select last week of data, cut the rest. Save into new dataframe.
    print("\tNarrowing scope of data...")
    range_max = ETcsv["selfOrPlanLastSavedAt"].max()
    range_max = dt.datetime.now(dt.timezone.utc) +dt.timedelta(days=start_day)
    range_min = range_max - dt.timedelta(days=days_back_to_look)
    
    print("\tData between %s and %s.\n" % (range_min,range_max))

    print(range_max)
    print (ETcsv["selfOrPlanLastSavedAt"][0])
    
    lastWeek_df = ETcsv[(ETcsv["selfOrPlanLastSavedAt"] >= range_min) &
                   (ETcsv["selfOrPlanLastSavedAt"] <= range_max)]
    
    
    print("\tSuccessfully connected and sorted.\n")
    numProjects = str(len(lastWeek_df.index))
    print("----Number of projects to update: %s ----" % numProjects)
    print()
    
    if numProjects == "0":
        print("No projects to update this time brotha!")
        
    
    filename1 = "ShortList.csv"
    lastWeek_df = lastWeek_df[list(lastWeek_df_static.columns)]
    lastWeek_df.to_csv("%s" % filename1, index=False, index_label=False)    

    print("Step 1/4: Compile project master list...")
    
    # Goes through project list to gather project ids, creates DF, appends to master
    for index,row in lastWeek_df.iterrows():
        currentID = row["id"]
        print("\tCurrently pulling project info for: " + str(currentID))
        url = "http://app.ekotrope.com/api/v1/projects/%s" % currentID
        res = requests.request("GET", url, data=payload, headers=headers).json()
        res_flattened = flatten_json(res)
        resultDF = pd.DataFrame(res_flattened, index=[0])
        projectMasterDF = projectMasterDF.append(resultDF)
        
    #after filling up dataframe make sure we have all of the correct columns    
    for x in list(projectMasterDF_static.columns):
        #print(x)
        if x not in projectMasterDF.columns:
            #print(x + ' aint in the df')
            projectMasterDF[x] = ''   
    #now that we know we have all of the columns we need, only keep the columns we need    
    projectMasterDF =  projectMasterDF[list(projectMasterDF_static.columns)]
    projectMasterDF = projectMasterDF.replace({',': ' '}, regex=True) # remove all commas
    projectMasterDF= projectMasterDF.replace({r'\r': ' '}, regex=True)# remove all returns
    projectMasterDF= projectMasterDF.replace({r'\n': ' '}, regex=True)# remove all newlines
            
    print("SUCCESS: Project list compiled!\n")
    filename2 = "projectMaster.csv"
    projectMasterDF.to_csv("%s" % filename2, index=False, index_label=False)
    print("Step 2/4: Compile SEM-Entered house plans...")
     
    # Goes through project list and pulls SEM-generated house plans
    for index,row in projectMasterDF.iterrows():
        try:   
            currentID = row["masterPlanId"]
            print("\tCurrently pulling house plans for: " + str(currentID))
            url = "http://app.ekotrope.com/api/v1/houseplans/%s" % currentID
            res = requests.request("GET", url, data=payload, headers=headers).json()
            res = str(res)
            #print(type(res))
            res  = res.replace("179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000","0")
            #print(type(res))
            res = ast.literal_eval(res)
            #print(type(res))               
            planID = res["id"]
            
            #Splits out mechanical equipment into its own dataframe
            mechres = res["mechanicals"]["mechanicalEquipment"]
            mechtempdf = json_normalize(mechres)
            mechtempdf["id"] = planID
            mechEquipment = mechEquipment.append(mechtempdf)
            
            #Splits out windows into its own dataframe
            windowres = res['thermalEnvelope']['windows']
            windowtempdf = json_normalize(windowres)
            windowtempdf["id"] = planID
            windows = windows.append(windowtempdf)


            #Splits out mechanical equipment into its own dataframe
            distsysres = res["mechanicals"]["distributionSystems"]
            #print(distsysres)
            #distsysres = flatten_json(distsysres)
            distsystempdf = json_normalize(distsysres)
            distsystempdf["id"] = planID
            distsys = distsys.append(distsystempdf)
            filename_distsys = "distsys.csv"
            distsys.to_csv("%s" % filename_distsys, index=False, index_label=False)
            
            #create master houseplandf turn st into its own dataframe
            res_flattened = flatten_json(res)
            resultDF = pd.DataFrame(res_flattened, index=[0])                    
            housePlanMasterDF = housePlanMasterDF.append(resultDF)             
        except Exception as e:
           with open("ekotrope_super_looper_ProjectMaster_log.csv", "a") as log:
              log.write(str(e) +","+row["masterPlanId"]+ "\n")
           print('Error: ' + str(e))   
           pass  
        
    # after filling up the dataframe make sure we have all of the correct columns    
    
    #--------------mech equipment
    for x in list(mechEquipment_static.columns):
        if x not in mechEquipment.columns:
            print(x + ' aint in the df')
            mechEquipment[x] = ''  
    mechEquipment =  mechEquipment[list(mechEquipment_static.columns)]    #now that we know we have all of the columns we need, only keep the columns we need  
    mechEquipment  = mechEquipment.replace({',': ' '}, regex=True) # remove all commas
    mechEquipment = mechEquipment.replace({r'\r': ' '}, regex=True)# remove all returns
    mechEquipment = mechEquipment.replace({r'\n': ' '}, regex=True)# remove all newlines  
    filename_mech = "mech_equip.csv"
    mechEquipment.to_csv("%s" % filename_mech, index=False, index_label=False)
    
    #--------------windows
    for x in list(windows_static.columns):
        if x not in windows.columns:
            print(x + ' aint in the df')
            windows[x] = ''  
    windows =  windows[list(windows_static.columns)]    #now that we know we have all of the columns we need, only keep the columns we need  
    windows = windows.replace({',': ' '}, regex=True) # remove all commas
    windows = windows.replace({r'\r': ' '}, regex=True)# remove all returns
    windows = windows.replace({r'\n': ' '}, regex=True)# remove all newlines  
    filename_windows = "windows_equip.csv"
    windows.to_csv("%s" % filename_windows, index=False, index_label=False)


      #--------------distsy equipment
    for x in list(distsys_static.columns):
        if x not in distsys.columns:
            print(x + ' aint in the df')
            distsys[x] = ''  
    distsys =  distsys[list(distsys_static.columns)]    #now that we know we have all of the columns we need, only keep the columns we need  
    distsys  = distsys.replace({',': ' '}, regex=True) # remove all commas
    distsys = distsys.replace({r'\r': ' '}, regex=True)# remove all returns
    distsys = distsys.replace({r'\n': ' '}, regex=True)# remove all newlines  
    filename_distsys = "distsys_equip.csv"
    distsys.to_csv("%s" % filename_distsys, index=False, index_label=False)
    
    print("SUCCESS: SEM-Entered house plans compiled!\n")
     
    # Modifies house plan for export (renames col, de-dupes, splits, makes sure id is in both dfs)
    
    renameColumns(housePlanMasterDF)
    housePlanMasterDF = housePlanMasterDF.loc[:,~housePlanMasterDF.columns.duplicated()]
    housePlanMasterDF   = housePlanMasterDF.replace({',': ' '}, regex=True) # remove all commas
    housePlanMasterDF  = housePlanMasterDF.replace({r'\r': ' '}, regex=True)# remove all returns
    housePlanMasterDF = housePlanMasterDF.replace({r'\n': ' '}, regex=True)# remove all newlines  
    housePlanMasterDF.to_csv("houseplanMaster.csv") # Can uncomment for csv generation
    
    hp1 =  pd.read_csv('houseplan1 columns.csv') # defines columns and order of columns from database so that our dataframe can go to csv and then to the database in the correct order.
    for x in list(hp1.columns):
         if x not in housePlanMasterDF.columns:
             #print(x + ' aint in the df')
             housePlanMasterDF[x] = ''    
         
    hp2 =  pd.read_csv('houseplan2 columns.csv') # defines columns and order of columns from database so that our dataframe can go to csv and then to the database in the correct order.    
    for x in list(hp2.columns):
         if x not in housePlanMasterDF.columns:
             #print(x + ' aint in the df')
             housePlanMasterDF[x] = ''    
             
    # ---- house plan 2           
    housePlan1 = housePlanMasterDF[list(hp1.columns)]
    filename3 = "housePlan1.csv"
    housePlan1.to_csv("%s" % filename3,index=False)
    
    # ---- house plan 2    
    housePlan2 = housePlanMasterDF[list(hp2.columns)]                 
    housePlan2.insert(loc=0, column="id", value=housePlan1["id"])   
    filename4 = "housePlan2.csv"
    housePlan2.to_csv("%s" % filename4,index=False)
         
    print("Step 3/4: Run code analysis on house plans...")
     
# Goes through project list and pulls Ekotrope calculated plans (AsModeled & HERS Rated)
    for index,row in projectMasterDF.iterrows():
        currentID = row["masterPlanId"]
        print("\tCurrently running analysis on plan: " + str(currentID))
         
        # Call for EkotropeAsModeled  Analysis
        url = "http://api.ekotrope.com/api/v1/planAnalysis/%s?buildingType=EkotropeAsModeled" % currentID
        res = requests.request("GET", url, data=payload, headers=headers).json()
        res_flattened = flatten_json(res)
        resultDF = pd.DataFrame(res_flattened, index=[0])
        analysisAsModeled = analysisAsModeled.append(resultDF)
        
        # Call for NC 2012 Analysis
        url = "http://api.ekotrope.com/api/v1/planAnalysis/%s?buildingType=NorthCarolina2012Reference" % currentID
        res = requests.request("GET", url, data=payload, headers=headers).json()
        res_flattened = flatten_json(res)
        resultDF = pd.DataFrame(res_flattened, index=[0])
        nc2012ref = nc2012ref.append(resultDF)
    
        # Call for ESv3 Analysis
        url = "http://api.ekotrope.com/api/v1/planAnalysis/%s?codesToCheck=EnergyStarV3&buildingType=EkotropeAsModeled" % currentID
        res = requests.request("GET", url, data=payload, headers=headers).json()
        res_flattened = flatten_json(res)
        resultDF = pd.DataFrame(res_flattened, index=[0])
        EnergyStarV3 = EnergyStarV3.append(resultDF)

        # Call for ESv3.1 Analysis
        url = "http://api.ekotrope.com/api/v1/planAnalysis/%s?codesToCheck=EnergyStarV31&buildingType=EkotropeAsModeled" % currentID
        res = requests.request("GET", url, data=payload, headers=headers).json()
        res_flattened = flatten_json(res)
        resultDF = pd.DataFrame(res_flattened, index=[0])
        EnergyStarV31 = EnergyStarV31.append(resultDF)

        # Call for Tax Credit Analysis
        url = "http://api.ekotrope.com/api/v1/planAnalysis/%s?codesToCheck=TaxCredit45L&buildingType=EkotropeAsModeled" % currentID
        res = requests.request("GET", url, data=payload, headers=headers).json()
        res_flattened = flatten_json(res)
        resultDF = pd.DataFrame(res_flattened, index=[0])
        TaxCredit45L = TaxCredit45L.append(resultDF)
        
        
    analysisAsModeled = analysisAsModeled.replace({',': ' '}, regex=True) # remove all commas
    analysisAsModeled = analysisAsModeled.replace({r'\r': ' '}, regex=True)# remove all returns
    analysisAsModeled = analysisAsModeled.replace({r'\n': ' '}, regex=True)# remove all newlines             
    analysisAsModeled = analysisAsModeled[list(analysisAsModeled_static.columns)]
    filename5 = "houseAnalysisEko.csv"
    analysisAsModeled.to_csv("%s" % filename5, index=False, index_label=False)
    
    nc2012ref  = nc2012ref.replace({',': ' '}, regex=True) # remove all commas
    nc2012ref  = nc2012ref.replace({r'\r': ' '}, regex=True)# remove all returns
    nc2012ref  = nc2012ref.replace({r'\n': ' '}, regex=True)# remove all newlines             
    nc2012ref  = nc2012ref[list(analysisAsModeled_static.columns)]
    filenameNC2012ref = "nc2012ref.csv"
    nc2012ref.to_csv("%s" % filenameNC2012ref, index=False, index_label=False)
    
    TaxCredit45L  = TaxCredit45L.replace({',': ' '}, regex=True) # remove all commas
    TaxCredit45L  = TaxCredit45L.replace({r'\r': ' '}, regex=True)# remove all returns
    TaxCredit45L  = TaxCredit45L.replace({r'\n': ' '}, regex=True)# remove all newlines             
    TaxCredit45L  = TaxCredit45L[list(analysiscompliance_static.columns)]
    filenameTaxCredit45L = "TaxCredit45L.csv"
    TaxCredit45L.to_csv("%s" % filenameTaxCredit45L, index=False, index_label=False)

    EnergyStarV3  = EnergyStarV3.replace({',': ' '}, regex=True) # remove all commas
    EnergyStarV3  = EnergyStarV3.replace({r'\r': ' '}, regex=True)# remove all returns
    EnergyStarV3  = EnergyStarV3.replace({r'\n': ' '}, regex=True)# remove all newlines             
    EnergyStarV3  = EnergyStarV3[list(analysiscompliance_static.columns)]
    filenameEnergyStarV3 = "EnergyStarV3.csv"
    EnergyStarV3.to_csv("%s" % filenameEnergyStarV3, index=False, index_label=False)

    EnergyStarV31  = EnergyStarV31.replace({',': ' '}, regex=True) # remove all commas
    EnergyStarV31  = EnergyStarV31.replace({r'\r': ' '}, regex=True)# remove all returns
    EnergyStarV31  = EnergyStarV31.replace({r'\n': ' '}, regex=True)# remove all newlines             
    EnergyStarV31  = EnergyStarV31[list(analysiscompliance_static.columns)]
    filenameEnergyStarV31 = "EnergyStarV31.csv"
    EnergyStarV31.to_csv("%s" % filenameEnergyStarV31, index=False, index_label=False)
    
    print("SUCCESS: House Analysis statistics compiled!\n")

    print("Step 4/4: Exporting data to MySQL...\n")
     
    # Exports all dataframes to MySQL Database
     
    mydb = MySQLdb.connect(
        host='104.154.197.202',
        port=3306,
        user='ekotrope_python',
        passwd='Clyde<3sek0troPe',
        db='sem_eko',
        charset='utf8',
        local_infile = 1)
     
    print("\tCurrently Exporting Master Project list...")
    mysqlExport(mydb, filename1, "project")
    
    print("\tCurrently Exporting Project Details...")
    mysqlExport(mydb, filename2, "project_details")
    
    print("\tCurrently Exporting House Plans...")
    mysqlExport(mydb, filename3, "house_plans1")
    mysqlExport(mydb,filename4, "house_plans2")
    
    print("\tCurrently Exporting AsModeled Analysis...")
    mysqlExport(mydb, filename5, "asmodeled_analysis")
    mysqlExport(mydb, filenameNC2012ref, "nc2012ref_analysis")
    mysqlExport(mydb, filenameTaxCredit45L, "TaxCredit45L_analysis")
    mysqlExport(mydb, filenameEnergyStarV3, "energystarv3_analysis")
    mysqlExport(mydb, filenameEnergyStarV31, "energystarv31_analysis")     
    print("\tCurrently Exporting Mech_Equip...")
    
    #delete any equipment with ids in current dataframe from database before inserting new rows in database to avoid creating duplicates.
    idList =  list(mechEquipment['id']) # convert id dataframe column to list
    idList = str(idList)[1:][:-1] # convert list to string and remove first and last characters which are brackets
    print(idList)
    deleteStr = 'delete from mech_equip where id in (%s)' % idList # insert list string into mysql delete query    
    # connect to database and run delete script
    cursor = mydb.cursor()
    cursor.execute(deleteStr)
    mydb.commit()
    cursor.close()   
    mysqlExport(mydb, filename_mech, "mech_equip")
    
    print("\tCurrently Exporting Windows...")
    
    #delete any equipment with ids in current dataframe from database before inserting new rows in database to avoid creating duplicates.
    idList =  list(windows['id']) # convert id dataframe column to list
    idList = str(idList)[1:][:-1] # convert list to string and remove first and last characters which are brackets
    print(idList)
    deleteStr = 'delete from windows where id in (%s)' % idList # insert list string into mysql delete query    
    # connect to database and run delete script
    cursor = mydb.cursor()
    cursor.execute(deleteStr)
    mydb.commit()
    cursor.close()   
    mysqlExport(mydb, filename_windows, "windows")

    print("\tCurrently Exporting Distribution Systems...")
    
    #delete any equipment with ids in current dataframe from database before inserting new rows in database to avoid creating duplicates.
    idList =  list(distsys['id']) # convert id dataframe column to list
    idList = str(idList)[1:][:-1] # convert list to string and remove first and last characters which are brackets
    print(idList)
    deleteStr = 'delete from distsys where id in (%s)' % idList # insert list string into mysql delete query    
    # connect to database and run delete script
    cursor = mydb.cursor()
    cursor.execute(deleteStr)
    mydb.commit()
    cursor.close()   
    mysqlExport(mydb, filename_distsys, "distsys")
  
    print("----All project files updated!----\n")
     
    #Cleans up csv files. Can comment these out to keep related csvs
#     os.remove("%s" % filename1) #Master Project List
#     os.remove("%s" % filename2) #Project Details
#     os.remove("%s" % filename3) #House Plan 1
#     os.remove("%s" % filename4) #House Plan 2
#     os.remove("%s" % filename5) #AsModeled Analysis
#     os.remove("%s" % filename6) #HERSRated Analysis
     
    scriptTime = time.time() - start_time
    print("Script completed in %.2f seconds." % scriptTime)
     
#main(0,.5) #parameter is start date and days back in time to look ----- example "main(0, 1)" starts today and goes back 1 day)

    # 1 - Export Dash Services
#------------------------------------------------

def eko_super_loop(start_day,end_day,interval):

    while start_day > -1:
        print('getting tropes from '+ str(start_day)+' days ago to '+ str(end_day)+' days ago with '+str(interval)+' days in between' )
        try:
            print('ran')
            main(-start_day,interval)
            handle = urllib.request.urlopen(urlString)
            handle.read()
            handle.close()
            with open("LOG_ekotrope_super_looper_log.csv", "a") as log:
                log.write(str(start_day)+','+ str(end_day)+','+str(interval)+','+'ran great brotha' +','+ str(dt.datetime.now()) +'\n')
        except Exception as e:
                with open("LOG_ekotrope_super_looper_log.csv", "a") as log:
                         log.write(str(start_day)+','+ str(end_day)+','+str(interval)+','+str(e) +','+ str(dt.datetime.now()) +','+ 'didnt work this time brotha'+ '\n')
                         #log.write(logging.Formatter('[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(funcName)s %(levelname)s - %(message)s','%m-%d %H:%M:%S')+'\n')
                print('Error: ' + str(e))
        start_day = start_day - interval
        print(str(start_day) + ' after add')
        continue

eko_super_loop(1,0,1)

end_time = time.perf_counter()

time_spent_running_manually = end_time - start_time

time_spent_running_manually_in_minutes = (time_spent_running_manually / 60)

print("You've spent " + str(time_spent_running_manually_in_minutes) + " minutes running this manually.")

print("Finished Running at", time.ctime(time.time()))
