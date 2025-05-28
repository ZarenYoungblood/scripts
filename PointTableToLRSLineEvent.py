import pandas as pd

pts     = pd.read_excel('signs.xlsx','Signs').sort_values(by=['Route_ID','MPT'])
rs      = pd.read_excel('signs.xlsx','RouteStatus')

ptRID       = pts['Route_ID']
ptEvents    = pts['Sign_Text']
ptMP        = pts['MPT']

ptRIDstr    = 'Route_ID'
ptMPstr     = 'MPT'

rsRID   = 'ROUTE_ID'
rsBMP   = 'FROM_MEASURE'
rsEMP   = 'TO_MEASURE'

#'''find the undriven directions, mirror them and combine them with pts (some points will be out of bounds and need to be filtered out)
dirs = ['00','EB','WB','NB','SB']   #makes a table of RouteIDs of all possible directions in pts (13 char IDs only)
IDs = pd.DataFrame()
for i in range(len(dirs)):
    IDs[dirs[i]] = ptRID.str[:11] + dirs[i]

exists = pd.DataFrame(False, index=range(len(IDs)), columns=dirs)
recorded = pd.DataFrame(False, index= range(len(IDs)), columns= dirs)
unrecorded = pd.DataFrame('', index= range(len(IDs)), columns= dirs)

for row in range(IDs.shape[0]):     #this takes awhile, better way to do things to every element in a table?
    for col in range(IDs.shape[1]):

        if IDs.iloc[row,col] in rs[rsRID].values:   #sets 'exists' to True if the ID is in RouteStatus
            exists.iloc[row,col] = True

        if IDs.iloc[row,col] in ptRID.values:   #sets 'recorded' to True if the ID is in pts
            recorded.iloc[row,col] = True

        if exists.iloc[row,col] == True and recorded.iloc[row,col] == False:    #inputs the RID where it exists but was not recorded
            unrecorded.iloc[row,col] = IDs.iloc[row,col]

ptEvents    = pd.DataFrame(ptEvents)
ptRID       = pd.DataFrame(ptRID)
ptMP        = pd.DataFrame(ptMP)
udrs        = pd.DataFrame()

udrs[ptRID.columns.to_list()[0]] = unrecorded['00'] + unrecorded['EB'] + unrecorded['WB']+ unrecorded['NB'] + unrecorded['SB']
    #^smushes unrecorded into one column. Unsure if there is ever a case where more than one RID will appear but it will cause problems if there are.

for col in ptEvents.columns.to_list():  #ads the events to the udrs (the for loop allows multiple fields to be preserved)
    udrs[col]    = ptEvents[col].copy()
udrs[ptMP.columns.to_list()[0]] = ptMP.copy() #adds the MPs to the udrs
udrs = udrs[udrs[ptRID.columns.to_list()[0]] != ''] #removes the rows with blank RIDs

allPts = pd.concat([pts,udrs],ignore_index=True)

#'''get the appropriate bmp and emp for each mp, filter out the out of bounds mps
bmp = []
emp = []
for rid in range(allPts.shape[0]):
    #makes a smaller route status list with just the matching route ids
    sample  = rs[rs[rsRID] == allPts[ptRIDstr][rid]]
    i = 0
    while i <= sample.shape[0]-1:
        if sample.iloc[i][rsBMP]  <= allPts[ptMPstr][i] and allPts[ptMPstr][i] < sample.iloc[i][rsEMP]:
            #grabs bmp/emp of the correct segment
            bmp.append(sample.iloc[i][rsBMP])
            emp.append(sample.iloc[i][rsEMP])
            break
        elif i == sample.shape[0]-1:
            #if all the segments have been checked and no match found, puts in a blank placeholder
            bmp.append('')
            emp.append('')
        i+=1

#combine and filter
allPts['BMP'] = bmp
allPts['EMP'] = emp
allPts = allPts[allPts['BMP'] != '']

#make the segments
allPts.sort_values(by=[ptRIDstr,ptMPstr], inplace=True)
allPts = allPts.reset_index(drop=True)

firstSegs = allPts.copy().drop_duplicates(subset=[ptRIDstr,'BMP'],keep='first')
firstSegs['EMP']            = firstSegs['MPT']

toMP  = []
for row in range(allPts.shape[0]):
    if row + 1 >= allPts.shape[0]:
        toMP.append(allPts['EMP'][row])
    elif allPts[ptRIDstr][row] == allPts[ptRIDstr][row+1] and allPts['BMP'][row] == allPts['BMP'][row+1]:
        toMP.append(allPts[ptMPstr][row+1])
    else:
        toMP.append(allPts['EMP'][row])

allPts['BMP']   = allPts['MPT']
allPts['EMP']   = toMP

allSegs = pd.concat([firstSegs,allPts],ignore_index=True)
allSegs.sort_values(by=[ptRIDstr,'BMP'], inplace=True)
allSegs = allSegs.reset_index(drop=True)

#merge contiguous events
i = 0
while i < allSegs.shape[0]:
    if i == 0:
        allSegs.at[0, 'From_Measure'] = allSegs.at[0, 'BMP']

    n = 0 #number of rows that make up a contiguous event
    # Make sure we don't go out of bounds
    while (i + n + 1) < allSegs.shape[0] and \
          allSegs.at[i + n, ptRIDstr] == allSegs.at[i + n + 1, ptRIDstr] and \
          allSegs.at[i + n, 'Sign_Text'] == allSegs.at[i + n + 1, 'Sign_Text'] and \
          allSegs.at[i + n, 'EMP'] == allSegs.at[i + n + 1, 'BMP']:
        n += 1

    # Set To_Measure using .at for label-based setting
    allSegs.at[i, 'To_Measure'] = allSegs.at[i + n, 'EMP']

    # Set From_Measure for the next segment, if within bounds
    if (i + n + 1) < allSegs.shape[0]:
        allSegs.at[i + n + 1, 'From_Measure'] = allSegs.at[i + n + 1, 'BMP']

    i += (n + 1)


mergedSegs  = allSegs.copy()
mergedSegs  =mergedSegs[mergedSegs['From_Measure'] != '']
mergedSegs.drop(['MPT','BMP','EMP'], axis=1, inplace=True)

mergedSegs.to_excel('MergedSegs.xlsx',index=False)

import arcpy
arcpy.conversion.ExcelToTable('C:/Users/e131971/Desktop/Archive/2025/4_and_older/LRSProjects/SpeedSignstoEvent/V4-ThisOneForSure/MergedSegs.xlsx', 'C:/Users/e131971/Desktop/Archive/ArcPro/ArcPro.gdb/Test_Event')
#arcpy.lr.MakeRouteEventLayer(in_routes="ROUTE_STATUS",route_id_field="ROUTE_ID",in_table="Test_Event",in_event_properties="Route_ID; LINE; From_Measure; To_Measure",out_layer="Test_Event_layer",add_error_field="ERROR_FIELD")
