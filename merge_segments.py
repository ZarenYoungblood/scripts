def merge_segments(allSegs,RIDBMPEMPlist, merge_fields):
    """
    Merges contiguous segments in the DataFrame based on specified carry fields.
    
    Parameters:
    allSegs (DataFrame): The DataFrame containing segmented data.
    RIDBMPEMPlist (list): a list with the column names for the RouteID, BMP, and EMP fields in that order
    merge_fields (list or string): List of column name(s) to check for continuity.
    
    Returns:
    DataFrame: A DataFrame with merged segments.
    """
    if not isinstance(RIDBMPEMPlist,list):
        print("RIDBMPEMPlist must be a list ex. ['RouteID','BMP','EMP']")
        return None
    print("Starting merge, this may take a few minutes...")

    
    RouteID = RIDBMPEMPlist[0]
    BMP = RIDBMPEMPlist[1]
    EMP = RIDBMPEMPlist[2]
    b = 'From_Measure'
    e = 'To_Measure'
    if BMP == b:
        b = 'From_Measure_New'
    if EMP == e:
        e = 'To_Measure_New'
    allSegs[b] = ''
    allSegs[e] = ''
    if isinstance(merge_fields,str):
        cf = [field.strip() for field in merge_fields.split(',')]
        merge_fields = cf
    
    i = 0
    while i < allSegs.shape[0]:
        if i == 0:
            allSegs.at[0, b] = allSegs.at[0, BMP]

        n = 0 #number of rows that make up a contiguous event
        # Make sure we don't go out of bounds
        while (i + n + 1) < allSegs.shape[0] and \
                allSegs.at[i + n, RouteID] == allSegs.at[i + n + 1, RouteID] and \
                all(
                    allSegs.at[i + n, field] == allSegs.at[i + n + 1, field]
                    for field in merge_fields
                    ) and \
                allSegs.at[i + n, EMP] == allSegs.at[i + n + 1, BMP]:
            n += 1

        # Set To-Measure using .at for label-based setting
        allSegs.at[i, e] = allSegs.at[i + n, EMP]

        # Set From-Measure for the next segment, if within bounds
        if (i + n + 1) < allSegs.shape[0]:
            allSegs.at[i + n + 1, b] = allSegs.at[i + n + 1, BMP]

        i += (n + 1)
        print(f"{i+n}/{allSegs.shape[0]} : {round((i+n)/allSegs.shape[0]*100,2)}%", end='\r')


    mergedSegs  = allSegs.copy()
    mergedSegs  = mergedSegs[mergedSegs[b] != '']

    mergedSegs.drop(columns=[BMP,EMP], inplace=True)
    return mergedSegs
