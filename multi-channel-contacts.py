# Import dependencies
import json
import pandas as pd

# Read json dataset
with open('contacts.json') as f:
    data = json.loads(f.read())

df = pd.DataFrame(data)

# Search and create a result dataframe process
result = pd.DataFrame({}, columns = ['ticket_id', 'ticket_trace/contact'])

# Looping per row in dataset
for index, row in df.iterrows():
    # Check whether the ID has been found
    if (result.where(result['ticket_id'] == row['Id']).dropna().ticket_id.count() == 0):
        # Declare variabel
        ids = []
        conc = ''
        sum_of_contacts = 0
        
        # Labelling Null value
        row['Email'] = 'Null' if row['Email'] == '' else row['Email']
        row['Phone'] = 'Null' if row['Phone'] == '' else row['Phone']
        row['OrderId'] = 'Null' if row['OrderId'] == '' else row['OrderId']

        # Get not Null value columns from iterated row
        col_search = row[row != 'Null'].index.drop(['Id', 'Contacts'])
        
        # Get ID from iterated row
        ids = [row['Id']]
        
        # Search the matching column values from iterated row
        for x in col_search:
            id_res = df.where(df[x] == row[x]).dropna().Id.to_list()
            
            # Convert an ID type from float to int
            ids += list(map(int, id_res))

        # Listing the ID
        ids = list(set(ids))

        # Sorting the ID
        ids.sort()

        # Concatenate an ID
        for x in ids:
            if (ids.index(x) != len(ids)-1):
                conc = conc + str(x) + '-'
            else:
                conc = conc + str(x)
                
            # Enumerate the Contacts column value
            sum_of_contacts += df.loc[x].Contacts

        # Create a temp dataframe
        df_temp = pd.DataFrame({
            'ticket_id' : ids,
            'ticket_trace/contact' : [conc + ', ' + str(sum_of_contacts) for x in ids]
        }, index = ids)

        # Concatenate the temp dataframe by recent result dataframe
        result = pd.concat([result, df_temp])
        
        # Sorting the csv index 
        result.sort_index().to_csv(r'result.csv')
        
        # Drop the rows based by ID that has been found
        df.drop(result.index.to_list())
