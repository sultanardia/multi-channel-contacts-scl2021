#!/usr/bin/env python
# coding: utf-8

# In[60]:


# Import dependencies
import json
import pandas as pd


# In[3]:


# Read json dataset
with open('contacts.json') as f:
    data = json.loads(f.read())

df = pd.DataFrame(data)


# In[130]:


# Search and create a result dataframe process
result = pd.DataFrame({}, columns = ['ticket_id', 'ticket_trace/contact'])

for index, row in df.iterrows():
    if (result.where(result['ticket_id'] == row['Id']).dropna().ticket_id.count() == 0):
        iters += 1
        ids = []
        conc = ''
        sum_of_contacts = 0

        row['Email'] = 'Null' if row['Email'] == '' else row['Email']
        row['Phone'] = 'Null' if row['Phone'] == '' else row['Phone']
        row['OrderId'] = 'Null' if row['OrderId'] == '' else row['OrderId']

        col_search = row[row != 'Null'].index.drop(['Id', 'Contacts'])

        ids = [row['Id']]
        
        for x in col_search:
            id_res = df.where(df[x] == row[x]).dropna().Id.to_list()

            ids += list(map(int, id_res))

        ids = list(set(ids))

        ids.sort()

        for x in ids:
            if (ids.index(x) != len(ids)-1):
                conc = conc + str(x) + '-'
            else:
                conc = conc + str(x)

            sum_of_contacts += df.loc[x].Contacts

        df_temp = pd.DataFrame({
            'ticket_id' : ids,
            'ticket_trace/contact' : [conc + ', ' + str(sum_of_contacts) for x in ids]
        }, index = ids)

        result = pd.concat([result, df_temp])
        result.sort_index().to_csv(r'result.csv')
        pd.DataFrame({iters}).to_csv(r'iters.csv')
        
        df.drop(result.index.to_list())


# In[ ]:


iters

