from flask import Flask
from flask import render_template

import requests
import pandas as pd
import datetime
import json
import gspread
from gspread_dataframe import set_with_dataframe
import os

app = Flask(__name__)


@app.route("/")
def hello_world():
    headers={'Content-Type':'application/json'}
    hapikey = os.getenv('HAPIKEY')


    deals = []
    offset = 0
    has_more = True

    stime = "01/01/2021"
    date = round(datetime.datetime.strptime(stime, "%d/%m/%Y").timestamp() * 1000)


    # Build DF
    while has_more:
        search_payload = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "closedate",
                            "operator": "GTE",
                            "value": date
                        },
                        {
                            "propertyName": "deal_status",
                            "operator": "EQ",
                            "value": "Open"
                        }

                    ]
                }
            ],
            # "properties": ["associated_company_id"],
            "properties": ["closedate", "hs_object_id", "associated_company_id","dealname", "hubspot_owner_id", "deal_status", "time_until_closed", "pipeline", "dealstage", "time_in_stage", "amount", "hs_acv", "hs_mrr", "num_contacted_notes", "notes_last_contacted", "notes_last_updated", "notes_next_activity_date", "hs_manual_forecast_category", "hs_forecast_amount", "last_deal_stage_date", "deal_stage_valid"],
            "limit": 100,
            "after": offset
        }

        # Properties: Lead Source, hs_ideal_customer_profile, hs_total_deal_value, total_closed_deal_value, hs_analytics_source_data_1 (make sure about this, ugly campaign name), lifecyclestage

        url = f"https://api.hubapi.com/crm/v3/objects/deals/search?hapikey={hapikey}"

        r = requests.post(url=url, data=json.dumps(search_payload), headers=headers)
        res = json.loads(r.text)


        deals.extend(res['results'])

        offset += 100
        print(f'Fetching {offset} of {res["total"]}')

        if offset > int(res["total"]):
            break

    list_length = len(deals)
    print(f"You've succesfully parsed through {list_length} engagement records and added them to a list")



    df = pd.json_normalize(deals, max_level=1)


    # Convert Dates
    df['createdAt'] = pd.to_datetime(df['createdAt'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%d/%m/%Y')
    df['updatedAt'] = pd.to_datetime(df['updatedAt'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%d/%m/%Y')

    df['properties.createdate'] = pd.to_datetime(df['properties.createdate'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%d/%m/%Y')
    df['properties.closedate'] = pd.to_datetime(df['properties.closedate'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%d/%m/%Y')

    df['properties.hs_lastmodifieddate'] = pd.to_datetime(df['properties.hs_lastmodifieddate'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%d/%m/%Y')
    df['properties.last_deal_stage_date'] = pd.to_datetime(df['properties.last_deal_stage_date'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%d/%m/%Y')
    df['properties.notes_last_contacted'] = pd.to_datetime(df['properties.notes_last_contacted'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%d/%m/%Y')

    df['properties.notes_last_updated'] = pd.to_datetime(df['properties.notes_last_updated'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%d/%m/%Y')
    df['properties.notes_next_activity_date'] = pd.to_datetime(df['properties.notes_next_activity_date'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%d/%m/%Y')

    pd.to_numeric(df['properties.time_until_closed'])

    df['properties.time_until_closed'] = df['properties.time_until_closed'].astype(float)
    df['properties.time_until_closed'] = (df['properties.time_until_closed'] / (1000*60*60*24))

    # df['properties.time_in_stage'] = df['properties.time_in_stage'].astype(float)
    # df['properties.time_in_stage'] = (df['properties.time_in_stage'] / (1000*60*60*24))


    # DEALSTAGE | SANITISE
    df.loc[df['properties.dealstage'] == '2097135', ['properties.dealstage']] = 'Lead Qualification'
    df.loc[df['properties.dealstage'] == '2097136', ['properties.dealstage']] = 'Initial Discovery'
    df.loc[df['properties.dealstage'] == '2097137', ['properties.dealstage']] = 'Establish Value'
    df.loc[df['properties.dealstage'] == '2097138', ['properties.dealstage']] = 'Validate'
    df.loc[df['properties.dealstage'] == '2097139', ['properties.dealstage']] = 'Proof'
    df.loc[df['properties.dealstage'] == '2097149', ['properties.dealstage']] = 'Agreement'
    df.loc[df['properties.dealstage'] == '2097140', ['properties.dealstage']] = 'Closed Won'
    df.loc[df['properties.dealstage'] == '2097141', ['properties.dealstage']] = 'Closed Lost'
    df.loc[df['properties.dealstage'] == '2097150', ['properties.dealstage']] = 'Closed Abandoned'
    df.loc[df['properties.dealstage'] == 'f2247d97-e1bc-42d0-8def-44895bf11b3f', ['properties.dealstage']] = 'MQL (Partners)'
    df.loc[df['properties.dealstage'] == '424774', ['properties.dealstage']] = 'Lead (Partners)'
    df.loc[df['properties.dealstage'] == '2096561', ['properties.dealstage']] = 'Lead Qualification (Existing Customers: Renewals and Upsells)'
    df.loc[df['properties.dealstage'] == 'bae12787-d38f-43f5-a4e3-9a55018ac61b', ['properties.dealstage']] = 'Introduction Meeting (Partners)'
    df.loc[df['properties.dealstage'] == '83858ad5-7d45-462e-b497-e6df81750385', ['properties.dealstage']] = 'Negotiation (Partners)'
    df.loc[df['properties.dealstage'] == 'fceb9abc-6765-44c5-a344-7b6a31b7d64f', ['properties.dealstage']] = 'Demo Meeting (Partners)'
    df.loc[df['properties.dealstage'] == '8a264584-d97a-4a85-a307-7fd2b17aec67', ['properties.dealstage']] = 'Proof of Concept (Partners)'
    df.loc[df['properties.dealstage'] == 'e6a690d6-a029-4e3f-bae3-7fee86b5d474', ['properties.dealstage']] = 'SQL (Partners)'
    df.loc[df['properties.dealstage'] == '2096562', ['properties.dealstage']] = 'Initial Discovery (Existing Customers: Renewals and Upsells)'
    df.loc[df['properties.dealstage'] == '2096563', ['properties.dealstage']] = 'Establish Value (Existing Customers: Renewals and Upsells)'
    df.loc[df['properties.dealstage'] == '2097164', ['properties.dealstage']] = 'Lead Qualification (Existing Customers: New Business)'


    # PIPELINE | SANITISE
    # Partners - 28828c61-12ef-4f30-9739-ad17abe4a04d
    # New Customers: New Business - 2097134
    # Existing Customer: New Business - 2097163
    # Existing Customer: Renewals and Upsells - 2096560
    df.loc[df['properties.pipeline'] == '28828c61-12ef-4f30-9739-ad17abe4a04d', ['properties.pipeline']] = 'Partners'
    df.loc[df['properties.pipeline'] == '2097134', ['properties.pipeline']] = 'New Customers: New Business'
    df.loc[df['properties.pipeline'] == '2097163', ['properties.pipeline']] = 'Existing Customer: New Business'
    df.loc[df['properties.pipeline'] == '2096560', ['properties.pipeline']] = 'Existing Customer: Renewals and Upsells'

    # OWNERS | SANTISE
    df.loc[df['properties.hubspot_owner_id'] == '30531852', ['properties.hubspot_owner_id']] = 'Gabi Goldberg'
    df.loc[df['properties.hubspot_owner_id'] == '34083800', ['properties.hubspot_owner_id']] = 'Dave Blakey'
    df.loc[df['properties.hubspot_owner_id'] == '34371624', ['properties.hubspot_owner_id']] = 'Grant Duke'
    df.loc[df['properties.hubspot_owner_id'] == '34384577', ['properties.hubspot_owner_id']] = 'Tim Elston'
    df.loc[df['properties.hubspot_owner_id'] == '34386167', ['properties.hubspot_owner_id']] = 'Cole Bisset'
    df.loc[df['properties.hubspot_owner_id'] == '34386168', ['properties.hubspot_owner_id']] = 'Sarah Ashton'
    df.loc[df['properties.hubspot_owner_id'] == '34386171', ['properties.hubspot_owner_id']] = 'Jesse Pols'
    df.loc[df['properties.hubspot_owner_id'] == '34386172', ['properties.hubspot_owner_id']] = 'Gizelle Myburgh'
    df.loc[df['properties.hubspot_owner_id'] == '34893760', ['properties.hubspot_owner_id']] = 'Thia Heyns'
    df.loc[df['properties.hubspot_owner_id'] == '35315102', ['properties.hubspot_owner_id']] = 'Charles Jolaoso'
    df.loc[df['properties.hubspot_owner_id'] == '35521343', ['properties.hubspot_owner_id']] = 'Douglas Cherry'
    df.loc[df['properties.hubspot_owner_id'] == '36028018', ['properties.hubspot_owner_id']] = 'Iwan Price-Evans'
    df.loc[df['properties.hubspot_owner_id'] == '36249287', ['properties.hubspot_owner_id']] = 'Jonathan Brinson'
    df.loc[df['properties.hubspot_owner_id'] == '36550393', ['properties.hubspot_owner_id']] = 'Mareliza Viljoen'
    df.loc[df['properties.hubspot_owner_id'] == '40462238', ['properties.hubspot_owner_id']] = 'Aalia Manie'
    df.loc[df['properties.hubspot_owner_id'] == '42593488', ['properties.hubspot_owner_id']] = 'Sal Yousufi'
    df.loc[df['properties.hubspot_owner_id'] == '49464327', ['properties.hubspot_owner_id']] = 'Katinka Vreugdenhil'
    df.loc[df['properties.hubspot_owner_id'] == '52741477', ['properties.hubspot_owner_id']] = 'Conner Smith'
    df.loc[df['properties.hubspot_owner_id'] == '54023228', ['properties.hubspot_owner_id']] = 'Sebastian Jochelson'
    df.loc[df['properties.hubspot_owner_id'] == '49493772', ['properties.hubspot_owner_id']] = 'Lou Grande'



    # RENAME COLS
    df.columns = ['Deal Id','Create Date','Updated Date','Archived','Deal Amount', 'Company Id', 'Closed Date' ,'Create Date Prop', 'Deal Stage Valid', 'Deal Status', 'Deal Name', 'Deal Stage', 'Annual Contract Value', 'Forecasted Amount', 'Last Modified Date', 'Forecast Category', 'Monthly Recurring Revenue', 'Object ID', 'Deal Owner', 'Last Deal Stage Date', 'Last Contacted', 'Last Activity Date', 'Next Activity Date', 'Number Contacted', 'Pipeline','Time in Stage', 'Time Until Closed']



    gc = gspread.service_account(filename='creds.json')
    sh = gc.open('Deals').sheet1

    newdf = df[['Deal Name', 'Company Id', 'Deal Owner', 'Create Date', 'Closed Date', 'Deal Amount', 'Time Until Closed', 'Pipeline', 'Deal Stage','Time in Stage' ,'Deal Stage Valid', 'Annual Contract Value', 'Monthly Recurring Revenue', 'Number Contacted', 'Last Contacted', 'Last Activity Date', 'Next Activity Date', 'Forecast Category', 'Forecasted Amount']]
    newdf = newdf.sort_values(by = 'Deal Amount', ascending=False)

    sh.clear()
    set_with_dataframe(sh, newdf)
    print('Sheet Updated...')

    return 'Sheet Update...'
