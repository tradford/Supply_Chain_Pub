from msTeam import msteam
from logMessage import logMessage as lm
import pandas as pd
import urllib
import pyodbc
from sqlalchemy import create_engine

'''TODO - Change the shipment API Call to not require the equipment ID and then compare the equipment ID's with the master list, if one equipment ID 
from the master list is not present, run another API call to get location data 
this might be the shipment call with the equipment id path"
or the public API call path"
to get all of the movements for the railcar the path parameter is the 
id given in the shipmets api call path"
'''

LOG = r"path"
site = "path"
df1 = pd.read_excel(r'path"
#df1 = pd.read_excel(r'path"

try:
    url = "path"

    payload='grant_type=client_credentials'
    headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Accept': 'application/json',
    'Authorization': ''
    }

    response = requests.post( url, headers=headers, data=payload)

    #print(response.text)
    token = response.json()

###### Step 3 Access site and copy RAIL TRANSPORT TASK data to a local folder
    today = dt.today()
    tody = today.strftime('%a, %d %b %Y %H:%M:%S')
    #print(tody)
    access = "Bearer " + token["access_token"]
    df1['Car Number equip'] = 'equipment_id=' + df1['Car Number'].astype(str)
    no_dup_json = []
    all_json = []    
    info_json = []
    
    for railcar in df1['Car Number']:
        
        #print(railcar)
        url = f"path"
        url2 = f"path"
        url1 = f"path"
        #speed up waybill by path"

        payload2={}
        headers2 = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Date': tody,
        'Authorization': access,
        'Cookie': ''
        }  
        #region for comment header 
        # headers3 = {
        # 'Content-Type': 'application/json',
        # 'Accept': 'application/json',
        # 'Date': tody,
        # 'Authorization': access,
        # 'Cookie': ''
        # }  
        #endregion
       
        response = requests.request("GET", url, headers=headers2, data=payload2)
        response1 = requests.request("GET", url1, headers=headers2, data=payload2)
        response2 = requests.request("GET", url2, headers=headers2, data=payload2)

        #print(response1.status_code)
        #print(response1.text)
        def check_data(data, file_j):
            if data.status_code == 200:
                json_data = data.json()
                if json_data:
                    #print(data.text)
        # Append the parsed JSON data to the all_json list
                    file_j.append(json_data)
            else:
                lm(LOG, f"Railcar {railcar} was not found with data")    
    
        check_data(response, all_json)
        check_data(response1, no_dup_json)
        check_data(response2, info_json)
      
except Exception as e:
    msteam(site, "RailCar Data is not found for Supply Chain Automation see file up_equipment.py: Error: " + str(e),"Trent Radford")
    lm(LOG, "RailCar Data is not found for Supply Chain Automation Error: " + str(e))
#
#print(all_json)  
#print(no_dup_json)  
## for entry in all_json:
##     i=0
##     print(i)
##     print('\n')
##     print(entry[0].get("load", {}))
##     i = i+1
##     print(entry[0].get("load", {}).get("commodities", [{}])[0].get("stcc", "N/A"))

try:
    loc = []
    loc_final = []
    for entry in all_json:
        origin = entry[0].get("route", {}).get("origin", {}).get("location", {}).get("id")
        destination = entry[0].get("route", {}).get("destination", {}).get("location", {}).get("id")
        last_stop = entry[0].get("route", {}).get("last_accomplished_event_stop", {}).get("location", {}).get("id")
        loc.append(origin)
        loc.append(destination)
        loc.append(last_stop)
    unique_loc = list(set(loc))
    print(unique_loc)  
    for loc in unique_loc:
        url_loc = f"path"
        response3 = requests.request("GET", url_loc, headers=headers2, data=payload2)
        check_data(response3, loc_final)
    print(loc_final)
    df5 = pd.DataFrame(loc_final)
#prefix = 'origin_'
#df_prefixed = df5.add_prefix(prefix)

    for col in df5.columns:
        df5[col + '_destination'] = df5[col]
    
    #print(df5)

except Exception as e:
    msteam(site, "RailCar Location Data is not found for Supply Chain UP Automation see file up_equipment.py: Error: " + str(e),"Trent Radford")
    lm(LOG, "RailCar Location Data is not found for Supply Chain UP Automation Error: " + str(e))
    
try:
    def flatten_json(json_data):
        flat_data = []

        for entry in json_data:
            load = entry[0].get("load", {})
            waybill = load.get("waybill", {})
            equipment = load.get("equipment", {})
            commodities = load.get("commodities", [{}])[0]

            route = entry[0].get("route", {})
            origin = route.get("origin", {}).get("location", {})
            destination = route.get("destination", {}).get("location", {})
            last_accomplished_event_stop = route.get("last_accomplished_event_stop", {}).get("location", {})

            current_event = entry[0].get("current_event", {})
            location = current_event.get("location", {})
            carrier_train = current_event.get("carrier_train", {})

            flat_entry = {
                "id": entry[0].get("id"),
                "waybill_id": waybill.get("id"),
                "equipment_id": equipment.get("id"),
                "commodity_stcc": commodities.get("stcc"),
                "commodity_description": commodities.get("description"),
                "load_empty_code": load.get("load_empty_code"),
                "yard_block": load.get("yard_block"),
                "origin_id": origin.get("id"),
                "origin_city": origin.get("city"),
                "origin_state": origin.get("state_abbreviation"),
                "origin_country": origin.get("country_abbreviation"),
                "destination_id": destination.get("id"),
                "destination_city": destination.get("city"),
                "destination_state": destination.get("state_abbreviation"),
                "destination_country": destination.get("country_abbreviation"),
                "destination_carrier": route.get("destination", {}).get("carrier"),
                "last_accomplished_event_stop_id": last_accomplished_event_stop.get("id"),
                "last_accomplished_event_stop_city": last_accomplished_event_stop.get("city"),
                "last_accomplished_event_stop_state": last_accomplished_event_stop.get("state_abbreviation"),
                "last_accomplished_event_stop_country": last_accomplished_event_stop.get("country_abbreviation"),
                "last_accomplished_event_stop_carrier": route.get("last_accomplished_event_stop", {}).get("carrier"),
                "eta": route.get("eta"),
                "eta_original": route.get("eta_original"),
                "event_code": current_event.get("event_code"),
                "type_code": current_event.get("type_code"),
                "status_code": current_event.get("status_code"),
                "date_time": current_event.get("date_time"),
                "location_id": location.get("id"),
                "carrier_abbreviation": current_event.get("carrier_abbreviation"),
                "carrier_train_symbol": carrier_train.get("symbol"),
                "carrier_train_start_date": carrier_train.get("start_date"),
                "offline": current_event.get("offline"),
                "phase_code": entry[0].get("phase_code")
            }
            flat_data.append(flat_entry)

        return flat_data

    flattened_data = flatten_json(all_json)

    unique_objects = {}
    for obj in no_dup_json:
        primary_reference_id = obj[0]["primary_reference_id"]
        if primary_reference_id not in unique_objects:
            unique_objects[primary_reference_id] = obj

    unique_json_data = list(unique_objects.values())

    # #for obj in unique_json_data:
    #     #print(obj)
        
    def flatten_json2(json_data):
        flat_data = []
        flat_data1 = []

        for entry in json_data:
            loads = entry[0]["loads"]
            parties = entry[0]["parties"]

            for party in parties:
                party_name = party.get("name") if party is not None else None
                party_type_code = party.get("type_code") if party is not None else None
                party_account = party.get("account") if party is not None else None
                party_account_id = party_account.get("id") if party_account is not None else None
                    
                for load in loads:
                    equipment_id = load.get("equipment", {}).get("id") if load is not None else None
                    load_empty_code = load.get("load_empty_code") if load is not None else None
                    waybill_status_code = load.get("waybill_status_code") if load is not None else None
                    commodities = load.get("commodities", []) if load is not None else None

                    for commodity in commodities:
                        stcc = commodity.get("stcc") if commodity is not None else None
                        description = commodity.get("description") if commodity is not None else None
                        quantity = commodity.get("quantity") if commodity is not None else None
                        unit_of_measure_code = commodity.get("unit_of_measure_code") if commodity is not None else None




                        ## flat_entry = {
                            #     "id": entry[0]["id"],
                            #     "primary_reference_id": entry[0].get("primary_reference_id"),
                            #     "primary_reference_id_type_code": entry[0].get("primary_reference_id_type_code"),
                            #     "waybill_number": entry[0].get("waybill_number"),
                            #     "waybill_date": entry[0].get("waybill_date"),
                            #     "equipment_id": equipment_id,
                            #     "stcc": stcc,
                            #     "description": description,
                            #     "quantity": quantity,
                            #     "unit_of_measure_code": unit_of_measure_code,
                            #     "load_empty_code": load_empty_code,
                            #     "waybill_status_code": waybill_status_code,
                            #     "party_name": party_name,
                            #     "party_type_code": party_type_code,
                            #     "party_account_id": party_account_id,
                            #     "origin_location_id": entry[0].get("route", {}).get("origin", {}).get("location", {}).get("id"),
                            #     "origin_carrier": entry[0].get("route", {}).get("origin", {}).get("carrier"),
                            #     "destination_location_id": entry[0].get("route", {}).get("destination", {}).get("location", {}).get("id")
                                
                            ## }
                            
                        flat_entry = {
                            "id": entry[0]["id"],
                            "primary_reference_id": entry[0].get("primary_reference_id"),
                            "primary_reference_id_type_code": entry[0].get("primary_reference_id_type_code"),
                            "waybill_number": entry[0].get("waybill_number"),
                            "waybill_date": entry[0].get("waybill_date"),
                            "equipment_id": equipment_id,
                            "stcc": stcc,
                            "description": description,
                            "quantity": quantity,
                            "unit_of_measure_code": unit_of_measure_code,
                            "load_empty_code": load_empty_code,
                            "waybill_status_code": waybill_status_code,
                            "origin_location_id": entry[0].get("route", {}).get("origin", {}).get("location", {}).get("id"),
                            "origin_carrier": entry[0].get("route", {}).get("origin", {}).get("carrier"),
                            "destination_location_id": entry[0].get("route", {}).get("destination", {}).get("location", {}).get("id")
                            
                        }
                        flat_data.append(flat_entry)
                        flat_entry1 = {
                            "id": entry[0]["id"],
                            "primary_reference_id": entry[0].get("primary_reference_id"),
                            "primary_reference_id_type_code": entry[0].get("primary_reference_id_type_code"),
                            "waybill_number": entry[0].get("waybill_number"),
                            "waybill_date": entry[0].get("waybill_date"),
                            "party_name": party_name,
                            "party_type_code": party_type_code,
                            "party_account_id": party_account_id,
                            "origin_location_id": entry[0].get("route", {}).get("origin", {}).get("location", {}).get("id"),
                            "origin_carrier": entry[0].get("route", {}).get("origin", {}).get("carrier"),
                            "destination_location_id": entry[0].get("route", {}).get("destination", {}).get("location", {}).get("id")
                            
                        }
                    
                    flat_data1.append(flat_entry1)

        return flat_data, flat_data1


    flattened_data_equip, flattened_data_party = flatten_json2(no_dup_json)
    # # print(flattened_data2)
    
except Exception as e:
    msteam(site, "Cannot flatten RailCar Data for Supply Chain UP Automation see file up_equipment.py: Error: " + str(e),"Trent Radford")
    lm(LOG, "Cannot flatten RailCar Data for Supply Chain UP Automation Error: " + str(e)) 

    
# # # #print(flattened_data)
try:
    conn_str = "DRIVER={SQL Server};" + "SERVER=;DATABASE="
    quoted = urllib.parse.quote_plus(conn_str)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={quoted}")

    df = pd.json_normalize(flattened_data)
    df_no_dup = pd.json_normalize(flattened_data_equip)
    df_no_dup1 = pd.json_normalize(flattened_data_party)
    df_loc = pd.json_normalize(df5)
    newdf = df.astype(str)
    newdf_no_dup = df_no_dup.astype(str)
    newdf_no_dup1 = df_no_dup1.astype(str)
    newdf_loc = df5.astype(str)
    print(newdf.head(20))
    df_car = pd.json_normalize(info_json)
    newdf_car = df_car.astype(str)
    newdf.to_sql("RailCarCurrent", con=engine, if_exists="replace", index=False)
    newdf_no_dup.to_sql("RailCarReferenceEquipment", con=engine, if_exists="replace", index=False)
    newdf_no_dup1.to_sql("RailCarReferenceParty", con=engine, if_exists="replace", index=False)
    newdf_car.to_sql("RailCarInfo", con=engine, if_exists="replace", index=False)
    newdf_loc.to_sql("RailCarLocations", con=engine, if_exists="replace", index=False)

except Exception as e:
    msteam(site, "Cannot load RailCar Data to MS SQL for Supply Chain UP Automation see file up_equipment.py: Error: " + str(e),"Trent Radford")
    lm(LOG, "Cannot load RailCar Data to MS SQL for Supply Chain UP Automation Error: " + str(e))