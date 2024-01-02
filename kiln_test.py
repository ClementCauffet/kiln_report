"""
This program is a component of Kiln company's recruitment process. Its aim is to utilize Kiln's API to retrieve information regarding staking for one of its clients.

Author : Cauffet Clement
Date : 2023-12-28
Version : 2.0

Note : To use this program, you will require a Kiln API key and Google credentials associated with your Google Sheet. 
For further information, please refer to the Readme file in the following GitHub repository : https://github.com/ClementCauffet/kiln_report
"""

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests

# Authenticate Google Sheets
def authenticate_google_sheets():
    scope = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name('credencials.json', scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open('--YOUR GOOGLE SHEET NAME--')
    start_date = spreadsheet.worksheet('Overview').acell('E2').value
    end_date = spreadsheet.worksheet('Overview').acell('E3').value
    return spreadsheet.worksheet('--YOUR SOURCE FILE NAME--'), start_date, end_date

# Read Kiln API Token
def read_kiln_token():
    with open('kiln_token.txt', 'r') as file:
        return file.read().strip()
    
# API calls and Gsheet update
def kiln_api_calls(wsTest, bearer_token, api_url, call_type, start_date, end_date):   

    #Used to build routes depending on addresses in the source file
    blockchain_mapping = {
        'ethereum': 'eth/' + call_type + '?validators',
        'solana': 'sol/' + call_type + '?stake_accounts',
        'cosmos': 'atom/' + call_type + '?validators',
        'near': 'near/' + call_type + '?stake_accounts',
        'cardano': 'ada/' + call_type + '?stake_addresses',
        'polygon': 'matic/' + call_type + '?wallets',
        'kusama' : 'ksm'
    }
    
    # Grouping addresses based on their protocols
    grouped_addresses_by_protocol = {protocol: [] for protocol in blockchain_mapping.values()}

    for index, row in enumerate(wsTest.get_all_values()):
        if index == 0:  # Ignore the first row (headers)
            continue
        id_validator = row[1]
        address = row[2]

        protocol = blockchain_mapping.get(id_validator.split('_')[0].lower(), '')
        if not protocol:
            print(f"Invalid blockchain for ID: {id_validator}")            
            continue
        
        grouped_addresses_by_protocol[protocol].append(address)     

    # Object designed to store modifications intended for updating our Google Sheet
    updates = {}

    for protocol, addresses in grouped_addresses_by_protocol.items():        
        if addresses:
            if(call_type == 'stakes'): # Depending on the call_type
                grouped_addresses = [addresses[i:i + 80] for i in range(0, len(addresses), 80)] # Aggregating addresses into groups of up to 80 values to reduce the number of API calls
                for index, grouped_address in enumerate(grouped_addresses):    
                    if 'atom' in protocol: # Need to handle atom calls / responses separately                                
                        parts = grouped_address[index].split('_')
                        if len(parts) == 2:
                            validator = parts[0]
                            delegator = parts[1]                            
                            route = f'{api_url}/{protocol}={validator}&delegators={delegator}' # Building route for Atom addresses                  
                        else:
                            print(f"Invalid address format for Cosmos protocol: {address}")
                    else:                        
                        route = f'{api_url}/{protocol}={",".join(grouped_address)}' # Building route for other addresses            
                            
                    headers = {"Authorization": f"Bearer {bearer_token}"}
                    response = requests.get(route, headers=headers)   # Calling API

                    if response.status_code == 200:
                        data = response.json()
                        if 'data' in data and isinstance(data['data'], list) and data['data']:                    
                            for entry in data.get('data', []):
                                if 'atom' in protocol:  
                                    current_address = grouped_address[index] 
                                elif 'validator_address' in entry:
                                    current_address = entry['validator_address']
                                elif 'delegator_address' in entry:
                                    current_address = entry['delegator_address']
                                elif 'stake_account' in entry:
                                    current_address = entry['stake_account'] 
                                elif 'stake_address' in entry:
                                    current_address = entry['stake_address']                                                                  
                                else:
                                    print("Unknown address field in the response")                           
                                    continue  # Moving on

                                # Checking ig 'is_kiln' in the response and adapting answers
                                if 'is_kiln' in entry:
                                    is_kiln = str(entry.get('is_kiln', False)).capitalize()
                                else:
                                    is_kiln = 'defaultTrue'  # Ou toute autre valeur par défaut

                                # Updating balance
                                balance = entry.get('balance', '0')
                               

                                # Adjusting the 'updates' object for subsequent pushing
                                if current_address in updates:
                                    updates[current_address]['is_kiln'] = is_kiln
                                    updates[current_address]['balance'] = balance
                                else:
                                    
                                    updates[current_address] = {'is_kiln': is_kiln, 'balance': balance}                                                          
                        else:
                            print(f"No valid data found in the response or 'data' key is missing or empty for {protocol}")                      
                    else: #No status 200 -> problem while calling
                        print(f"Failed to fetch data for {protocol}")
                        is_kiln = 'noresponseTrue'
                        balance = '0'
                        for address in grouped_address:
                            current_address = address
                            if current_address in updates:
                                updates[current_address]['is_kiln'] = is_kiln
                                updates[current_address]['balance'] = balance
                            else:
                                updates[current_address] = {'is_kiln': is_kiln, 'balance': balance}
            else: # Calling for rewards, should be updating if calling other routes 
                for address in addresses:
                    if 'atom' in protocol:                                           
                        parts = address.split('_')
                        if len(parts) == 2:
                            validator = parts[0]
                            delegator = parts[1]                            
                            route = f'{api_url}/{protocol}={validator}&delegators={delegator}&start_date={start_date}&end_date={end_date}'                            
                            
                        else:
                            print(f"Invalid address format for Cosmos protocol: {address}")
                            route = f''
                        
                    else:                   
                        route = f'{api_url}/{protocol}={address}&start_date={start_date}&end_date={end_date}'                        
                    
                    headers = {"Authorization": f"Bearer {bearer_token}"}                            
                    response = requests.get(route, headers=headers)

                    rewards = 0

                    if response.status_code == 200:
                        data = response.json()
                        if 'data' in data and isinstance(data['data'], list) and data['data']:                    
                            for entry in data.get('data', []):                                    
                                    rewards += int(entry['rewards'])                              
                        else:
                            print(f"No valid data found in the response or 'data' key is missing or empty for {protocol}")                      
                    else:
                        print(f"Failed to fetch data for {protocol}")
                        rewards = -1
                    
                    if address in updates:
                        updates[address]['rewards'] = rewards
                    else:
                        updates[address] = {'rewards': rewards} 
        else:
            print(f'No addresses for this protocol : {protocol}')

    # Object designed to store modifications intended for updating our Google Sheet
    cell_list = []

    # Utilizing 'batch_updates' to push data into the Google Sheet file, mitigating the risk of exceeding the rate limit and increasing performances
    if(call_type == 'stakes'):

        for index, row in enumerate(wsTest.get_all_values()):
            if index == 0:  
                continue
            gsheet_address = row[2]  

            if gsheet_address in updates:
                row_number = index + 1  
                cell_list.append({
                    'range': f'D{row_number}',
                    'values': [[updates[gsheet_address]['is_kiln']]]
                })
                cell_list.append({
                    'range': f'H{row_number}',
                    'values': [[updates[gsheet_address]['balance']]]
                })

    else:
        for index, row in enumerate(wsTest.get_all_values()):
            if index == 0:  
                continue
            gsheet_address = row[2]  

            if gsheet_address in updates:
                row_number = index + 1  
                cell_list.append({
                    'range': f'E{row_number}',
                    'values': [[updates[gsheet_address]['rewards']]]
                })

    if cell_list:
        wsTest.batch_update(cell_list)


def manipulate_data(worksheet):

    #Adjusting the coefficient according to the decimal precision of each protocol
    all_rows = worksheet.get_all_values()

    cell_list = []

    for index, row in enumerate(all_rows[1:], start=1):
        target_value = row[1]       
        if 'cosmos' in target_value:
            cell_list.append({
                'range': f'F{index + 1}',
                'values': [[1e6]]
            })
        elif 'ethereum' in target_value:
            cell_list.append({
                'range': f'F{index + 1}',
                'values': [[1e18]]
            })
        elif 'polygon' in target_value:
            cell_list.append({
                'range': f'F{index + 1}',
                'values': [[1e18]]
            })
        elif 'solana' in target_value:
            cell_list.append({
                'range': f'F{index + 1}',
                'values': [[1e9]]
            })
        elif 'near' in target_value:
            cell_list.append({
                'range': f'F{index + 1}',
                'values': [[1e23]]
            })
        elif 'cardano' in target_value:
            cell_list.append({
                'range': f'F{index + 1}',
                'values': [[1e6]]
            })            
                     
    if cell_list:
        worksheet.batch_update(cell_list)

# Main function
def main():
    #Base route for API Calls
    api_url = 'https://api.kiln.fi/v1'
    
    #Initiating connection to Google Sheets.
    worksheet_test, start_date, end_date = authenticate_google_sheets()
    token = read_kiln_token()

    call_type = 'stakes'
    kiln_api_calls(worksheet_test, token, api_url, call_type,start_date, end_date)
    call_type = 'rewards'
    kiln_api_calls(worksheet_test, token, api_url, call_type,start_date, end_date)

    manipulate_data(worksheet_test)

# Run the main function
if __name__ == "__main__":
    main()
