
import csv
import datetime

data_base = []

#transaction que
transactions = [
    (1, 'Department', 'Music'),  
    (5, 'Civil_status', 'Divorced'),  
    (15, 'Salary', '200000')
]
log = []


def read_file(file_name:str)->dict:
    
    data = []
    #
    # one line at-a-time reading file
    #
    with open(file_name, 'r') as file:
    # Read and print the entire file line by line
        read_csv = csv.DictReader(file)
        data = [row for row in read_csv]
        
    size = len(data)
    print('\nThe data entries BEFORE updates are presented below:\n')
    for item in data:
        print(item)
    print(f"\nThere are {size} records in the DB, including the header.\n")
    return data

# function to update rows. 
def update_db_row(data,transactions,log):
    ''' attributemapping = {
        'Salary': 3,
        'Department': 4,
        'Civil_status': 5
    }
    attribute_index = attributemapping.get(attribute)
  
    
    if attribute_index is None:
        raise ValueError(f"Invalid attribute '{attribute}' for update.")
    

    # Find the row to update by transaction_id
    for row in data[1:]:  # Skip the header row
        if row[0] == str(transaction_id):
            before = row[attribute_index]  # Get the old value
            row[attribute_index] = new  # Update the attribute with the new value
            return row[attribute_index], before  # Return the new value and the old value '''
    #loop thru 
    for items in data:
        status = 'incomplete'
        for items2 in transactions:
            for i in items2:
                if(i == int(items['Unique_ID'])):
                    transID = transactions.index(items2)
                    for i in items2:
                        for keys in items.keys():
                            if (i == keys):
                                before_value = items[keys] 
                                items[keys] = transactions[transID][items2.index(i)+1]
                                new_value = items[keys]
                                status = 'committed'
                                log.append(logUpdates(len(log)+1,before_value,new_value,status))
               
    return before_value, new_value, log
        


def logUpdates(transaction_id,before,after,status):
    log_entry = [(transaction_id, before,after, status)]

    return log_entry
    

'''
# Log for rollback (storing old values before modification)
def rollback(data, logs):
    print("\n***Initiating rollback to before state***\n")
    for log in logs: 
        if log['Status'] == 'Complete':
            transaction_id = log['Transaction_ID']
            before_value = log['Before']  # This is the 'old' value (before the update
            # Find the transaction row and revert the changes using the 'Before' value
            for transaction in transactions:
                if transaction[0] == transaction_id:
                    attribute = transaction[1]  # Find the correct attribute for the transaction
                    update_db_row(data, transaction_id, attribute, before_value)  # Revert to old value
                    break  # Exit the loop once the transaction is found
    return data
'''

def dict_toList(data_dict):
    body = []
    header = []
    for item in data_dict:
        empty = []
        for i in item.values():
            body.append(i)
    for key in item.keys():
        header.append(key)

    return (header, body)


def is_there_a_failure(count):
    emptuy =[]
    #if count == 1:  # Hardcoded failure for the second transaction
       # return True
   # return False
    

def main():
    logs = []
    count = 0
    
   # number_of_transactions = len(transactions)
    must_recover = False
    data_base = read_file('Employees_DB_ADV.csv')
    
 
    failure = is_there_a_failure(count)
    failing_transaction_index = None
    # Process transaction
    #dict_toList(data_base)
    #update_db_row(data_base,transactions)
    (before_value, after_value, logs) = update_db_row(data_base,transactions, log)

    for index, transaction in enumerate(transactions):
        transaction_id,attribute,new = transaction
        print(f"\nProcessing transaction No. {index+1}: {transaction}.")
        # In your assignment, a failure will occur
        # whilst processing Transaction No. 2
        failure = is_there_a_failure(count)
        if failure:
            # Do Recovery process as per the proper method
            status = 'failure'
            must_recover = True
            failing_transaction_index = index + 1
            #print(f"There was a failure while processing Transaction #{failing_transaction_index}")
            #data_base = rollback(data_base, logs)
            break
                
        else:
            status = 'Complete'
            # All good, update Log Record & DB as required
            #logs.append(logUpdates(before,after,status,transaction_id))
            count = count + 1  

            #putting main memory db(python list) into secondary memory(new csv file) after changes
            
            with open ('testDB.csv', 'w', newline = '') as csvfile:
                writer = csv.writer(csvfile)
                header, body = dict_toList(data_base)
                writer.writerow(header)
                writer.writerow(body)
            
            #print(f'Transaction No. {index+1} has been committed!Changes are permanent.')
            #print('The data entries AFTER all work is completed are presented below:')
            
    print("\nFinal State of the Database:")
    for item in data_base:
        print(item)

    # Print here the contents of your Log Record System, please.
    print("\nTransaction Logs:")
    for l in log:
       print(l)



main()




