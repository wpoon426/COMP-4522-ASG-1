
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
    before_value = None
    new_value = None  # Initialize new_value to store the updated value
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
                                status = 'comitted'
                                log_entry = logUpdates(transactions[transID][0], before_value, new_value, status)  # Assuming the first element is transaction_id
                                log.append(log_entry)
                                break
    return before_value, new_value, log
        


def logUpdates(transaction_id,before,after,status):
    log_entry = [(transaction_id, before,after, status)]

    return log_entry


# Log for rollback (storing old values before modification)
def rollback(log, data):
    print("\n***Initiating rollback to before state***\n")

    for logs in reversed(log):

        if len(logs) != 4:  # Ensure that the transaction logs at the end is printing properly
            continue

        transaction_id, before_value, new_value, status = logs
        
       
        for item in data:
            if item['Unique_ID'] == transaction_id:  
                # This Reverts the change
                item['value_key'] = before_value  
                break

    return data 


def is_there_a_failure(count):

# Hardcoded failure for the second transaction
    if count == 1:  
        return True
    return False
    

def main():
    logs = []
    count = 0
    
   # number_of_transactions = len(transactions)
    must_recover = False
    data_base = read_file('Employees_DB_ADV.csv')
    failure = is_there_a_failure(count)
    failing_transaction_index = None
    # Process transaction
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
            print(f"There was a failure while processing Transaction #{failing_transaction_index}")
            data_base = rollback(log, data_base)
            break
                
        else:
            status = 'Complete'
            # All good, update Log Record & DB as required
            #logs.append(logUpdates(before,after,status,transaction_id))
            count = count + 1  

            #putting main memory db(python list) into secondary memory(new csv file) after changes
            
            with open ('testDB.csv', 'w', newline = '') as csvfile:
                header = ['Unique_ID', 'First_name', 'Last_name', 'Salary', 'Department', 'Civil_status']
                writer = csv.DictWriter(csvfile, fieldnames=header)
                body = data_base
                writer.writeheader()
                writer.writerows(body)
            
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




