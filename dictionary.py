
import csv
import datetime

data_base = []

#transaction que
transactions = [
    (1, 'Department', 'Music'),  
    (5, 'Civil_status', 'Divorced'),  
    (15, 'Salary', '200000')
]
log = [] # array for logging transactions
columns = [] #array for where columns are edited on csv

#Function to read csv file info
def read_file(file_name:str)->dict:
    
    data = []

    # one line at-a-time reading file
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

# function to update data based on transactions. 
def update_db_row(data,transactions,log):
    before_value = None
    new_value = None  # Initialize new_value to store the updated value
    #loop thru main memory
    for items in data:
        status = 'incomplete' # set transaction status to incomplete
        for items2 in transactions:
            for i in items2:
                if(i == int(items['Unique_ID'])):# check if row logged transaction id lines up with the looped through row of main memory
                    transID = transactions.index(items2) # get the id of transaction
                    for i in items2:#loop through items in diction
                        for keys in items.keys():#loop through items in dictionary aka main memory
                            if (i == keys):# if value in log equal one of the keys in dictonary (column name) change the value per transaction
                                before_value = items[keys] 
                                items[keys] = transactions[transID][items2.index(i)+1]
                                columns.append(transactions[transID][items2.index(i)])
                                column = transactions[transID][items2.index(i)] # save columns where changes were made 
                                new_value = items[keys]
                                status = 'comitted' # set status to commited 
                                log_entry = logUpdates(transactions[transID][0],column,before_value, new_value, status)# log the transaction
                                log.append(log_entry) # add to array
                                break
    return before_value, new_value, log, columns
        

#function to create format for array of logs
def logUpdates(transaction_id,column,before,after,status):
     return [(transaction_id,column,before,after, status)]



# Log for rollback (storing old values before modification)
def rollback(transaction_id,log, data, column):
    print("\n***Initiating rollback to before state***\n")
    for item in data:# loop thru data of main memory
        if item['Unique_ID'] == str(transaction_id):
            for l in log:
                for item2 in l:
                    for i in item2:
                        if(i == transaction_id):
                            before = item2[2]
                            new_status = 'rolled-back'
                            # change status to rolled back. 
                                # item2[4] = new_status
                            print(item2[4])
                            # loop to go thru chnaged columns and match to the current failed transaction and changed
                            for c in columns:
                                if c == item2[1]:
                                    item[c] = before
                                    return data  
                            
    return data 

# Function for hardcoded failure for the second transaction
def is_there_a_failure(count):
    if count == 1:  
        return True
    return False
    
# main function 
def main():
    count = 0    
    must_recover = False
    data_base = read_file('Employees_DB_ADV.csv')
    failure = is_there_a_failure(count)
    failing_transaction_index = None
    # Process transaction
    (before_value, after_value, logs, column) = update_db_row(data_base,transactions, log)
    for index, transaction in enumerate(transactions):
        transaction_id,attribute,new = transaction
        print(f"\nProcessing transaction No. {index+1}: {transaction}.")
        failure = is_there_a_failure(count)
        if failure:
            # Do Recovery process as per the proper method
            status = 'failure'
            must_recover = True
            failing_transaction_index = index + 1
            print(f"There was a failure while processing Transaction #{failing_transaction_index}")
            data_base = rollback(transaction_id,log, data_base, column)
            count = count + 1
                
        else:
            status = 'Complete'
            count = count + 1  
            #putting main memory db(python dictionary) into secondary memory(new csv file) after changes
            with open ('testDB.csv', 'w', newline = '') as csvfile:
                header = ['Unique_ID', 'First_name', 'Last_name', 'Salary', 'Department', 'Civil_status']
                writer = csv.DictWriter(csvfile, fieldnames=header)
                body = data_base
                writer.writeheader()
                writer.writerows(body)
            
            print(f'Transaction No. {index+1} has been committed!Changes are permanent.')
            print('The data entries AFTER all work is completed are presented below:')
            
    print("\nFinal State of the Database:")
    for item in data_base:
        print(item)

    # Print here the contents of your Log Record System, please.
    print("\nTransaction Logs:")
    for l in log:
       print(l)


main()




