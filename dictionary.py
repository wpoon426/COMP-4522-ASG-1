
import csv
import datetime

data_base = []

#transaction que
transactions = [
    (1, 'Department', 'Music'),  
    (5, 'Civil_status', 'Divorced'),  
    (15, 'Salary', '200000')
]


def read_file(file_name:str)->list:
    
    data = []
    #
    # one line at-a-time reading file
    #
    with open(file_name, 'r') as reader:
    # Read and print the entire file line by line
        line = reader.readline()
        while line != '':  # The EOF char is an empty string
            line = line.strip().split(',')
            data.append(line)
            # get the next line
            line = reader.readline()
    size = len(data)
    print('\nThe data entries BEFORE updates are presented below:\n')
    for item in data:
        print(item)
    print(f"\nThere are {size} records in the DB, including the header.\n")
    return data

# function to update rows. 
# currently hardcoded. MUST change to not be 
def update_db_row(data,transaction_id,attribute,new):

    attributemapping = {
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
            return row[attribute_index], before  # Return the new value and the old value
 


def logUpdates(before,after,status,transaction_id):
    log_entry = {
        'Transaction_ID': transaction_id,
        'Status': status,
        'Before': before,
        'After': after,
    }
    return log_entry
        

  

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





def is_there_a_failure(count):
    if count == 1:  # Hardcoded failure for the second transaction
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
            data_base = rollback(data_base, logs)
            break
                
        else:
            after, before = update_db_row(data_base,transaction_id,attribute,new)
            status = 'Complete'
            # All good, update Log Record & DB as required
            print("**************************************")
            logs.append(logUpdates(before,after,status,transaction_id))
            count = count + 1  

            #putting main memory db(python list) into secondary memory(new csv file) after changes
            with open ('testDB.csv', 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(data_base)
            print(f'Transaction No. {index+1} has been committed!Changes are permanent.')
            print('The data entries AFTER all work is completed are presented below:')
            
    print("\nFinal State of the Database:")
    for item in data_base:
        print(item)

    # Print here the contents of your Log Record System, please.
    print("\nTransaction Logs:")
    for log in logs:
        print(log)



main()




