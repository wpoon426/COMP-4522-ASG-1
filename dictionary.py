
import csv
import datetime

data_base = []

#transaction que
transactions = [
    {'ID': 1, 'Attribute': 'Department', 'New_value': 'Music'},
    {'ID': 5, 'Attribute': 'Civil_status', 'New_value': 'Divorced'},
    {'ID': 15, 'Attribute': 'Salary', 'New_value': 200000}
]
# function to update rows. Gonna try to make into dict first to acces values easier 
'''
def update_db_row(data,transactions):
    data_row = []
    for item in data:
        if(item[0] == transactions[0]['ID'])
            for i in item 
                if(i == transactions[0][''])

    return data_row
'''

# Log for rollback (storing old values before modification)

#def logData(data):



def read_file(file_name:str)->list:
    
    data = []
    Data_log = []
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
    print('The data entries BEFORE updates are presented below:')
    for item in data:
        print(item)
    print(f"\nThere are {size} records in the DB, including the header.\n")
    return data


def is_there_a_failure():
    
    ''''''


def main():
    number_of_transactions = len(transactions)
    must_recover = False
    data_base = read_file('Employees_DB_ADV.csv')
    
    failure = is_there_a_failure()
    failing_transaction_index = None
    # Process transaction
    
    for index in range(number_of_transactions):
        print(f"\nProcessing transaction No. {index+1}.")
        # In your assignment, a failure will occur
        # whilst processing Transaction No. 2
        failure = is_there_a_failure()
        if failure:
            # Do Recovery process as per the proper method
            must_recover = True
            failing_transaction_index = index + 1
            print(f'There was a failure while processing the transaction # {failing_transaction_index}')
            break
                
        else:
            # All good, update Log Record & DB as required

            #putting main memory db(python list) into secondary memory(new csv file) after changes
            with open ('testDB.csv', 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(data_base)
            print(f'Transaction No. {index+1} has been committed!Changes are permanent.')
            print('The data entries AFTER all work is completed are presented below:')

    for item in data_base:
        print(item)
    # Print here the contents of your Log Record System, please.

main()




