
import csv
import datetime

data_base = []

#transaction que
transactions = [
    { 1, 'Department',  'Music'},
    {5,  'Civil_status','Divorced'},
    { 15, 'Salary', '200000'}
]
# function to update rows. 
# currently hardcoded. MUST change to not be 
def update_db_row(data,transactions,count):
    if count == 0:
        for item in data[1:15]:
            if item[0] == '1':
                before = item[4]
                item[4] = 'Music'
                return (item[4],before)
    elif count == 1:
        for item in data[1:16]:
            if item[0] == '5':
                before = item[5]
                item[5] = 'Divorced'
                return (item[5],before)
    elif count == 2:
        for item in data[1:16]:
            if item[0] == '15':
                item[3] = '200000'
                before = item[3]
                return (item[3],before)
            
 
        

  

# Log for rollback (storing old values before modification)

#def logData(data):



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
    print('The data entries BEFORE updates are presented below:')
    for item in data:
        print(item)
    print(f"\nThere are {size} records in the DB, including the header.\n")
    return data
  

def logUpdates(before,after,status,count):  
    log = {'Log_ID': count, 'Before':before, 'After': after,'UpdateStatus': status ,'TimeStamp': datetime.time} 
    print(before,after) 

    return log




def is_there_a_failure():
    print()
    


def main():
    logs = []
    count = 0
    number_of_transactions = len(transactions)
    must_recover = False
    data_base = read_file('Employees_DB_ADV.csv')
    

    failure = is_there_a_failure()
    failing_transaction_index = None
    # Process transaction
    update_db_row(data_base,transactions,count)

    for index in range(number_of_transactions):
        print(f"\nProcessing transaction No. {index+1}.")
        # In your assignment, a failure will occur
        # whilst processing Transaction No. 2
        failure = is_there_a_failure()
        if failure:
            # Do Recovery process as per the proper method
            status = 'failure'
            must_recover = True
            failing_transaction_index = index + 1
            print(f'There was a failure while processing the transaction # {failing_transaction_index}')
            break
                
        else:
            status = 'Complete'
            # All good, update Log Record & DB as required
            after, before = update_db_row(data_base,transactions,count)
            print("**************************************")
            logs.append(logUpdates(before,after,status,count))
            count = count + 1  

            #putting main memory db(python list) into secondary memory(new csv file) after changes
            with open ('testDB.csv', 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(data_base)
            print(f'Transaction No. {index+1} has been committed!Changes are permanent.')
            print('The data entries AFTER all work is completed are presented below:')
            

    for item in data_base:
        print(item)

    # Print here the contents of your Log Record System, please.
    print(logs)
main()




