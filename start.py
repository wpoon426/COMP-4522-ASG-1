
'''
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
              failing_transaction_index = index + 1
          else:
              # All good, update Log Record & DB as required
              print(f'Transaction No. {index+1} has been committed!
                      Changes are permanent.')
      print('The data entries AFTER all work is completed are presented below:')
      for item in data_base:
          print(item)
      # Print here the contents of your Log Record System, please.
 
    
    
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
    print(f"There are {size} records in the DB, including the header.\n")
    return data
'''


