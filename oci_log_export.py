import pandas as pd
import csv
import os
import datetime
from datetime import timedelta,date
import subprocess
import json
import sys
import getopt

def myfunc(argv):

    START_DATE = ""
    END_DATE = ""
    COMPARTMENT_ID = ""
    arg_help = "\nUsage:\n\npython3 oci_log_export.py -s <log_start_date in YYYY-MM-DD Format> -e <log_end_date in YYYY-MM-DD Format> -c <Compartment_ID>\n".format(argv[0])
    try:
        opts, args = getopt.getopt(argv[1:],"hs:e:c:")
    except:
        print("\nenter valid argument values. Try -h option with script to know the usage.\n")
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-h"):
            print(arg_help)  # print the help message
            sys.exit(2)
        elif opt in ("-s"):
            START_DATE = arg
        elif opt in ("-e"):
            END_DATE = arg
        elif opt in ("-c"):
            COMPARTMENT_ID = arg

    INC = -1
    year1, month1, day1 = map(int, START_DATE.split('-'))
    DATE1 = datetime.date(year1, month1, day1)
    year2, month2, day2 = map(int, END_DATE.split('-'))
    DATE2 = datetime.date(year2, month2, day2)
    NUM_DAYS = DATE2 - DATE1
    NUM_DAYS = NUM_DAYS.days

# Function for converting JSON to .CSV format

    def convert_csv():

        with open('out.json', 'r') as file:
            data = json.load(file)
        datetimez = []
        compartmentID = []
        compartmentName = []
        message = []
        tenantId = []
        userAgent = []
        path = []
        ingestedtime = []
        principalName= []
        type = []
        id = []
        for ele in data['data']['results']:
            datetimez.append(ele['data']['datetime'])
            compartmentID.append(ele['data']['logContent']['data']['compartmentId'])
            compartmentName.append(ele['data']['logContent']['data']['compartmentName'])
            message.append(ele['data']['logContent']['data']['message'])
            tenantId.append(ele['data']['logContent']['data']['identity']['tenantId'])
            userAgent.append(ele['data']['logContent']['data']['identity']['userAgent'])
            path.append(ele['data']['logContent']['data']['request']['path'])
            ingestedtime.append(ele['data']['logContent']['oracle']['ingestedtime'])
            principalName.append(ele['data']['logContent']['data']['identity']['principalName'])
            type.append(ele['data']['logContent']['type'])
            id.append(ele['data']['logContent']['id'])
        finaldate = []
        for ts in datetimez:
            finaldate.append(datetime.datetime.fromtimestamp(int(ts) / 1000).strftime('%Y-%m-%d %H:%M:%S'))

        output = zip(finaldate, compartmentID, compartmentName, message, tenantId, userAgent, path, ingestedtime,principalName, type, id)
        output = list(output)
        df = pd.DataFrame(output)
        df.to_csv('/tmp/out.csv', header=False , mode='a',index=False)
        return None

# Check and validate the .CSV file in the /tmp directory

    os.system("touch /tmp/out.csv" )
    os.remove("/tmp/out.csv")
    header=['Date-Time', 'CompartmentID', 'CompartmentName', 'Message', 'TenantId', 'UserAgent', 'Path', 'Ingested-Time', 'principalName' , 'Type', 'ID']
    data = []
    with open('/tmp/out.csv', 'a') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)

# Block for saving Audit Logs in JSON format to out.json file    

    for i in range(INC, NUM_DAYS):
        print("\ncollecting logs for", DATE1)
        p = subprocess.Popen(''' oci logging-search search-logs --search-query 'search "''' + str(COMPARTMENT_ID) + '''/_Audit" | sort by datetime desc' --time-start ''' + str(DATE1) + '''"T00:00:00Z" --time-end ''' + str(DATE1) + '''"T23:59:00Z" > out.json ''', shell=True, stdout=subprocess.PIPE)
        (output, err) = p.communicate()
        convert_csv()
        PAG = subprocess.check_output(''' oci logging-search search-logs --search-query 'search "''' + str(COMPARTMENT_ID) + '''/_Audit" | sort by datetime desc' --time-start ''' + str(DATE1) + '''"T00:00:00Z" --time-end ''' + str(DATE1) + '''"T23:59:00Z" | grep "opc-next-page" | awk -F":" '{print $2}' | tr -d '"' | tr -d " " | tail -1 ''', shell=True).strip().decode('ascii')

        while (PAG != ""):
            p = subprocess.Popen(''' oci logging-search search-logs --search-query 'search "''' + str(COMPARTMENT_ID) + '''/_Audit" | sort by datetime desc' --time-start ''' + str(DATE1) + '''"T00:00:00Z" --time-end ''' + str(DATE1) + '''"T23:59:00Z" --page ''' + str(PAG) + '''  > out.json ''', shell=True, stdout=subprocess.PIPE)
            (output, err) = p.communicate()
            convert_csv()
            print("1:",PAG)
            PAG = subprocess.check_output(''' oci logging-search search-logs --search-query 'search "''' + str(COMPARTMENT_ID) + '''/_Audit" | sort by datetime desc' --time-start ''' + str(DATE1) + '''"T00:00:00Z" --time-end ''' + str(DATE1) + '''"T23:59:00Z" --page ''' + str(PAG) + ''' | grep "opc-next-page" | awk -F":" '{print $2}' | tr -d '"' | tr -d " " | tail -1 ''', shell=True).strip().decode('ascii')
            print("2:",PAG)
            if PAG in ["","["]:
                break
        print("successfully collected logs for", DATE1)
        DATE1 += timedelta(days=1)
        i = i + 1
    os.system("cp /tmp/out.csv /tmp/"+ str(COMPARTMENT_ID) +"_auditlog.csv" )
    print("\nThe .csv file is saved in location /tmp/out.csv")

if __name__ == "__main__":
    myfunc(sys.argv)

