import csv
import mysql.connector
from mysql.connector import Error
import datetime
import os
import time
from shutil import copyfile


'''

Flow
======
    1. Search new .csv file in specific folder according to input in src_path variable.
    2. Copy new files (less than 7 days) from source folder.
    3. If file was copied delete old records from DB.
    4. Insert new records from the new file.
    5. Delete files older than 7 days.
    6. Writing to log located inside _logs\ folder


Tasks
======
    1. NICE TO HAVE -> Check if date already exist for each file.
    2. LogHandler is checking every time we are writing to file if he need to open new file --> need to change.
    3. To Check First that DB is accessible
    4. Sum Changes into Text File per folder
    5. Represent the Initial configurations to external configuration file.
    
    
Tests
======
    1. Checking File Deletion.
    
'''


def LogHandling(stringtowrite, messagesevirity):
    logtime = time.strftime("%d-%m-%y")
    logdate = time.strftime("%H:%M:%S")
    current_time = time.time()
    file_was_created = 0
    found_file = 0
    logfolder = '_logs\\'

    def CreateNewFile():
        global logfile
        filename = "CSV_Parsing_Log_" + logtime + '.txt'
        filetoopen = logfolder + filename
        logfile = open(filetoopen, mode='w')
        print('I will Create New File {}'.format(filename))

    # Checking If We Need A New File
    if len(os.listdir(logfolder)) == 0:  # Check If Folder Is Empty
        CreateNewFile()
        file_was_created = 1
    else:
        for f in os.listdir(logfolder):
            filename, file_extension = os.path.splitext(logfolder + f)
            print 'Now Checking The File:  ' + f
            creation_time = os.path.getctime(logfolder + f)
            print 'Days This File Open:  ' + str((current_time - creation_time) // (24 * 3600))

            if (current_time - creation_time) // (24 * 3600) >= 7 and file_extension == '.txt':
                found_file = 1

            filetoopen = f  # To Save The Last File

        if found_file == 0:
            CreateNewFile()
            file_was_created = 1



    if file_was_created == 1:
        logfile.write(logdate + ' - ' + logtime + stringtowrite + "\n")  # Write To New File
    else:
        print '\n\nWriting to existing file:  ' + filetoopen
        logfile = open(logfolder + filetoopen, 'a')
        logfile.write(logdate + ' - ' + logtime + '    ' + messagesevirity + '    ' + stringtowrite + "\n")  #  Append Text To File



    logfile.close()


# Initial Configurations
# -------------------------------
# path_src = From where you want to copy.
# path_dst = Where you want to copy.
# folders = list of internal folders and the matching subfolder.
# dbIp = Enter DB IP\hostname
# schemeName = to which scheme the data will be entered.
path_src = "P:\"
path_dst = "C:\"
folders = ['folder1\\', 'folder2\\', 'folder3\\', 'folder4\\']
dbIP = '127.0.0.1'
schemeName = 'DBname'

LogHandling('Starting New Import', 'INFO')
LogHandling('-------------------------------------', 'INFO')
for subfolder in folders:
    LogHandling('New Checking ' + str(subfolder[0:len(subfolder) - 1]) + ' Folder', 'INFO')
    copy_flag = 0
    path_src = path_src + subfolder
    path_dst = path_dst + subfolder

    current_time = time.time()

    # Copy New Files.
    for f in os.listdir(path_src):
        creation_time = os.path.getctime(path_src + f)  # GET CREATION TIME FUNCTION.
        filename, file_extension = os.path.splitext(path_src + f)

        # Printing to log which file we are handling now.
        LogHandling('File exist. Filename:  ' + str(f), 'INFO')
        LogHandling('Amount of days that this file is open:  ' + str((current_time - creation_time) // (24 * 3600)), 'INFO')

        # Check if older than 7 days and is .csv file and is not exist in the folder.
        if (current_time - creation_time) // (24 * 3600) <= 7 and file_extension == '.csv' and os.path.isfile(path_dst+f) == 0:
            copy_flag = 1
            LogHandling('I found the filename:  ' + str(f) + ' suitable - Copying it now', 'INFO')
            copyfile(path_src+f, path_dst+f)

            # PREPARE DB BY DELETING OLD RECORDS.
            # THE DELETION OF OLD RECORDS IS DONE BECAUSE THE NEW FILE ALREADY CONTAIN OLD RECORDS. THEREFORE, WE WILL
            # DELETE OLD RECORDS.
            if copy_flag == 1:
                # IF FILE EXIST WE WILL DELETE OLD RECORDS FOR THAT subfolder.
                LogHandling('Starting the deletion of ' + subfolder[0:len(subfolder) - 1] + ' part In DB', 'INFO')
                delete_command = "DELETE FROM grafana.live_reports WHERE partner='" + subfolder[0:len(subfolder) - 1]+"'"
                LogHandling('Deleting data From DB according to the following command:  ' + delete_command, 'WARN')

                # CREATE DB CONNECTION -> EXECUTE QUERY -> CLOSE CONNECTION
                dbcon = mysql.connector.connect(host=dbIp, database=schemeName, user='root', password='123456')
                if dbcon:
                    cursor = dbcon.cursor()
                    cursor.execute(delete_command)
                    dbcon.commit()
                    cursor.close()
                    dbcon.close()

                # DONE DELETION --- DB IS READY
                LogHandling('DB IS READY', 'INFO')
            # RENDER THE CSV FILE TO VARIABLE
            csv_data = csv.reader(file(path_dst + f))

            try:
                dbcon = mysql.connector.connect(host=dbIp, database=schemeName, user='root', password='123456')
                LogHandling('Reconnection to DB established', 'INFO')

                # NEED TO GET LAST ID RECORD
                if dbcon:
                    # MAKING QUERY TO DATABASE
                    cursor = dbcon.cursor()

                    i = 0

                    # CHECK LAST ID IN DB
                    query_last = "SELECT id FROM live_reports ORDER BY id DESC LIMIT 1;"
                    cursor.execute(query_last)
                    lastid = cursor.fetchall()[0][0]

                cursor.close()
                dbcon.close()

                # OPEN NEW CONNECTION
                dbcon = mysql.connector.connect(host=dbIp, database=schemeName, user='root', password='123456')

                # INSERT NEW ROWS PROCESS
                if dbcon:
                    cursor = dbcon.cursor()
                    # LOOP ON EACH OF THE LINES IN THE CSV FILE AND INSERT THEM ONE BY ONE.
                    for row in csv_data:
                        if i != 0:
                            print row

                            for j in range(0, 14):
                                if row[j] == '':
                                    row[j] = 0

                            LogHandling('NOW INSERTING RECORD WITH ID: ' + str(lastid + i), 'WARN')
                            queryadd = "INSERT INTO grafana.live_reports(`id`,`date`,`total_users`,`total_sensors`,`7days_avg`,`dif_sensors`,`users_hypno_no`,`sessions_hypno_no`,`avg_apnea_not`,`avg_hr_not`,`avg_rr_not`,`premium_users`,`followed_users`,`avg_hr_alert`,`avg_rr_alert`,`avg_bex_alert`,`partner`) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                            args = (lastid + i, datetime.date(int(row[0][13:17]), int(row[0][18:20]), int(row[0][21:23])),
                                    int(row[1]), int(row[2]), round(float(row[3]), 2),
                                    int(row[4]), int(row[5]), int(row[6]), round(float(row[7]), 2), round(float(row[8]), 2),
                                    round(float(row[9]), 2), int(row[10]), int(row[11]), round(float(row[12]), 2),
                                    round(float(row[13]), 2), round(float(row[14]), 2), subfolder[0:len(subfolder) - 1])
                            cursor.execute(queryadd, (args))
                            dbcon.commit()
                        i += 1

            except mysql.connector.Error as e:
                print e

            else:
                cursor.close()
                dbcon.close()

            cursor.close()
            dbcon.close()

            LogHandling("Done With " + subfolder[0:len(subfolder) - 1], 'INFO')
            LogHandling('-------------------------------------', 'INFO')

    # Delete Old Files
    LogHandling('Starting Files Deletion', 'WARN')
    LogHandling('-------------------------------------', 'INFO')
    for f in os.listdir(path_dst):
        creation_time = os.path.getctime(path_dst + f)
        if (current_time - creation_time) // (24 * 3600) >= 7 and copy_flag == 1:  # If Older Than 7 Days --> Delete.
            LogHandling('I will delete this file:  {}'.format(f), 'WARN')
            os.remove(path_dst + f)
            LogHandling('The file {} was deleted'.format(f), 'WARN')

    path_src = "P:\"
    path_dst = "C:\"
