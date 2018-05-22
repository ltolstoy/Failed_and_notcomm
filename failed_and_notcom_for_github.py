#!/home/ltolstoy/anaconda3/bin/python
"""
Script to find failed FET in units, by comparing Iout to Iin1 and Iin2.
In failed unit Iout should be very close to one of Iin (with delta~=0.1
or 0.05A) for 95% of all records.
Find outliers, send email alert
"""

import os
import sys, argparse
import numpy as np
import pandas as pd
import warnings

def mail_notification_sendgrid(subject, text):
    """It sends email to several people with warning
    subject - like "List of FET failure units"
    text - body, like 
    "Body"
    https://stackoverflow.com/questions/31936881/sendgrid-python-library-and-templates
    """
    import smtplib
    from email.mime.text import MIMEText
    
    msg = MIMEText(text)
    me = "ltolstoy@xxxx.com"
    you = ["ListofFETfailures@xxxx.com" ]       

    msg['Subject'] = subject
    msg['From'] = me
    msg['To'] = ",".join(you)
    s = smtplib.SMTP('smtp.sendgrid.net')
    s.login('apikey', 'hardcoded_sendgrid_key') 
    s.sendmail(me, you, msg.as_string())
    s.quit()

def get_list_of_macs(block, p_to_logs):
    '''
    block = '302'...'405',
    p_to_logs - path to folder where log is, like /mnt/data_log/canadian_solar/151105/
    Here we find a file structure.xml, and get list_of_macs from it
    '''
    import xml.etree.ElementTree as ET
    from pathlib2 import Path
    p_to_struc = str(Path(p_to_logs).parents[0]) #gets 2 levels up, to /mnt/data_log/canadian_solar/
    
    
    name_str = '/structure_'+block+'.xml'
    p = p_to_struc + name_str #full path, including file name
    tree = ET.parse(p)
    root = tree.getroot()
    list_of_macs = []
    for m in root.iter('Converter'):
        a = m.get('mac')
        list_of_macs.append(''.join(a)) #otherwise doesn't work 
    print("getting list of macs from {}: got {} macs".format(name_str, len(list_of_macs) ))
    return list_of_macs

def ser2mac(serial):
    serial = serial.upper()
    week = int(serial[:2])
    year = int(serial[2:4])
    letter = ord(serial[4]) - 65
    ser = int(serial[5:])

    prefix = '%06X' % ((week << 18) | (year << 11) | (letter << 6))
    suffix = '%06X' % ser

    return prefix + suffix


def get_list_of_items(block, p_to_logs):
    '''
    block = '302','303',
    p_to_logs - path to folder where log is, like /mnt/data_log/canadian_solar/tmp/
    p_to_struc - Here we find a file structure_xxx.xml, and get list_of_macs, sn, string_name from it
    '''
    import xml.etree.ElementTree as ET
    p_to_struc = os.path.abspath(os.path.join(p_to_logs, os.pardir)) # gets 1 level up, to /mnt/data_log/canadian_solar/
    name_str = '/structure_'+block+'.xml'
    p = p_to_struc + name_str #full path, including file name
    if os.path.exists(p):
        tree = ET.parse(p)
        root = tree.getroot()
        macs = [] #mac addresses
        sns = []  #serial numbers
        stnames =[]  #string names "02.01.01-1"
        for m in root.iter('String'):
            a = m.get('name')
            stnames.append(''.join(a))
        for m in root.iter('Converter'):
            b = m.get('sn')
            try:
                sns.append(''.join(b))
            except:
                print("Exception in get_list_of_items: can't get sn. Receiving {} instead from {}".format(b, m.attrib))
            a = m.get('mac')
            try:
                macs.append(''.join(a))  # otherwise doesn't work
            except:
                print("Exception in get_list_of_items: can't get mac, probably it was not commissioned. The line is {} {}.Restoring mac from sn!".format(a, m.attrib))
                a2 = ser2mac(b)      # if no mac exsists in xml, restore it using ser2mac and add anyway
                macs.append(''.join(a2))  # otherwise doesn't work
                print("Restored mac {} from sn {}".format(a2,b))
        print("getting items from structure_{}.xml: got {} items".format(block, len(macs)) )
        return macs,sns,stnames
    else:
        print("{} doesnt exist, can work without structure.xml. Exiting now".format( p) )
        sys.exit()
"""
Main part
"""
parser = argparse.ArgumentParser(description='This is a script to find potentially failed units ')
parser.add_argument('-i','--input', help='CSV file to analyze with full path',required=True)
args = parser.parse_args()
    
if os.path.exists(args.input):
    fname = os.path.basename(args.input)            #getting log filename
    p_to_logs = os.path.dirname(args.input)+'/'     #getting log path
    #p_to_csv = p_to_logs
    print("Working on file {} from {}".format(fname, p_to_logs))  
else:
    print("File not found at {}".format(args.input))
    print("Exiting script, can't work without CSV file as input")
    sys.exit()
if not args.input.endswith('.csv'):
    print("Exiting script, can't work with log file as input, need csv file instead")
    sys.exit()
    
block = fname[fname.find('_b') + 1:fname.find('.')]  # Either b1,b2,b3 or b4, or b301_2...b508

print("Found block {}, all right, continuing".format(block ))

#list_of_macs = get_list_of_macs(block, p_to_logs)  # all good macs
macs, sns, stnames = get_list_of_items(block, p_to_logs)
with warnings.catch_warnings(record=True) as ws:
    warnings.simplefilter("always")
    data = pd.read_csv(args.input)    #, dtype={object}
    #print("Warnings raised:", ws)    #catch warning
total_rows , total_columns = data.shape #6535 ,3024
total_units = int((total_columns - 4)/20) #151
delta = 0.05 #A
percentage= 0.1 # was 70%, now 10% records with currents close enough, of n_total - means it's not single event but happens a lot
lim=2 #A. Current Iout (not Iin1,Iin2) should be > than this threshold
iout_thr = 0.1 #A. If average current < this threshold, it's probably "No Output Power" case
pdiss_thr = 120     # Threshold for selecting sporadic Pdiss units

n_highpdiss = 0     # count units with sporadic high Pdiss
n_nopower = 0       # Count units with Iout too low (No Power)
n_opencircuit = 0   # Count units with open circuit voltage,
n_failed = 0        # Count all failed for output
n_notcom = 0        #number of notcommunicating units
n_ref18 = 0         # number of units with some lines having Vref=18
n_ref78 = 0         # number of units with some lines having Vref=78
n_outliers = 0      #number of units of weired values in I,U,P, etc
n_moduloff = 0      #number of units with Module Off
list_moduloff = []  #list of macs with module off
list_outliers = []  # list macs with outliers
#need to change vout_thr for case 600V or 900 V units
vout_all = [] #list of all mean_vouts, for all macs
for i in range(total_units):
    pos_vout = 10 + i * 20
    cname_vout = data.columns.tolist()[pos_vout]
    mean_vout = data[cname_vout].mean()
    if not np.isnan(mean_vout): #append only not-nan elements
        vout_all.append(mean_vout)
vout_average = np.mean(vout_all)  # ex:  800.149 or 622.5
if vout_average > 700: #separate 600V and 800V cases
    vout_thr = 857  # V. If mean_vout > this threshold, it's probably "Open Circuit" case
else:
    vout_thr = 657  # V. If mean_vout > this threshold, it's probably "Open Circuit" case

list_fet_fails = [] #list of units with fet fails, to send alarm
for i in range(total_units):
    pos_mod =   9 + i * 20
    pos_vout = 10 + i * 20 #as in original csv, not _electrical!
    pos_vin1 = 11 + i * 20
    pos_iout = 12 + i * 20
    pos_vin2 = 13 + i * 20
    pos_text = 14 + i * 20
    pos_iin2 = 15 + i * 20
    pos_iin1 = 16 + i * 20
    pos_ref =  17 + i * 20
    pos_goff = 18 + i * 20
    pos_grss = 19 + i * 20
    pos_eoff = 20 + i * 20
    pos_erss = 21 + i * 20
    pos_ov = 22 + i * 20
    pos_oc = 23 + i * 20

    cname_iout = data.columns.tolist()[pos_iout]  #column name for Iout
    cname_iin1 = data.columns.tolist()[pos_iin1]
    cname_iin2 = data.columns.tolist()[pos_iin2]
    cname_vout = data.columns.tolist()[pos_vout]
    cname_vin1 = data.columns.tolist()[pos_vin1]
    cname_vin2 = data.columns.tolist()[pos_vin2]
    cname_text = data.columns.tolist()[pos_text]
    cname_ref = data.columns.tolist()[pos_ref]
    cname_mod = data.columns.tolist()[pos_mod]

    mean_vout = data[cname_vout].mean()
    mean_iout = data[cname_iout].mean()
    data['difference1'] = abs(data[cname_iout] - data[cname_iin1])
    data['difference2'] = abs(data[cname_iout] - data[cname_iin2])
    #data['difference1'] = abs(data.ix[:,pos_iout] - data.ix[:,pos_iin1])
    #data['difference2'] = abs(data.ix[:,pos_iout] - data.ix[:,pos_iin2])
    n1 = data[(data['difference1'] < delta) & (data[cname_iout] > lim)].count()['difference1'] # Num of rows where currents are the same
    n2 = data[(data['difference2'] < delta) & (data[cname_iout] > lim)].count()['difference2']
    n_total = data[data[cname_iout] > lim ].count()[cname_iout] #Num of not-NaN raws with Iout >lim
    #print "For mac ",i+1," n1=",n1," n2=",n2, " of total rows ", str(n_total)
    n_recorded = data[cname_iout].count() # Num of all recorded
    num_rec_to_consider = 30 # number of found records with Iout ~= Iin1 or Iin2, to say it's FET failure

    if np.isnan(mean_vout) : #means that there were no communication at all for this unit, so mean_vout is "nan"
        print("Mac-{} {} sn={} located at {} was not talking".format((i + 1), 
              macs[i],  macs[i], stnames[i]))
        n_notcom += 1

    if n1 > n_total*percentage and n_total > num_rec_to_consider:
        message = "Mac-"+ str(i+1)+" "+ macs[i]+" sn="+sns[i]+" located at "\
        +stnames[i]+" has Iout=Iin1 in "+str(round(100*n1/n_total, 1))\
        +"% of records ("+str(n1)+" of "+str(n_total)+") of recorded "\
        + str(n_recorded)
        print(message)
        list_fet_fails.append(message+"\n")
        n_failed += 1
        
        
    if n2 > n_total*percentage and n_total > num_rec_to_consider:
        message = "Mac-"+ str(i+1)+" "+ macs[i]+" sn="+sns[i]+" located at "\
        +stnames[i]+" has Iout=Iin2 in "+str(round(100*n2/n_total, 1))\
        +"% of records ("+str(n2)+" of "+str(n_total)+") of recorded "\
        + str(n_recorded)
        print(message)
        list_fet_fails.append(message + "\n")
        n_failed += 1
    # and n_total > num_rec_to_consider - doesn't work bcs n_total counted for Iout> 2 A!
    if mean_vout > vout_thr and n_recorded > num_rec_to_consider: #probably Open Circuit
        print("Mac-{} {} sn={} located at {} has {} records and mean Vout={}V, Open Circuit".format(
            (i + 1), macs[i], sns[i], stnames[i], 
              n_recorded, round(mean_vout, 1) ))
        #print "\n"
        n_opencircuit += 1

    if mean_iout < iout_thr and n_recorded > num_rec_to_consider: # probably no power produced
        print("Mac-{} {} sn={} located at {} has {} records and mean Iout={}A, No Output Power produced".format((i + 1), macs[i],
               sns[i], stnames[i], n_recorded, round(mean_iout, 3) ))
        #print "\n"
        n_nopower += 1

    if 18 in data[cname_ref].values: #found 18 in "Ref" column
        print("Mac-{} {} sn={} located at {} has Ref = 18, meaning FET failure suspected".format((i + 1), 
        macs[i], sns[i], stnames[i] ))
        # print "\n"
        n_ref18 += 1

    if 78 in data[cname_ref].values: #found 78 in "Ref" column
        print("Mac-{} {} sn={} located at {} has Ref = 78, meaning module was turned off.".format( (i + 1), 
        macs[i], sns[i], stnames[i]))
        # print "\n"
        n_ref78 += 1

    if 0 in data[cname_mod].values and 78 in data[cname_ref].values: #look for module OFF signatures
        # together with Vref=78, to eliminate case Module off in the morning 1 time
        message = "Mac-" + str(i + 1) + " " + macs[i] + " sn=" + sns[i] + " located at " \
              + stnames[i] + " has both Module OFF sign and Vref=78"
        print(message)
        list_moduloff.append(message+"\n")
        n_moduloff += 1

    if mean_vout > vout_thr and n_recorded > num_rec_to_consider: #probably Open Circuit
        print("Mac-{} {} sn={} located at {} has {} records and mean Vout={}V, Open Circuit.".format((i + 1), macs[i], sns[i], stnames[i], 
              n_recorded, round(mean_vout, 1) ))
        #print "\n"
        n_opencircuit += 1

    #data['pdiss_tmp'] = data.apply(lambda row: row.cname_iin1*row.cname_vin1 + row.cname_iin2*row.cname_vin2 - row.cname_iout*row.cname_vout, axis=1)
    data['pdiss_tmp'] = data.apply(
        lambda row: row[cname_iin1] * row[cname_vin1] + row[cname_iin2] * row[cname_vin2] - row[cname_iout] * row[cname_vout],
        axis=1)  #create additional temp column with Pdiss for current unit
    #data['pdiss_tmp'].plot()       #to plot only Pdiss

    n_pdiss = data[data['pdiss_tmp'] > pdiss_thr].count()['pdiss_tmp']  #count records with high Pdiss
    if n_pdiss >0:
        print("Mac-{} {} sn={} located at {} has {} records where Pdiss>{}W, probably sporadic high Pdiss.".format((i + 1),
         macs[i], sns[i], stnames[i], n_pdiss, pdiss_thr))
        n_highpdiss += 1

    #Looking for outliers
    v_top = 1500 #V, top voltage to find outliers
    v_bot = -1 #V, bottom voltage to find outliers
    i_top = 15 #A, top current to find outliers
    i_bot = -1 #A, bottom current to find outliers
    t_top = 100 #C, top Text to find outliers
    t_bot = -30 ##C, bottom Text to find outliers
    n_outliers_vout = data[(data[cname_vout] > v_top) | (data[cname_vout] < v_bot)].count()["VOut"]
    n_outliers_vin1 = data[(data[cname_vin1] > v_top) | (data[cname_vin1] < v_bot)].count()["Vin1"]
    n_outliers_vin2 = data[(data[cname_vin2] > v_top) | (data[cname_vin2] < v_bot)].count()["Vin2"]
    n_outliers_iout = data[(data[cname_iout] > i_top) | (data[cname_iout] < i_bot)].count()["IOut"]
    n_outliers_iin1 = data[(data[cname_iin1] > i_top) | (data[cname_iin1] < i_bot)].count()["Iin1"]
    n_outliers_iin2 = data[(data[cname_iin2] > i_top) | (data[cname_iin2] < i_bot)].count()["Iin2"]
    n_outliers_text = data[(data[cname_text] > t_top) | (data[cname_text] < t_bot)].count()["Text"]
    if n_outliers_vout != 0:
        print("Mac-{} {} sn={} located at {} has Vout outside the range [{} to {}]V in {} records, possibly outliers".format(i + 1,
            macs[i],sns[i], stnames[i], v_bot, v_top, n_outliers_vout ))
        list_outliers.append(macs[i])

    if n_outliers_vin1 != 0:
        #print "Mac-" + str(i + 1) + " " + macs[i] + " sn=" + sns[i] + " located at " \
        #    + stnames[i] + " has Vin1 outside the range [ " + str(v_bot) \
        #    + " to " + str(v_top) + " ]V in  " + str(n_outliers_vin1) + " records, possibly outliers"
        print("Mac-{} {} sn={} located at {} has Vin1 outside the range [{} to {}]V in {} records, possibly outliers".format(i + 1,
            macs[i],sns[i], stnames[i], v_bot, v_top, n_outliers_vin1 ))
        list_outliers.append(macs[i])

    if n_outliers_vin2 != 0:
        #print "Mac-" + str(i + 1) + " " + macs[i] + " sn=" + sns[i] + " located at " \
        #    + stnames[i] + " has Vin2 outside the range [ " + str(v_bot) \
        #    + " to " + str(v_top) + " ]V in  " + str(n_outliers_vin2) + " records, possibly outliers"
        print("Mac-{} {} sn={} located at {} has Vin2 outside the range [{} to {}]V in {} records, possibly outliers".format(i + 1,
            macs[i],sns[i], stnames[i], v_bot, v_top, n_outliers_vin2 ))
        list_outliers.append(macs[i])

    if n_outliers_iout != 0:
        #print "Mac-" + str(i + 1) + " " + macs[i] + " sn=" + sns[i] + " located at " \
        #    + stnames[i] + " has Iout outside the range [ " + str(i_bot) \
        #    + " to " + str(i_top) + " ]A in  " + str(n_outliers_iout) + " records, possibly outliers"
        print("Mac-{} {} sn={} located at {} has Iout outside the range [{} to {}]A in {} records, possibly outliers".format(i + 1,
            macs[i],sns[i], stnames[i], i_bot, i_top, n_outliers_iout ))
        list_outliers.append(macs[i])
        

    if n_outliers_iin1 != 0:
        #print "Mac-" + str(i + 1) + " " + macs[i] + " sn=" + sns[i] + " located at " \
        #    + stnames[i] + " has Iin1 outside the range [ " + str(i_bot) \
        #    + " to " + str(i_top) + " ]A in  " + str(n_outliers_iin1) + " records, possibly outliers"
        print("Mac-{} {} sn={} located at {} has Iin1 outside the range [{} to {}]A in {} records, possibly outliers".format(i + 1,
            macs[i],sns[i], stnames[i], i_bot, i_top, n_outliers_iin1 ))
        list_outliers.append(macs[i])

    if n_outliers_iin2 != 0:
        #print "Mac-" + str(i + 1) + " " + macs[i] + " sn=" + sns[i] + " located at " \
        #    + stnames[i] + " has Iin2 outside the range [ " + str(i_bot) \
        #    + " to " + str(i_top) + " ]A in  " + str(n_outliers_iin2) + " records, possibly outliers"
        print("Mac-{} {} sn={} located at {} has Iin2 outside the range [{} to {}]A in {} records, possibly outliers".format(i + 1,
            macs[i],sns[i], stnames[i], i_bot, i_top, n_outliers_iin2 ))
        list_outliers.append(macs[i])

    if n_outliers_text != 0:
        #print "Mac-" + str(i + 1) + " " + macs[i] + " sn=" + sns[i] + " located at " \
        #    + stnames[i] + " has Text outside the range [ " + str(t_bot) \
        #    + " to " + str(t_top) + " ]C in  " + str(n_outliers_text) + " records, possibly outliers"
        print("Mac-{} {} sn={} located at {} has Text outside the range [{} to {}]C in {} records, possibly outliers".format(i + 1,
            macs[i],sns[i], stnames[i], t_bot, t_top, n_outliers_text ))
        list_outliers.append(macs[i])

    if i%20 ==0:
        #print 'Device:' + str(i),
        #sys.stdout.flush()
        pass
if n_failed !=0:
    print("File {}\nTotal number of FET failed units for the site is {}".format(args.input, n_failed)) 
    #msg = '{p1} \n {p2}'.format(p1="List of FET failures for " + fname , p2=list_fet_fails)
    msg = "\n".join([str(i) for i in list_fet_fails])
    #print msg
    print("Sending email alert about FET failures...")
    subj_path = " site " + args.input.split('/')[3] + " file " + args.input.split('/')[-1]
    mail_notification_sendgrid("List of FET failures for the " + subj_path, msg)
if n_opencircuit !=0:
    print("Total number of open circuit units (mean Vout > {}V) is {}".format(vout_thr, n_opencircuit))
if n_nopower != 0:
    print("Total number of units not producing power (mean Iout < F{}A) is {}".format(iout_thr, n_nopower))
if n_notcom !=0:
    print("Total number of units that didn't talk is {}".format(n_notcom))
if n_ref18 !=0:
    print("Total number of units with Ref=18 is {}".format(n_ref18))
if n_ref78 !=0:
    print("Total number of units with Ref=78 is {}".format(n_ref78))
if n_highpdiss !=0:
    print("Total number of units with sporadic hight Pdiss is {}".format(n_highpdiss))
if len(set(list_outliers)) !=0:
    print("Total number of units with found outliers is {}".format(len(set(list_outliers)) ) )
if n_moduloff !=0:
    print("Total number of units with Module OFF sign and Vref=78 is {}".format(n_moduloff))
    print("Sending email alert about units with Module OFF and Vref=78...")
    subj_path = " site " + args.input.split('/')[3] + " file " + args.input.split('/')[-1]
    msg = "\n".join([str(i) for i in list_moduloff])
    mail_notification_sendgrid("List of Module Off units for the " + subj_path, msg)

print("\n" , "---"*10)
