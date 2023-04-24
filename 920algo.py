"""
Author = RustAlgo

Basic Code Created in an Hour purely for Education, Not for Trading Using Real Money
You are Solely and wholly Responsible for Your Loss, Author is Not Responsible for Any of Your Losses

For More details refer README.md

"""

import datetime
import time
import requests
import polars as pl
from dhanhq import dhanhq
import os
from nsepython import *
import dhancred
import json


dhan= dhanhq(client_id=dhancred.client_id(),access_token=dhancred.access_token())
print(f"Session to Broker Dhan Established at {datetime.datetime.now()}")

## Lot Size and SL Points ##########

Qty= 50 #For Nifty Enter in Multiples of 50
SL= 30 # SL in Points
############################################################################################
underlying = "NIFTY"

nearestexpiry = expiry_list(underlying)
expiry= nearestexpiry[0]

print(f'Retrieving List of Expires')

print(f'List of Expires available {nearestexpiry[0:3]}')

print(f'Selected Expiry is {expiry}')

##############################################################################################

# Downloading Scrip Master of Dhan

print("Trying to Download Scrip Master from Broker....")
security_idsource = "https://images.dhan.co/api-data/api-scrip-master.csv"

todays_date= datetime.datetime.now().strftime("%Y-%m-%d")

file_name= f'api-scrip-master.csv'

response = requests.get(security_idsource)
with open(file_name, "wb") as f:
    f.write(response.content)
    print(f"Scrip Master '{file_name}' downloaded successfully.")

#Created a Polars Data Frame for Placing Orders
required_cols = ['SEM_EXM_EXCH_ID', 'SEM_SMST_SECURITY_ID', 'SEM_INSTRUMENT_NAME', 'SEM_TRADING_SYMBOL', 'SEM_CUSTOM_SYMBOL', 'SEM_EXPIRY_DATE', 'SEM_EXPIRY_FLAG']
master = pl.read_csv("api-scrip-master.csv", columns=required_cols)

#############################################################################################

while True:
    current_time = datetime.datetime.now().time()
    if current_time >= datetime.time(9, 20, 00):  #9,20,30
        print(f"It is now {datetime.datetime.now()}. Continuing...")
        break
    else:
        print(f"It is currently {current_time}. Waiting for Time Specified")
        time.sleep(1) # sleep for 1 second before checking Time Specified

###############################################################################

ltp= (nse_quote_ltp(underlying))
print(f'LTP of {underlying} is {ltp}')
ATM = round(ltp/50)*50
print(f'At The Money Strike Of Underlying {underlying} is {ATM}')
expiry_date_string = expiry
expiry_date = datetime.datetime.strptime(expiry_date_string, '%d-%b-%Y')
year = expiry_date.year
month = expiry_date.strftime('%b').upper()
day = expiry_date.day

atmCE= f'NIFTY {day} {month} {ATM} CALL'

atmPE= f'NIFTY {day} {month} {ATM} PUT'

print(f"At The Money Call Strike is {atmCE}, At The Money Put Strike is {atmPE}")

LTPofCE= float(nse_quote_ltp(underlying,"latest","CE",ATM))
print(f'Call is Now Trading at {LTPofCE}')

LTPofPE= float(nse_quote_ltp(underlying,"latest","PE",ATM))
print(f'Put is Now Trading at {LTPofPE}')

# Saving SL as LTP of CE and PE Minus SL as defined earlier Before placing order

ce_entry_price= float(LTPofCE)
pe_entry_price= float(LTPofPE)

#SL in Points

ceSL= float(round(LTPofCE+SL,1)) #SL in Points
peSL= float(round(LTPofPE+SL,1)) #SL in Points

#SL in Percentage

# ceSL = float(round(LTPofCE * (1 + SL), 1)) #SL in Percentage
# peSL = float(round(LTPofPE * (1 + SL), 1)) #SL in Percentage


#Getting Security ID for ATM CE and PE
securityidCE = master.filter(pl.col("SEM_CUSTOM_SYMBOL") == atmCE)["SEM_SMST_SECURITY_ID"][0]
securityidPE = master.filter(pl.col("SEM_CUSTOM_SYMBOL") == atmPE)["SEM_SMST_SECURITY_ID"][0]

print(f'Retrieved Security Id of Boker for CE is {securityidCE}')
print(f'Retrieved Security Id of Boker for PE is {securityidPE}')

print(f'placing Market Orders for {atmCE} and {atmPE}, Time is {datetime.datetime.now()}')

########################################################################################

dhan.place_order(security_id=str(securityidCE),
    exchange_segment=dhan.FNO,
    transaction_type=dhan.SELL,
    quantity=Qty,
    order_type=dhan.MARKET,
    product_type=dhan.INTRA,
    price=0)
print(f'Market Order Placed for {atmCE}, Time is {datetime.datetime.now()}')

dhan.place_order(security_id=str(securityidPE),  
    exchange_segment=dhan.FNO,
    transaction_type=dhan.SELL,
    quantity=Qty,
    order_type=dhan.MARKET,
    product_type=dhan.INTRA,
    price=0)
print(f'Market Order Placed for {atmPE}, Time is {datetime.datetime.now()}')

################################################################################
print(f"CEentryprice is {ce_entry_price}")
print(f"PEentryprice is {pe_entry_price}")
print(f"CESL is now {ceSL}")
print(f"PESL is now {peSL}")

###############################################################################

print("All Positions are Punched, Wating for SL")

traded = "No"
while traded == "No":
    dt=datetime.datetime.now()
    try:
        LTPofCE=nse_quote_ltp(underlying,"latest","CE",ATM)
        LTPofPE=nse_quote_ltp(underlying,"latest","PE",ATM)
        print("Check Started for SL and Exit")
        if ((LTPofCE > ceSL) or (dt.hour >= 15 and dt.minute >= 15)):
            exitCE= dhan.place_order(security_id=str(securityidCE),  exchange_segment=dhan.FNO,transaction_type=dhan.BUY,quantity=Qty,order_type=dhan.MARKET,product_type=dhan.INTRA,price=0)
            traded = "CE"
            print(f"Call SL Hit,LTP of CE Leg is {LTPofCE}")
            time.sleep(5)
        elif((LTPofPE > peSL) or (dt.hour >= 15 and dt.minute >= 15)):

            exitPE= dhan.place_order(security_id=str(securityidPE),  exchange_segment=dhan.FNO,transaction_type=dhan.BUY,quantity=Qty,order_type=dhan.MARKET,product_type=dhan.INTRA,price=0)
            traded = "PE"
            print(f"PUT SL Hit,LTP of PE Leg is {LTPofPE}")
            time.sleep(5)
        else:
            print(f"No SL is Hit, LTP of CE is {LTPofCE} and LTP of PE is {LTPofPE}")
            time.sleep(5)
            
    except:
        print("Couldn't find LTP , RETRYING !!")
        time.sleep(5)
        
    if(traded=="CE"):
        peSL = pe_entry_price
        while traded == "CE":
            dt= datetime.datetime.now()
            try:
                LTPofPE=nse_quote_ltp(underlying,"latest","PE",ATM)
                if ((ltp > peSL) or (dt.hour >= 15 and dt.minute >= 15)):
                    exitPE= dhan.place_order(security_id=str(securityidPE),  exchange_segment=dhan.FNO,transaction_type=dhan.BUY,quantity=Qty,order_type=dhan.MARKET,product_type=dhan.INTRA,price=0)
                    traded = "Close"
                    print(f"PE SL Hit, LTP of PE is {LTPofPE}")
                    time.sleep(5)
                else:
                    print(f"PE SL not hit,LTP of PE is {LTPofPE}")
                    time.sleep(1)
                  
            except:
                print("Couldn't find LTP , RETRYING !!")
                time.sleep(1)

    elif(traded == "PE"):
        ceSL = ce_entry_price
        while traded == "PE":
            dt = datetime.datetime.now()
            try:
                LTPofCE=nse_quote_ltp(underlying,"latest","CE",ATM)
                if ((ltp > ceSL) or (dt.hour >= 15 and dt.minute >= 15)):
                    exitCE= dhan.place_order(security_id=str(securityidCE),  exchange_segment=dhan.FNO,transaction_type=dhan.BUY,quantity=Qty,order_type=dhan.MARKET,product_type=dhan.INTRA,price=0)
                    traded = "Close"
                    time.sleep(5)
                    
                else:
                    print(f"CE SL not hit,LTP of CE is {LTPofCE}")
                    time.sleep(5)
            except:
                print("Couldn't find LTP , RETRYING !!")
                time.sleep(1)
    elif (traded == "Close"):
        print("All trades done. Exiting Code")
        