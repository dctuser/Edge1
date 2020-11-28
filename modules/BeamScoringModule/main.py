# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.

# check backup file- BeamScoringModule_runable.txt for runable module

import time
import datetime
import os
import sys
import asyncio
from six.moves import input
import threading
from azure.iot.device.aio import IoTHubModuleClient

import pandas as pd
import numpy as np
import scipy
from scipy import stats
from scipy.spatial import distance
import pickle
import json

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)


# Initialize parameters
RECEIVED_MESSAGES_COUNTER = 0  # number of received messages
variables = [
    'MoisturePPM',
    'OxgenPercentage',
    'OxgenPPM']  # Variables to parse from the data received
var_for_MLprediction = [0,2]   # User to choose which variable to calculate the mean value, current ML model use 'current', variables[2]

# Set/initialize global para for storing coming data per min
MOISTUREppm_DATA = []
MOISTUREppm_TIMESTAMP = []
OXGENppm_DATA = []
OXGENppm_TIMESTAMP = []
data_all_moisturePPM = pd.DataFrame([[0,str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())),0]],columns = ['MOISTUREppm_DATA', 'MOISTUREppm_TIMESTAMP', 'Data_noLeak'])
data_all_oxgenPPM = pd.DataFrame([[0,str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())),0]],columns = ['OXGENppm_DATA', 'OXGENppm_TIMESTAMP','Data_noLeak'])

# data_all_moisturePPM = pd.DataFrame([0,str(time.time()),0],columns = ['MOISTUREppm_DATA', 'MOISTUREppm_TIMESTAMP', 'Data_noLeak'])
# data_all_oxgenPPM = pd.DataFrame([0,str(time.time()),0],columns = ['OXGENppm_DATA', 'OXGENppm_TIMESTAMP','Data_noLeak'])
MESSAGE_RECEIVED_COUNTER = 0

THR_TO_ACCUMULATE_moisturePPM = 25  # Threshold to start to accumulate points for calculating the distance
ACCUMULATE_FLAG_moisturePPM = 0  # signal control the accumulation process
ZERO_COUNTER_moisturePPM = 0
ACCUMULATE_LEN_moisturePPM = 0  # variable to count the number of accumulated data record for MoisturePPM

THR_TO_ACCUMULATE_oxgenPPM = 25  # Threshold to start to accumulate points for calculating the distance
ACCUMULATE_FLAG_oxgenPPM = 0  # signal control the accumulation process
ZERO_COUNTER_oxgenPPM = 0
ACCUMULATE_LEN_oxgenPPM = 0  # variable to count the number of accumulated data record for MoisturePPM

print("000000000000000000000 come here 1")

async def main():
    try:
        if not sys.version >= "3.5.3":
            raise Exception( "The sample requires python 3.5.3+. Current version of Python: %s" % sys.version )
        print ( "IoT Hub Client for Python" )

        # The client object is used to interact with your Azure IoT hub.
        module_client = IoTHubModuleClient.create_from_edge_environment()

        # connect the client.
        await module_client.connect()
        print("000000000000000000000 come here 2")

        # # Prepare no-leak data
        # thr = 12064.74  # Set up threshold
        # thr_sl = 81000  # Set up small leak threshold
        # set parameters
        # thr_sl = 25000 #12064.74  # Set up threshold
        # thr_leak = 50000 #81000     # Set up small leak threshold
        #small_leak = 0
        #big_leak = 0
        thr_moisture = 50000
        # thr_oxygen = 5000
        
        data_noLeak_moisturePPM = pd.read_csv("data_moisturePPM_noLeak.csv")
        data_noLeak_moisturePPM = data_noLeak_moisturePPM['Data_noLeak']

        data_noLeak_oxgenPPM = pd.read_csv("data_oxygenPPM_noLeak.csv")
        data_noLeak_oxgenPPM = data_noLeak_oxgenPPM['Data_noLeak']
        print("000000000000000000000 come here 3")

        # define behavior for receiving an input message on input1
        async def input1_listener(module_client):

            global MESSAGE_RECEIVED_COUNTER
            global MOISTUREppm_DATA
            global MOISTUREppm_TIMESTAMP
            global OXGENppm_DATA
            global OXGENppm_TIMESTAMP

            global data_all_moisturePPM
            global data_all_oxgenPPM
            global THR_TO_ACCUMULATE_moisturePPM
            global ACCUMULATE_FLAG_moisturePPM
            global ZERO_COUNTER_moisturePPM
            global ACCUMULATE_LEN_moisturePPM
            global MESSAGE_RECEIVED_COUNTER

            global THR_TO_ACCUMULATE_oxgenPPM
            global ACCUMULATE_FLAG_oxgenPPM
            global ZERO_COUNTER_oxgenPPM
            global ACCUMULATE_LEN_oxgenPPM

            while True:
                try:
                    # Receive data from Data2dbModule
                    input_message = await module_client.receive_message_on_input("input31")  # blocking call
                    print("The data in the message received on input31 was ")
                    print(input_message.data)
                    print("custom properties are")
                    print(input_message.custom_properties)
                    MESSAGE_RECEIVED_COUNTER=MESSAGE_RECEIVED_COUNTER + 1
                    print("000000000000000000000 come here 4")

                    ########## Parse MoisturePPM data ###############
                    message = input_message.data
                    message_text = message.decode('utf-8')
                    data_list = json.loads(message_text)

                    # data_list = [
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 24,
                    #             "MeasureTime": "2020-10-09T04:01:19.6413502Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 23.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.232Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 22.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.6088079Z"
                    #         },
                    #         {
                    #             "Variable": "MoisturePPM",
                    #             "Value": 1970,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "MoisturePPM",
                    #             "Value": 1875,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value":  9000,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value": 9000,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         }
                    #     ]
                    # if MESSAGE_RECEIVED_COUNTER == 1 or MESSAGE_RECEIVED_COUNTER == 2 or MESSAGE_RECEIVED_COUNTER == 3:
                    #     data_list = [
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 24,
                    #             "MeasureTime": "2020-10-09T04:01:19.6413502Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 23.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.232Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 22.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.6088079Z"
                    #         },
                    #         {
                    #             "Variable": "MoisturePPM",
                    #             "Value": 800,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "MoisturePPM",
                    #             "Value": 800,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value":  9123,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value": 9123,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         }
                    #     ]
                    # elif MESSAGE_RECEIVED_COUNTER == 4:
                    #     data_list = [
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 24,
                    #             "MeasureTime": "2020-10-09T04:01:19.6413502Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 23.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.232Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 22.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.6088079Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value":  9004,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value": 9004,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         }
                    #     ]
                    # elif MESSAGE_RECEIVED_COUNTER == 5:
                    #     data_list = [
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 24,
                    #             "MeasureTime": "2020-10-09T04:01:19.6413502Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 23.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.232Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 22.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.6088079Z"
                    #         },
                    #         {
                    #             "Variable": "MoisturePPM",
                    #             "Value": 40,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "MoisturePPM",
                    #             "Value": 40,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value":  9005,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value": 9005,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         }
                    #     ]
                    # elif MESSAGE_RECEIVED_COUNTER == 6 or MESSAGE_RECEIVED_COUNTER == 7 or MESSAGE_RECEIVED_COUNTER == 8 or MESSAGE_RECEIVED_COUNTER == 9:
                    #     data_list = []
                    # if MESSAGE_RECEIVED_COUNTER == 1:
                    #     data_list = [
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 24,
                    #             "MeasureTime": "2020-10-09T04:01:19.6413502Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 23.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.232Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 22.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.6088079Z"
                    #         },
                    #         {
                    #             "Variable": "MoisturePPM",
                    #             "Value": 800,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "MoisturePPM",
                    #             "Value": 800,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value": 1228.236365,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value": 1228.236365,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         }
                    #     ]

                    # elif MESSAGE_RECEIVED_COUNTER == 2:
                    #     data_list = [
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 24,
                    #             "MeasureTime": "2020-10-09T04:01:19.6413502Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 23.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.232Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 22.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.6088079Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value": 1233.106582,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value": 1233.106582,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         }
                    #     ]
                    # elif MESSAGE_RECEIVED_COUNTER == 3:
                    #     data_list = [
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 24,
                    #             "MeasureTime": "2020-10-09T04:01:19.6413502Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 23.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.232Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 22.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.6088079Z"
                    #         },
                    #         {
                    #             "Variable": "MoisturePPM",
                    #             "Value": 40,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "MoisturePPM",
                    #             "Value": 40,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value": 1211.483372,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value": 1211.483372,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         }
                    #     ]
                    # elif MESSAGE_RECEIVED_COUNTER == 4:
                    #     data_list = [
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 24,
                    #             "MeasureTime": "2020-10-09T04:01:19.6413502Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 23.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.232Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 22.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.6088079Z"
                    #         },
                    #         {
                    #             "Variable": "MoisturePPM",
                    #             "Value": 40,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "MoisturePPM",
                    #             "Value": 40,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value": 1144.08382,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value": 1144.08382,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         }
                    #     ]
                    # elif MESSAGE_RECEIVED_COUNTER == 5:
                    #     data_list = [
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 24,
                    #             "MeasureTime": "2020-10-09T04:01:19.6413502Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 23.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.232Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 22.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.6088079Z"
                    #         },
                    #         {
                    #             "Variable": "MoisturePPM",
                    #             "Value": 40,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "MoisturePPM",
                    #             "Value": 40,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value": 1178.624673,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value": 1178.624673,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         }
                    #     ]
                    # elif MESSAGE_RECEIVED_COUNTER == 6:
                    #     data_list = [
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 24,
                    #             "MeasureTime": "2020-10-09T04:01:19.6413502Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 23.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.232Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 22.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.6088079Z"
                    #         },
                    #         {
                    #             "Variable": "MoisturePPM",
                    #             "Value": 40,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "MoisturePPM",
                    #             "Value": 40,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value": 1268.331661,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value": 1268.331661,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         }
                    #     ]
                    # elif MESSAGE_RECEIVED_COUNTER == 7:
                    #     data_list = [
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 24,
                    #             "MeasureTime": "2020-10-09T04:01:19.6413502Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 23.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.232Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 22.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.6088079Z"
                    #         },
                    #         {
                    #             "Variable": "MoisturePPM",
                    #             "Value": 40,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "MoisturePPM",
                    #             "Value": 40,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value": 1334.747125,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value": 1334.747125,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         }
                    #     ]
                    # elif MESSAGE_RECEIVED_COUNTER == 8:
                    #     data_list = [
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 24,
                    #             "MeasureTime": "2020-10-09T04:01:19.6413502Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 23.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.232Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 22.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.6088079Z"
                    #         },
                    #         {
                    #             "Variable": "MoisturePPM",
                    #             "Value": 40,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "MoisturePPM",
                    #             "Value": 40,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value": 1403.467644,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value": 1403.467644,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         }
                    #     ]
                    # elif MESSAGE_RECEIVED_COUNTER == 9:
                    #     data_list = [
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 24,
                    #             "MeasureTime": "2020-10-09T04:01:19.6413502Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 23.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.232Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPercentage",
                    #             "Value": 22.17,
                    #             "MeasureTime": "2020-10-09T04:01:20.6088079Z"
                    #         },
                    #         {
                    #             "Variable": "MoisturePPM",
                    #             "Value": 40,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "MoisturePPM",
                    #             "Value": 40,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value": 1465.555563,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         },
                    #         {
                    #             "Variable": "OxgenPPM",
                    #             "Value": 1465.555563,
                    #             "MeasureTime": "2020-10-09T04:01:21.6014776Z"
                    #         }
                    #     ]
                    # elif MESSAGE_RECEIVED_COUNTER == 10 or MESSAGE_RECEIVED_COUNTER == 12 or MESSAGE_RECEIVED_COUNTER == 13 or MESSAGE_RECEIVED_COUNTER == 14:
                    #     data_list = []
                    print("000000000000000000000 come here 5")

                    value_extracted=[]
                    measureTime_extracted=[]

                    if (data_list is not 'null'):
                    # print('JJJJJJJ came here lian 1')
                    # startTime2 = time.time()
                        for j in range(0,len(variables)):
                            value_extracted.append([])
                            measureTime_extracted.append([])
                            for data in data_list:
                                # print('==============data:', data)
                                if data['Variable'] == variables[j]:
                                    # print('============data[value]=',data['Value'])
                                    value_extracted[j].append(data['Value'])
                                    measureTime_extracted[j].append(data['MeasureTime'])
                                    # print('come here 23')
                    # Remove zero rows and set the zero row value as last record
                    print("000000000000000000000 come here 6")
                    kk = 0
                    for i in value_extracted:
                        if i:
                            print('value_extracted=', value_extracted[kk])
                        elif (not len(i) and kk==0):
                            value_extracted[kk] = [data_all_moisturePPM.iloc[-1,0]]#[0]
                            measureTime_extracted[kk] = [data_all_moisturePPM.iloc[-1,1]]#['2000-00-00T00:00:00.0000000Z']
                        elif (not len(i) and kk==2):
                            value_extracted[kk] = [data_all_oxgenPPM.iloc[-1,0]]#[0]
                            measureTime_extracted[kk] = [data_all_oxgenPPM.iloc[-1,1]]#['2000-00-00T00:00:00.0000000Z']
                        kk = kk + 1

                    print("000000000000000000000 come here 7")
                    # Calculate mean value of moisturePPM(mean_moisturePPM) and max value of moisturePPM_timestamp(max_moisturePPM_timestamp)
                    print('var_for_MLprediction=', var_for_MLprediction[0])
                    mean_var_MLprediction_moisturePPM = np.mean(value_extracted[var_for_MLprediction[0]])
                    max_time_MLprediction_moisturePPM = measureTime_extracted[var_for_MLprediction[0]][-1]
                    print("000000000000000000000 come here 8")

                    # Calculate mean value of oxgenPPM(mean_oxgenPPM) and max value of oxgenPPM_timestamp(max_oxgenPPM_timestamp)
                    print('var_for_MLprediction=', var_for_MLprediction[1])
                    mean_var_MLprediction_oxgenPPM = np.mean(value_extracted[var_for_MLprediction[1]])
                    max_time_MLprediction_oxgenPPM = measureTime_extracted[var_for_MLprediction[1]][-1]
                    print("000000000000000000000 come here 9")

                    #====================================================================
                    # Calculate distance for MoisturePPM
                    #====================================================================
                    # Check 0 records to verify whether machine is in process.
                    # If 0 record is found, then clear data and re-accumulate data, otherwise continue to accumulate.
                    # If accumulated data length reaches 5, then do not clear data and keep accumulating.
                    if mean_var_MLprediction_moisturePPM == 0:
                        ZERO_COUNTER_moisturePPM = ZERO_COUNTER_moisturePPM + 1
                    if ZERO_COUNTER_moisturePPM > 3 and ACCUMULATE_LEN_moisturePPM < 5:
                        print("=========================================================================")
                        print('Found 0 records before process, so data_all_moisturePPM are cleared and restarted to accumulate data.')
                        print("=========================================================================")
                        MESSAGE_RECEIVED_COUNTER = 0
                        MOISTUREppm_DATA = []
                        MOISTUREppm_TIMESTAMP = []
                        ACCUMULATE_FLAG_moisturePPM = 0
                        ZERO_COUNTER_moisturePPM = 0
                        ACCUMULATE_LEN_moisturePPM = 0
                    print("000000000000000000000 come here 10")

                    if (mean_var_MLprediction_moisturePPM > THR_TO_ACCUMULATE_moisturePPM) or (ACCUMULATE_FLAG_moisturePPM == 1):
                        print('00000000000000000 come here 11')
                        ACCUMULATE_FLAG_moisturePPM = 1
                        ACCUMULATE_LEN_moisturePPM = ACCUMULATE_LEN_moisturePPM + 1

                        ######### Inerting cycle ###############
                        # Step 1.1 Append mean_moisturePPM to moisturePPM_data, max_moisturePPM_timestamp to moisturePPM_timestamp
                        MOISTUREppm_DATA.append(mean_var_MLprediction_moisturePPM)
                        MOISTUREppm_TIMESTAMP.append(max_time_MLprediction_moisturePPM)
                        print("000000000000000000000 come here 12")

                        # Step 1.2 Join all data together into dataframe =[moisturePPM_timestamp, moisturePPM_data,noleak_data]
                        df_MOISTUREppm_DATA = pd.DataFrame(MOISTUREppm_DATA, columns=['MOISTUREppm_DATA'])
                        print("000000000000000000000 come here 13")
                        df_MOISTUREppm_TIMESTAMP = pd.DataFrame(MOISTUREppm_TIMESTAMP,
                                                                columns=['MOISTUREppm_TIMESTAMP'])
                        print("000000000000000000000 come here 14")
                        df_MOISTUREppm_DATA = df_MOISTUREppm_DATA.join(df_MOISTUREppm_TIMESTAMP, how='inner')
                        print("000000000000000000000 come here 15")

                        data_all_moisturePPM = df_MOISTUREppm_DATA.join(data_noLeak_moisturePPM.iloc[:MESSAGE_RECEIVED_COUNTER], how='inner')
                        pd.set_option('display.max_rows', None)
                        print("000000000000000000000 come here 16")
                        print('df_MOISTUREppm_DATA_shape=', df_MOISTUREppm_DATA.shape)  # add by lian
                        print('data_all_moisturePPM_shape=', data_all_moisturePPM.shape)  # add by lian
                        print('data_all_moisturePPM=', data_all_moisturePPM)

                        # Step 1.3 Calculate Euclidean distance between moisturePPM_data and noleak_data
                        distance_time_moisturePPM = distance.euclidean(data_all_moisturePPM.iloc[:, 0].values, data_all_moisturePPM.iloc[:, 2].values)
                        # print("000000000000000000000 come here 17")
                        print('distance_time=', distance_time_moisturePPM)
                        # print("000000000000000000000 come here 18")

                        # ====================================================================
                        # Calculate distance for oxgenPPM
                        # ====================================================================
                        # Check 0 records to verify whether machine is in process.
                        # If 0 record is found, then clear data and re-accumulate data, otherwise continue to accumulate.
                        # If accumulated data length reaches 5, then do not clear data and keep accumulating.
                        if mean_var_MLprediction_oxgenPPM == 0:
                            ZERO_COUNTER_oxgenPPM = ZERO_COUNTER_oxgenPPM + 1
                        if ZERO_COUNTER_oxgenPPM > 3 and ACCUMULATE_LEN_oxgenPPM < 5:
                            print("=========================================================================")
                            print(
                                'Found 0 records before process, so data_all_moisturePPM are cleared and restarted to accumulate data.')
                            print("=========================================================================")
                            MESSAGE_RECEIVED_COUNTER = 0
                            OXGENppm_DATA = []
                            OXGENppm_TIMESTAMP = []
                            # small_leak = 0
                            # big_leak = 0
                            ACCUMULATE_FLAG_oxgenPPM = 0
                            ZERO_COUNTER_oxgenPPM = 0
                            ACCUMULATE_LEN_oxgenPPM = 0
                        print("000000000000000000000 come here 19")

                        if (mean_var_MLprediction_oxgenPPM > THR_TO_ACCUMULATE_oxgenPPM) or (ACCUMULATE_FLAG_oxgenPPM == 1):
                            print('00000000000000000 come here 20')
                            ACCUMULATE_FLAG_oxgenPPM = 1
                            ACCUMULATE_LEN_oxgenPPM = ACCUMULATE_LEN_oxgenPPM + 1

                            ######### Inerting cycle ###############
                            # Step 1.1 Append mean_moisturePPM to moisturePPM_data, max_moisturePPM_timestamp to moisturePPM_timestamp
                            OXGENppm_DATA.append(mean_var_MLprediction_oxgenPPM)
                            OXGENppm_TIMESTAMP.append(max_time_MLprediction_oxgenPPM)
                            print("000000000000000000000 come here 21")

                            # ------------------------------------------------------------------------------------
                            # Step 1.2 Join all data together into dataframe =[moisturePPM_timestamp, moisturePPM_data,noleak_data]
                            df_OXGENppm_DATA = pd.DataFrame(OXGENppm_DATA, columns=['OXGENppm_DATA'])
                            print("000000000000000000000 come here 22")
                            df_OXGENppm_TIMESTAMP = pd.DataFrame(OXGENppm_TIMESTAMP,
                                                                    columns=['OXGENppm_TIMESTAMP'])
                            print("000000000000000000000 come here 23")
                            df_OXGENppm_DATA = df_OXGENppm_DATA.join(df_OXGENppm_TIMESTAMP, how='inner')
                            print("000000000000000000000 come here 24")
                            data_all_oxgenPPM = df_OXGENppm_DATA.join(data_noLeak_oxgenPPM.iloc[:MESSAGE_RECEIVED_COUNTER],
                                                                            how='inner')
                            pd.set_option('display.max_rows', None)
                            print("000000000000000000000 come here 25")
                            print('df_OXGENppm_DATA_shape=', df_OXGENppm_DATA.shape)  # add by lian
                            print('data_all_oxgenPPM_shape=', data_all_oxgenPPM.shape)  # add by lian
                            # print('df_MOISTUREppm_DATA=', df_MOISTUREppm_DATA)
                            print('data_all_oxgenPPM=', data_all_oxgenPPM)
                            print('MESSAGE_RECEIVED_COUNTER=',MESSAGE_RECEIVED_COUNTER)

                            #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
                            # =====================================================================
                            # calculate the slop change for oxygenPPM
                            # =====================================================================
                            if MESSAGE_RECEIVED_COUNTER==1 or MESSAGE_RECEIVED_COUNTER==2:
                                diff1 = 0
                                diff2 = 0
                            else:
                                diff1 = data_all_oxgenPPM.iloc[-1, 0] - data_all_oxgenPPM.iloc[-2, 0]
                                diff2 = data_all_oxgenPPM.iloc[-2, 0] - data_all_oxgenPPM.iloc[-3, 0]
                            if (diff1>0.001 and diff2>0.001) or distance_time_moisturePPM > thr_moisture:
                                Leak_Pred = int(1)
                                print("Leakage condition: Big Leak")
                                print('')
                                Leak_Message = {
                                    "Time_to_Detect_Leak": int(data_all_oxgenPPM.index[-1]),
                                    "Leakage_Detected_Timestamp": data_all_oxgenPPM['OXGENppm_TIMESTAMP'].values[-1],
                                    # "Time_to_Big_Leak": int(0),
                                    "Minimum_value_pred": int(-1),
                                    "Maximum_value_pred": int(1),
                                    "Minimum_value_time": int(0),
                                    "Maximum_value_time": int(60)
                                    }
                            else:
                                Leak_Pred = int(0)  # add by lian  0-No leak
                                Leak_Message = {
                                    "Time_to_Detect_Leak": int(0),
                                    "Leakage_Detected_Timestamp": str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())),
                                    # "Time_to_Big_Leak": int(0),
                                    "Minimum_value_pred": int(-1),
                                    "Maximum_value_pred": int(1),
                                    "Minimum_value_time": int(0),
                                    "Maximum_value_time": int(60)
                                }
                                print("Leakage condition: No Leak")
                                print('')
                            output = {'Leak_Pred': Leak_Pred}.copy()
                            output.update(Leak_Message)
                            output31 = output
                        else:
                            Leak_Pred = int(-1)  # -1-No data received
                            Leak_Message = {
                                "Time_to_Detect_Leak": int(0),
                                "Leakage_Detected_Timestamp": str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())),
                                # "Time_to_Big_Leak": int(0),
                                "Minimum_value_pred ": int(-1),
                                "Maximum_value_pred ": int(1),
                                "Minimum_value_time ": int(0),
                                "Maximum_value_time ": int(60)
                                }
                            output = {'Leak_Pred': Leak_Pred}.copy()
                            output.update(Leak_Message)
                            output31 = output

                            # ------------------------------------------------------------------------------------
                            # Step 1.3 Calculate Euclidean distance between moisturePPM_data and noleak_data
                            # distance_time_oxgenPPM = distance.euclidean(data_all_oxgenPPM.iloc[:, 0].values,
                            #                                 data_all_oxgenPPM.iloc[:, 2].values)
                            # print("000000000000000000000 come here 26")
                            # print('distance_time_oxgenPPM=', distance_time_oxgenPPM)
                            # print("000000000000000000000 come here 27")

                        #000000000000000000000000000000000000000000000000000000000000000000000000000
                        # Predict results based on thresholds
                        #000000000000000000000000000000000000000000000000000000000000000000000000000
                    #     print('distance_time_moisturePPM=', distance_time_moisturePPM)
                    #     print('distance_time_oxgenPPM=', distance_time_oxgenPPM)
                    #     if distance_time_moisturePPM < thr_moisture and distance_time_oxgenPPM < thr_oxygen:
                    #         Leak_Pred = int(0)  # add by lian  0-No leak
                    #         Leak_Message = {
                    #             "Time_to_Detect_Leak": int(0),
                    #             "Leakage_Detected_Timestamp": str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())),#'2000-01-01T10:00:00.1000000Z',
                    #             # "Time_to_Big_Leak": int(0),
                    #             "Minimum_value_pred": int(-1),
                    #             "Maximum_value_pred": int(1),
                    #             "Minimum_value_time": int(0),
                    #             "Maximum_value_time": int(60)
                    #         }
                    #         print("Leakage condition: No Leak")
                    #         print('')
                    #     else:
                    #         Leak_Pred = int(1)  # add by lian  2- big leak
                    #         # print("Leakage condition: Big Leak")
                    #         # print('')
                    #         Leak_Message = {
                    #             "Time_to_Detect_Leak": int(data_all_moisturePPM.index[-1]),
                    #             "Leakage_Detected_Timestamp": data_all_moisturePPM['MOISTUREppm_TIMESTAMP'].values[-1],
                    #             # "Time_to_Big_Leak": int(0),
                    #             "Minimum_value_pred": int(-1),
                    #             "Maximum_value_pred": int(1),
                    #             "Minimum_value_time": int(0),
                    #             "Maximum_value_time": int(60)
                    #             }
                    #     output = {'Leak_Pred': Leak_Pred}.copy()
                    #     output.update(Leak_Message)
                    #     output31 = output
                    #     print("000000000000000000000 come here 28")
                    # else:
                    #     Leak_Pred = int(-1)  # -1-No data received
                    #     Leak_Message = {
                    #         "Time_to_Detect_Leak": int(0),
                    #         "Leakage_Detected_Timestamp": str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())),#'2000-01-01T10:00:00.1000000Z',
                    #         # "Time_to_Big_Leak": int(0),
                    #         "Minimum_value_pred ": int(-1),
                    #         "Maximum_value_pred ": int(1),
                    #         "Minimum_value_time ": int(0),
                    #         "Maximum_value_time ": int(60)
                    #     }
                    #     output = {'Leak_Pred': Leak_Pred}.copy()
                    #     output.update(Leak_Message)
                    #     output31 = output
                    # print("000000000000000000000 come here 29")

                    print('-----------------Inerting Cycle Analysis output31:',output31)

                    ################### Generate results and output to IoT hub ####################
                    print("forwarding message to output1")
                    await module_client.send_message_to_output(json.dumps(output31), "output31")
                    print("000000000000000000000 come here 30")
                    for i in range(0,60):
                        await module_client.send_message_to_output(json.dumps(output31), "output31")
                        time.sleep(1)
                        # print('-----------------output31 times = ', i+1)
                        # print('-----------------Inerting Cycle Analysis output31:',output31)
                    
                    print("000000000000000000000 come here 31")
                    # await module_client.send_message_to_output(json.dumps(output32), "output32")
                except Exception as ex:
                    print ( "Unexpected error in input1_listener: %s" % ex )

        # define behavior for halting the application
        def stdin_listener():
            while True:
                try:
                    selection = input("Press Q to quit\n")
                    if selection == "Q" or selection == "q":
                        print("Quitting...")
                        break
                except:
                    time.sleep(10)

        # Schedule task for C2D Listener
        listeners = asyncio.gather(input1_listener(module_client))

        print ( "The sample is now waiting for messages. ")

        # Run the stdin listener in the event loop
        loop = asyncio.get_event_loop()
        user_finished = loop.run_in_executor(None, stdin_listener)

        # Wait for user to indicate they are done listening for messages
        await user_finished

        # Cancel listening
        listeners.cancel()

        # Finally, disconnect
        await module_client.disconnect()

    except Exception as e:
        print ( "Unexpected error %s " % e )
        raise

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()

    # If using Python 3.7 or above, you can use following code instead:
    # asyncio.run(main())