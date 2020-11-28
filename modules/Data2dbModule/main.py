# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.

import time
import datetime
import os
import sys
import asyncio
from six.moves import input
import threading
from azure.iot.device.aio import IoTHubModuleClient
import pandas as pd
import json
import pyodbc

import numpy as np

# -----------------Setting parameters---------------#
# Parameters of database-lian
server_name = 'sql'
database_name = 'KBMmeasureDB'
cnc_table_name = 'dbo.CNCMachineData'
cnc_plc_table_name = 'dbo.CNCMachineCurrData'
beam_table_name = 'dbo.BeAmMachineData'
# beam_lmd_table_name = 'dbo.BeAmMachineLMDData'

username = 'SA'
password = 'Strong!Passw0rd'

cnc_table_columns = ['Variable', 'Value', 'MeasureTime']
beam_table_columns = ['Variable', 'Value', 'MeasureTime']
node_id = ['ns=2;s=EdgeKepware_NTX1000.NTX1000.Siemens.EnergyMeter.ApparentEnergyDelivered',
            'ns=2;s=EdgeKepware_NTX1000.NTX1000.Siemens.EnergyMeter.ActiveEnergyDelivered',
            'ns=2;s=EdgeKepware_NTX1000.NTX1000.CNCSystem.CNCServoList.CNCServo.Axis_01.ServoAdjustmentRealCurrent',
            'ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.MoisturePPM',
            'ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.OxgenPercentage',
            'ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.OxgenPPM']

variables = [
    'CNCServoApparentEnergyDelivered',
    'CNCServoActiveEnergyDelivered',
    'CNCServoAxis01ServoAdjustmentRealCurrent',
    'MoisturePPM',
    'OxgenPercentage',
    'OxgenPPM']

var_for_MLprediction =2   # User to choose which variable to calculate the mean value, current ML model use 'current', variables[2]
var_for_energy=[0,1]      # Energy variables
feature_flag = "RMS"      #["Mean","RMS"]   # Choose feature as input of ML model to predict the tool condition

# batch_size_2_write_db = 58
# DATA_COUNTER_in_BATCH = 0                     #Sub-counter for data records received in a batch
RECEIVED_MESSAGES_COUNTER=0          # Counter for counting how many messages has been received
# N = 100
# MAX_LEN_DB = N*batch_size_2_write_db              # Maximum number of records allowed in DB
# LEN_COUNTER_DB = 0                                # Counter for whole length of data records in DB
# PERCENT_DEL = 0.8
CNC_BUFFER_TIME = 29                                 # Buffer 20 sec to send data into CNCscoring module
BEAM_BUFFER_TIME = 60                                # Buffer 1 min time data and send to BeamScoring module
BEAM_LMD_BUFFER_TIME = 30                                 # Buffer 30 sec time data and send to LMD scoring module
id = 0          # id to record the global label of the message, different from LEN_COUNTER_DB,RECEIVED_MESSAGES_COUNTER
count_servoCurrent_Axis01=0
count_servoCurrent_Axis04=0
count_moisturePPM=0
count_oxygenPercentage=0
count_oxygenPPM=0

PEAK_POS_THR = 65
PEAK_NEG_THR = 48
START_SAVE_DATA_FLAG = 0

print('Setting parameters of database completed-lian.')




async def main(conn,cursor):

    try:
        if not sys.version >= "3.5.3":
            raise Exception( "The sample requires python 3.5.3+. Current version of Python: %s" % sys.version )
        print ( "IoT Hub Client for Python" )

        # # Clear all data from plc_table
        # query_str = "DELETE FROM KBMmeasureDB."+ table_name
        # print('query_str=',query_str)
        # cursor.execute(query_str)
        # print('JJJJJJJ came here lian 0.1: clear all data from plc_table')

        # The client object is used to interact with your Azure IoT hub.
        module_client = IoTHubModuleClient.create_from_edge_environment()

        # Connect the client.
        await module_client.connect()

        # define behavior for receiving an input message on input11
        async def input1_listener(module_client, conn, cursor):
            global RECEIVED_MESSAGES_COUNTER
            # global LEN_COUNTER_DB
            # global MAX_LEN_DB
            global id
            # global DATA_COUNTER_in_BATCH
            global CNC_BUFFER_TIME
            global BEAM_BUFFER_TIME
            global count_servoCurrent_Axis01
            global count_servoCurrent_Axis04
            global count_moisturePPM
            global count_oxygenPercentage
            global count_oxygenPPM

            global PEAK_POS_THR         # Positive peak level to indicate to restart to store data in database
            global PEAK_NEG_THR         # Negtive peak level to indicate to restart to store data in database
            global START_SAVE_DATA_FLAG # Flag to delete the data in cnc_current_table_name

            cnc_start_time = time.time()
            beam_start_time=time.time()
            # beam_lmd_start_time=time.time()

            while True:
                try:
                    
                    input_message = await module_client.receive_message_on_input("input11")  # blocking call
                    print("      input_message=",input_message)
                    message = input_message.data

                    #------------
                    # print("    message_type:",type(message))
                    # message_text1 = message.decode('utf-8')
                    # json_data1 = json.loads(message_text1)
                    # print("    json_data1=", json_data1)
                    # df_data = pd.read_json(message)
                    # print('    df_data=',df_data)

                    #------------
                    # size = len(message)
                    message_text = message.decode('utf-8')
                    # print ( "    Data: <<<%s>>> & Size=%d" % (message_text, size) )# 8888888888888
                    # custom_properties = input_message.custom_properties
                    # print ( "    Properties: %s" % custom_properties )#88888888888
                    RECEIVED_MESSAGES_COUNTER = RECEIVED_MESSAGES_COUNTER + 1     # Count number of messages received in a batch
                    # LEN_COUNTER_DB = LEN_COUNTER_DB + 1                           # Count total number of messages received in max allowed length
                    id = id + 1

                    # message_text = [
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.Siemens.EnergyMeter.ActiveEnergyDelivered",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 11553.603,
                    #             "SourceTimestamp": "2020-09-08T16:13:16.9617014Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.Siemens.EnergyMeter.ApparentEnergyDelivered",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 20136.984,
                    #             "SourceTimestamp": "2020-09-08T16:13:16.9617014Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.Siemens.EnergyMeter.ActiveEnergyDelivered",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 11553.611,
                    #             "SourceTimestamp": "2020-09-08T16:13:22.902749Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.Siemens.EnergyMeter.ApparentEnergyDelivered",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 20136.998,
                    #             "SourceTimestamp": "2020-09-08T16:13:22.902749Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.CNCSystem.CNCServoList.CNCServo.Axis_01.ServoAdjustmentRealCurrent",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 48,
                    #             "SourceTimestamp": "2020-09-08T16:13:22.902749Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.CNCSystem.CNCServoList.CNCServo.Axis_01.ServoAdjustmentRealCurrent",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 53,
                    #             "SourceTimestamp": "2020-09-08T16:13:23.902749Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.CNCSystem.CNCServoList.CNCServo.Axis_01.ServoAdjustmentRealCurrent",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 55,
                    #             "SourceTimestamp": "2020-09-08T16:13:24.902749Z"
                    #         }
                    #     },
                    #                             {
                    #         "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.CNCSystem.CNCServoList.CNCServo.Axis_01.ServoAdjustmentRealCurrent",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 50,
                    #             "SourceTimestamp": "2020-09-08T16:13:25.902749Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.OxgenPercentage",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 22.18251859436035,
                    #             "SourceTimestamp": "2020-10-08T18:31:06.383Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.OxgenPercentage",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 23.17241859436035,
                    #             "SourceTimestamp": "2020-10-08T18:31:12.079Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.OxgenPercentage",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 22.178747177124023,
                    #             "SourceTimestamp": "2020-10-08T18:31:13.745Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.OxgenPPM",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 1800,
                    #             "SourceTimestamp": "2020-10-08T18:31:13.745Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.OxgenPPM",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 1900,
                    #             "SourceTimestamp": "2020-10-08T18:31:13.745Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.OxgenPPM",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 1950,
                    #             "SourceTimestamp": "2020-10-08T18:31:13.745Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.MoisturePPM",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 2500,
                    #             "SourceTimestamp": "2020-10-08T18:31:13.745Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.MoisturePPM",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 2530.5,
                    #             "SourceTimestamp": "2020-10-08T18:31:13.945Z"
                    #         }
                    #     }
                    #     ]
                    # # # testing data
                    # if RECEIVED_MESSAGES_COUNTER==2:
                    #     message_text = [
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.Siemens.EnergyMeter.ActiveEnergyDelivered",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 11553.603,
                    #             "SourceTimestamp": "2020-09-08T16:13:16.9617014Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.Siemens.EnergyMeter.ApparentEnergyDelivered",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 20136.984,
                    #             "SourceTimestamp": "2020-09-08T16:13:16.9617014Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.Siemens.EnergyMeter.ActiveEnergyDelivered",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 11553.611,
                    #             "SourceTimestamp": "2020-09-08T16:13:22.902749Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.Siemens.EnergyMeter.ApparentEnergyDelivered",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 20136.998,
                    #             "SourceTimestamp": "2020-09-08T16:13:22.902749Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.CNCSystem.CNCServoList.CNCServo.Axis_01.ServoAdjustmentRealCurrent",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 48,
                    #             "SourceTimestamp": "2020-09-08T16:13:22.902749Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.CNCSystem.CNCServoList.CNCServo.Axis_01.ServoAdjustmentRealCurrent",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 53,
                    #             "SourceTimestamp": "2020-09-08T16:13:23.902749Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.CNCSystem.CNCServoList.CNCServo.Axis_01.ServoAdjustmentRealCurrent",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 55,
                    #             "SourceTimestamp": "2020-09-08T16:13:24.902749Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.CNCSystem.CNCServoList.CNCServo.Axis_01.ServoAdjustmentRealCurrent",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 80,
                    #             "SourceTimestamp": "2020-09-08T16:13:25.902749Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.OxgenPercentage",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 22.18251859436035,
                    #             "SourceTimestamp": "2020-10-08T18:31:06.383Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.OxgenPercentage",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 23.17241859436035,
                    #             "SourceTimestamp": "2020-10-08T18:31:12.079Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.OxgenPercentage",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 22.178747177124023,
                    #             "SourceTimestamp": "2020-10-08T18:31:13.745Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.OxgenPPM",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 1800,
                    #             "SourceTimestamp": "2020-10-08T18:31:13.745Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.OxgenPPM",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 1900,
                    #             "SourceTimestamp": "2020-10-08T18:31:13.745Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.OxgenPPM",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 1950,
                    #             "SourceTimestamp": "2020-10-08T18:31:13.745Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.MoisturePPM",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 2500,
                    #             "SourceTimestamp": "2020-10-08T18:31:13.745Z"
                    #         }
                    #     },
                    #     {
                    #         "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.MoisturePPM",
                    #         "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #         "Value": {
                    #             "Value": 2530.5,
                    #             "SourceTimestamp": "2020-10-08T18:31:13.945Z"
                    #         }
                    #     }
                    #     ]
                    # elif RECEIVED_MESSAGES_COUNTER==3 or RECEIVED_MESSAGES_COUNTER==4:
                    #     message_text = [
                    #         {
                    #             "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.Siemens.EnergyMeter.ActiveEnergyDelivered",
                    #             "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #             "Value": {
                    #                 "Value": 11553.603,
                    #                 "SourceTimestamp": "2020-09-08T16:13:16.9617014Z"
                    #             }
                    #         },
                    #         {
                    #             "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.Siemens.EnergyMeter.ApparentEnergyDelivered",
                    #             "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #             "Value": {
                    #                 "Value": 20136.984,
                    #                 "SourceTimestamp": "2020-09-08T16:13:16.9617014Z"
                    #             }
                    #         },
                    #         {
                    #             "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.Siemens.EnergyMeter.ActiveEnergyDelivered",
                    #             "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #             "Value": {
                    #                 "Value": 11553.611,
                    #                 "SourceTimestamp": "2020-09-08T16:13:22.902749Z"
                    #             }
                    #         },
                    #         {
                    #             "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.Siemens.EnergyMeter.ApparentEnergyDelivered",
                    #             "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #             "Value": {
                    #                 "Value": 20136.998,
                    #                 "SourceTimestamp": "2020-09-08T16:13:22.902749Z"
                    #             }
                    #         },
                    #         {
                    #             "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.CNCSystem.CNCServoList.CNCServo.Axis_01.ServoAdjustmentRealCurrent",
                    #             "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #             "Value": {
                    #                 "Value": 48,
                    #                 "SourceTimestamp": "2020-09-08T16:13:22.902749Z"
                    #             }
                    #         },
                    #         {
                    #             "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.CNCSystem.CNCServoList.CNCServo.Axis_01.ServoAdjustmentRealCurrent",
                    #             "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #             "Value": {
                    #                 "Value": 53,
                    #                 "SourceTimestamp": "2020-09-08T16:13:23.902749Z"
                    #             }
                    #         },
                    #         {
                    #             "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.CNCSystem.CNCServoList.CNCServo.Axis_01.ServoAdjustmentRealCurrent",
                    #             "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #             "Value": {
                    #                 "Value": 68,
                    #                 "SourceTimestamp": "2020-09-08T16:13:24.902749Z"
                    #             }
                    #         },
                    #                                 {
                    #             "NodeId": "ns=2;s=EdgeKepware_NTX1000.NTX1000.CNCSystem.CNCServoList.CNCServo.Axis_01.ServoAdjustmentRealCurrent",
                    #             "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #             "Value": {
                    #                 "Value": 80,
                    #                 "SourceTimestamp": "2020-09-08T16:13:25.902749Z"
                    #             }
                    #         },
                    #         {
                    #             "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.OxgenPercentage",
                    #             "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #             "Value": {
                    #                 "Value": 22.18251859436035,
                    #                 "SourceTimestamp": "2020-10-08T18:31:06.383Z"
                    #             }
                    #         },
                    #         {
                    #             "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.OxgenPercentage",
                    #             "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #             "Value": {
                    #                 "Value": 23.17241859436035,
                    #                 "SourceTimestamp": "2020-10-08T18:31:12.079Z"
                    #             }
                    #         },
                    #         {
                    #             "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.OxgenPercentage",
                    #             "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #             "Value": {
                    #                 "Value": 22.178747177124023,
                    #                 "SourceTimestamp": "2020-10-08T18:31:13.745Z"
                    #             }
                    #         },
                    #         {
                    #             "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.OxgenPPM",
                    #             "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #             "Value": {
                    #                 "Value": 1800,
                    #                 "SourceTimestamp": "2020-10-08T18:31:13.745Z"
                    #             }
                    #         },
                    #         {
                    #             "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.OxgenPPM",
                    #             "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #             "Value": {
                    #                 "Value": 1900,
                    #                 "SourceTimestamp": "2020-10-08T18:31:13.745Z"
                    #             }
                    #         },
                    #         {
                    #             "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.OxgenPPM",
                    #             "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #             "Value": {
                    #                 "Value": 1950,
                    #                 "SourceTimestamp": "2020-10-08T18:31:13.745Z"
                    #             }
                    #         },
                    #         {
                    #             "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.MoisturePPM",
                    #             "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #             "Value": {
                    #                 "Value": 2500,
                    #                 "SourceTimestamp": "2020-10-08T18:31:13.745Z"
                    #             }
                    #         },
                    #         {
                    #             "NodeId": "ns=2;s=EdgeKepware_BeAM.BeAM.LMDSystem.LMDEnclosureStatus.MoisturePPM",
                    #             "ApplicationUri": "urn:KBM-THGWORX-3.modelfactory.local:Kepware.KEPServerEX.V6:UA%20Server",
                    #             "Value": {
                    #                 "Value": 2530.5,
                    #                 "SourceTimestamp": "2020-10-08T18:31:13.945Z"
                    #             }
                    #         }
                    #     ]


                    data_list = json.loads(message_text)  # when not testing, use this one!!!!
                    # data_list = message_text # when testing, use this one!!!

                    print('JJJJJJJ came here lian 0')#8888888888

                    # Write received message into database
                    if (data_list is not 'null'):
                        # print('JJJJJJJ came here lian 1')
                        for data in data_list:
                            # print('==============data:', data)
                            # print('==============node_id[0]=',node_id[0])
                            if data['NodeId'] == node_id[0]:
                                variableName = variables[0]
                                query_str = "INSERT INTO " + database_name + "." + cnc_table_name + "(" + ",".join(cnc_table_columns) + ")" + "  VALUES (\'" + variableName +'\', ' + str(data['Value']['Value'])+', \''+ str(data['Value']['SourceTimestamp']) +"\')"
                                # print(query_str)
                                # print('come here lian 1')
                                cursor.execute(query_str)

                            if data['NodeId'] == node_id[1]:
                                variableName = variables[1]
                                query_str = "INSERT INTO " + database_name + "." + cnc_table_name + "(" + ",".join(cnc_table_columns) + ")" + "  VALUES (\'" + variableName +'\', ' + str(data['Value']['Value'])+', \''+ str(data['Value']['SourceTimestamp']) +"\')"
                                # print(query_str)
                                # print('come here lian 2')
                                cursor.execute(query_str)
                                # print('come here lian 22')
                            if data['NodeId'] == node_id[2]:
                                variableName = variables[2]
                                count_servoCurrent_Axis01 = count_servoCurrent_Axis01 + 1
                                print('Number of ServoCurrent_Axis01 variable received: ', count_servoCurrent_Axis01)
                                print('ServoCurrent_Axis01 variable value received: ', data['Value']['Value'])
                                print('ServoCurrent_Axis01 variable measureTime: ', data['Value']['SourceTimestamp'])
                                query_str = "INSERT INTO " + database_name + "." + cnc_table_name + "(" + ",".join(cnc_table_columns) + ")" + "  VALUES (\'" + variableName + '\', ' + str(data['Value']['Value']) + ', \'' + str(data['Value']['SourceTimestamp']) + "\')"
                                cursor.execute(query_str)

                                # Only start to store to CNCMachineCurrData table when value value > PEAK_NEG_THR
                                if data['Value']['Value'] > PEAK_POS_THR:
                                    START_SAVE_DATA_FLAG = 1

                                if START_SAVE_DATA_FLAG == 1:
                                    #---------------------------------------------
                                    # Output data in cnc_current_table_name
                                    query_str = "Select * FROM " + database_name + "." + cnc_plc_table_name
                                    print('------------query_str=',query_str)
                                    sql_query_df = pd.read_sql_query(query_str,conn)
                                    print('------------came here lian 5')
                                    json_output = json.loads(sql_query_df.to_json(orient='records'))
                                    output13 = json_output
                                    # OUTPUT data received in 1 sec to scoring module
                                    print('777777777777777777777output13(json):',output13)
                                    await module_client.send_message_to_output(json.dumps(output13), "output13")

                                    # Delete data in cnc_current_table_name
                                    query_str = "DELETE FROM " + database_name + "." + cnc_plc_table_name
                                    print('query_str=',query_str)
                                    cursor.execute(query_str)
                                    START_SAVE_DATA_FLAG = 0

                                query_str = "INSERT INTO " + database_name + "." + cnc_plc_table_name + "(" + ",".join(cnc_table_columns) + ")" + "  VALUES (\'" + variableName + '\', ' + str(data['Value']['Value']) + ', \'' + str(data['Value']['SourceTimestamp']) + "\')"
                                cursor.execute(query_str)
                                    
                            if data['NodeId'] == node_id[3]:
                                variableName = variables[3]
                                count_moisturePPM=count_moisturePPM + 1
                                print('Number of moisturePPM variable received: ', count_moisturePPM)
                                print('moisturePPM variable value received: ', data['Value']['Value'])
                                print('moisturePPM variable measureTime: ', data['Value']['SourceTimestamp'])
                                query_str = "INSERT INTO " + database_name + "." + beam_table_name + "(" + ",".join(beam_table_columns) + ")" + "  VALUES (\'" + variableName + '\', ' + str(data['Value']['Value']) + ', \'' + str(data['Value']['SourceTimestamp']) + "\')"
                                cursor.execute(query_str)
                                # query_str2 = "INSERT INTO " + database_name + "." + beam_lmd_table_name + "(" + ",".join(beam_table_columns) + ")" + "  VALUES (\'" + variableName + '\', ' + str(data['Value']['Value']) + ', \'' + str(data['Value']['SourceTimestamp']) + "\')"
                                # cursor.execute(query_str2)
                                
                            if data['NodeId'] == node_id[4]:
                                variableName = variables[4]
                                count_oxygenPercentage=count_oxygenPercentage+1
                                print('Number of oxygenPercentage variable received: ', count_oxygenPercentage)
                                print('oxygenPercentage variable value received: ', data['Value']['Value'])
                                print('oxygenPercentage variable measureTime: ', data['Value']['SourceTimestamp'])
                                query_str = "INSERT INTO " + database_name + "." + beam_table_name + "(" + ",".join(beam_table_columns) + ")" + "  VALUES (\'" + variableName + '\', ' + str(data['Value']['Value']) + ', \'' + str(data['Value']['SourceTimestamp']) + "\')"
                                cursor.execute(query_str)
                                # query_str2 = "INSERT INTO " + database_name + "." + beam_lmd_table_name + "(" + ",".join(beam_table_columns) + ")" + "  VALUES (\'" + variableName + '\', ' + str(data['Value']['Value']) + ', \'' + str(data['Value']['SourceTimestamp']) + "\')"
                                # cursor.execute(query_str2)

                            if data['NodeId'] == node_id[5]:
                                variableName = variables[5]
                                count_oxygenPPM=count_oxygenPPM+1
                                print('Number of oxygenPPM variable received: ', count_oxygenPPM)
                                print('oxygenPPM variable value received: ', data['Value']['Value'])
                                print('oxygenPPM variable measureTime: ', data['Value']['SourceTimestamp'])
                                query_str = "INSERT INTO " + database_name + "." + beam_table_name + "(" + ",".join(beam_table_columns) + ")" + "  VALUES (\'" + variableName + '\', ' + str(data['Value']['Value']) + ', \'' + str(data['Value']['SourceTimestamp']) + "\')"
                                cursor.execute(query_str)
                                # query_str2 = "INSERT INTO " + database_name + "." + beam_lmd_table_name + "(" + ",".join(beam_table_columns) + ")" + "  VALUES (\'" + variableName + '\', ' + str(data['Value']['Value']) + ', \'' + str(data['Value']['SourceTimestamp']) + "\')"
                                # cursor.execute(query_str2)

                    # 88888888888888 Read back data from CNC table in DB 88888888888888888#
                    # Check whether it is time to read back last N records of data from CNC data table and pass to scoring module for processing
                    print("-------------Wait time: ", time.time()-cnc_start_time)
                    print("-------------Defined buffer time: ", CNC_BUFFER_TIME)
                    if (time.time()-cnc_start_time)>=CNC_BUFFER_TIME:
                        print('------------came here lian 4')
                        query_str = "Select * FROM " + database_name + "." + cnc_table_name
                        print('------------query_str=',query_str)
                        sql_query_df = pd.read_sql_query(query_str,conn)
                        print('------------came here lian 5')
                        json_output = json.loads(sql_query_df.to_json(orient='records'))
                        data_list_energy=json_output
                        #==========================================================
                        # output energy data to IoT hub directly
                        value_extracted=[]
                        measureTime_extracted=[]
                        i=0
                        # startTime2 = time.time()
                        print('------------came here lian 6')
                        for j in range(0,len(variables)):
                            value_extracted.append([])
                            measureTime_extracted.append([])
                            for data in data_list_energy:
                                # print('==============data:', data)
                                if data['Variable'] == variables[j]:
                                    # print('============data[value]=',data['Value'])
                                    value_extracted[j].append(data['Value'])
                                    measureTime_extracted[j].append(data['MeasureTime'])
                                    # print('come here 23')
                            i=i+1
                        # startTime3 = time.time()
                        print('-----------------value_extracted=',value_extracted)
                        print('-----------------measureTime_extracted=',measureTime_extracted)
                        print('------------came here lian 7')

                        # Forward energy data to IoT hub directly
                        output11_ = {}
                        # output_var = [var_for_energy,var_for_MLprediction]
                        var_for_energy.append(var_for_MLprediction)
                        print('------------came here lian 8')
                        for k in var_for_energy:
                            # output22[variables[k]]={'Value': value_extracted[k], 'MeasureTime': measureTime_extracted[k]}
                            if variables[k]=='CNCServoAxis01ServoAdjustmentRealCurrent':
                                # Calculate root mean square value
                                value_temp = np.array(value_extracted[k])
                                rms_axis01 = np.sqrt((value_temp**2).mean())
                                output11_[variables[k]] = {'RMS': rms_axis01,'AvgValue': np.mean(value_extracted[k])}
                            elif variables[k]=='CNCServoApparentEnergyDelivered':
                                output11_[variables[k]] = {'AvgValue': np.mean(value_extracted[k])}
                            elif variables[k]=='CNCServoActiveEnergyDelivered':
                                output11_[variables[k]] = {'RMS': np.sqrt((np.array(value_extracted[k])**2).mean())}
                        
                        print('------------came here lian 9')
                        print(' output11_=', output11_)
                        output11 = {
                            'EnergyData':{
                                'AvgValue_CNCServoApparentEnergyDelivered': output11_['CNCServoApparentEnergyDelivered']['AvgValue'],
                                'RMS_CNCServoActiveEnergyDelivered': output11_['CNCServoActiveEnergyDelivered']['RMS']
                                },
                                'CNCServoAxis01ServoAdjustmentRealCurrent':{
                                    'RMS': output11_['CNCServoAxis01ServoAdjustmentRealCurrent']['RMS'],
                                    'AvgValue': output11_['CNCServoAxis01ServoAdjustmentRealCurrent']['AvgValue']
                                    }
                                }

                        # print('output11(json):',output11)
                        # OUTPUT data received in 1 sec to scoring module
                        print('777777777777777777777output11(json):',output11)
                        await module_client.send_message_to_output(json.dumps(output11), "output11")
                        print('------------came here lian 10')
                        # output51 = {'show_flag': 1}
                        # await module_client.send_message_to_output(json.dumps(output51), "output51")
                        print("Come here===========================================================")

                        #88888888888888888 Delete data inside CNC table from DB 888888888888888888#
                        # print('------------came here lian 6')
                        query_str = "DELETE FROM " + database_name + "." + cnc_table_name
                        # print('query_str=',query_str)
                        cursor.execute(query_str)
                        # print('------------came here lian 7')
                        cnc_start_time = time.time()

                    # 88888888888888 Read back data from BEAM table in DB 88888888888888888#
                    if (time.time()-beam_start_time)>=BEAM_BUFFER_TIME:
                        # print('------------came here lian 4')
                        query_str = "Select * FROM " + database_name + "." + beam_table_name
                        print('------------query_str=',query_str)
                        sql_query_df = pd.read_sql_query(query_str,conn)
                        print('------------came here lian 55')
                        json_output = json.loads(sql_query_df.to_json(orient='records'))
                        output12 = json_output
                        # print('output11(json):',output11)
                        # OUTPUT data received in 1 sec to scoring module
                        print('777777777777777777777output12(json):',output12)
                        await module_client.send_message_to_output(json.dumps(output12), "output12")
                        print("Come here===========================================================")

                        #88888888888888888 Delete data inside CNC table from DB 888888888888888888#
                        # print('------------came here lian 6')
                        query_str = "DELETE FROM " + database_name + "." + beam_table_name
                        # print('query_str=',query_str)
                        cursor.execute(query_str)
                        # print('------------came here lian 7')
                        beam_start_time = time.time()

                    # # 88888888888888 Read back data from BEAM LMD table in DB 88888888888888888#
                    # if (time.time()-beam_lmd_start_time)>=BEAM_LMD_BUFFER_TIME:
                    #     # print('------------came here lian 4')
                    #     query_str = "Select * FROM " + database_name + "." + beam_lmd_table_name
                    #     print('------------query_str=',query_str)
                    #     sql_query_df = pd.read_sql_query(query_str,conn)
                    #     print('------------came here lian 55')
                    #     json_output = json.loads(sql_query_df.to_json(orient='records'))
                    #     output13 = json_output
                    #     # print('output11(json):',output11)
                    #     # OUTPUT data received in 1 sec to scoring module
                    #     print('777777777777777777777output13(json):',output13)
                    #     await module_client.send_message_to_output(json.dumps(output13), "output13")
                    #     print("Come here===========================================================")

                    #     #88888888888888888 Delete data inside CNC table from DB 888888888888888888#
                    #     # print('------------came here lian 6')
                    #     query_str = "DELETE FROM " + database_name + "." + beam_lmd_table_name
                    #     # print('query_str=',query_str)
                    #     cursor.execute(query_str)
                    #     # print('------------came here lian 7')
                    #     beam_lmd_start_time = time.time()
                except Exception as ex:
                    print ( "Unexpected error in input1_listener: %s" % ex )
        # Define behavior for halting the application
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
        listeners = asyncio.gather(input1_listener(module_client,conn,cursor))

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
    #------------------Connect to DB------------------#
    try:
        # conn_string = 'Driver={ODBC Driver 17 for SQL Server}; Server=' + server_name + '; Database=' + database_name + ';UID=' + username + ';PWD=' + password
        #print('conn_string',conn_string)
        conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server}; SERVER=' + server_name + '; DATABASE=' + database_name + ';UID=' + username + ';PWD=' + password)
        cursor = conn.cursor()
        print('Connected to database-lian')
        #Sample select query
        cursor.execute("SELECT @@version;")
        # row = cursor.fetchone()
        # while row:
        #     print(row[0])
        #     row = cursor.fetchone()
    except Exception as e:
        print("Failed to connect to db %s " % e)
        raise
    #----------------------Loop-----------------------#
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(conn,cursor))
    loop.close()

    # If using Python 3.7 or above, you can use following code instead:
    # asyncio.run(main())