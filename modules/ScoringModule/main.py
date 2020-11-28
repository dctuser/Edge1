# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.

import time
import os
import sys
import asyncio
from six.moves import input
import threading
from azure.iot.device.aio import IoTHubModuleClient

import pandas as pd
import pickle
import json
from sklearn.metrics import accuracy_score
from azureml.core.model import Model
import joblib
import numpy as np
from numpy import mean, sqrt, square
import datetime

MESSAGE_BLOCK_COUNTER = 0

variables = ['CNCServoAxis01ServoAdjustmentRealCurrent']  # Variables to parse from the data received
var_for_MLprediction =0   # User to choose which variable to calculate the mean value, current ML model use 'current', variables[2]
# var_for_energy=[0,1]    # Energy variables
feature_flag = "RMS" #["Mean","RMS"]   # Choose feature as input of ML model to predict the tool condition


async def main():
    try:
        # startTime0 = time.time()
        if not sys.version >= "3.5.3":
            raise Exception("The sample requires python 3.5.3+. Current version of Python: %s" % sys.version)
        print("IoT Hub Client for Python")

        # The client object is used to interact with your Azure IoT hub.
        module_client = IoTHubModuleClient.create_from_edge_environment()

        # connect the client.
        await module_client.connect()
        # startTime00 = time.time()

        # define behavior for receiving an input message on input1
        async def input1_listener(module_client):
            global MESSAGE_BLOCK_COUNTER
            # print("@@@@@@@@@@ startTime00-startTime0 = ", startTime00-startTime0)

            while True:
                try:
                    # read in data
                    # startTime1 = time.time()
                    # print("@@@@@@@@@Start Time: ", startTime)
                    input_message = await module_client.receive_message_on_input("input21")  # blocking call
                    # startTime11 = time.time()
                    print("    The data in the message received on input12 from output 21 is: ")
                    print("==================================================================")
                    print("Real data received: Input_message.data = ", input_message.data)
                    # print("    Custom properties = ")88888888
                    # print(input_message.custom_properties)88888
                    # print('Come here lian - 1')9999999
                    # data = input_message.data
                    # Define X_test for testing

                    # Test - reading original string
                    # input_string = "0|1583482895.193000|[59, 1, 0, 1, 0, -2, 0, 0, 0, -9, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]"
                    # split_data = list(input_string.split("|"))
                    # data= split_data[2].replace('[','').replace(']','').split(',')[3]
                    # print('    Data simulated for testing: df_data=', data)

                    # Test similated data
                    #data = b'[{"Mean_3": -0.043263731,"SMRA_3":	0.016168896, "RMS_3": -0.064318024, "Peak_3":	0.809310229, "Max_3": 0.809310229, "Min_3":	0.447200177, "MarginFactor_5":	0.651888273, "Skewness_5":	-0.217622452, "CrestFactor_5":0.557668396, "ImpulseFactor_5":0.620776019}]'
                    #df_data = pd.read_json(data)
                    #print('    Data simulated for testing: df_data=', df_data)

                    # Test a batch of data from csv
                    # df_data = pd.read_csv("rawdatatest2.csv")
                    # print("df_data=",df_data['Data'])

                    # Parse data into format: [[value1, value2],[value1,value2,value3],[value1,value2,value3]]
                    # And form data into dataframe
                    message = input_message.data
                    message_text = message.decode('utf-8')
                    data_list = json.loads(message_text)

                    value_extracted=[]
                    measureTime_extracted=[]
                    i=0
                    # if (data_list is not 'null'):
                        # print('JJJJJJJ came here lian 1')
                    # startTime2 = time.time()
                    print('JJJJJJJ came here lian 1')
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
                        i=i+1
                    # startTime3 = time.time()
                    print('-----------------value_extracted=',value_extracted)
                    print('-----------------measureTime_extracted=',measureTime_extracted)

                    # Forward energy data to IoT hub directly
                    print('JJJJJJJ came here lian 2')
                    # output22_ = {}
                    # # output_var = [var_for_energy,var_for_MLprediction]
                    # var_for_energy.append(var_for_MLprediction)
                    # for k in var_for_energy:
                    #     # output22[variables[k]]={'Value': value_extracted[k], 'MeasureTime': measureTime_extracted[k]}
                    #     if variables[k]=='CNCServoAxis01ServoAdjustmentRealCurrent':
                    #         # Calculate root mean square value
                    #         value_temp = np.array(value_extracted[k])
                    #         rms_axis01 = np.sqrt((value_temp**2).mean())
                    #         output22_[variables[k]] = {'RMS': rms_axis01,'AvgValue': np.mean(value_extracted[k])}
                    #     elif variables[k]=='CNCServoApparentEnergyDelivered':
                    #         output11_[variables[k]] = {'AvgValue': np.mean(value_extracted[k])}
                    #     elif variables[k]=='CNCServoActiveEnergyDelivered':
                    #         output11_[variables[k]] = {'RMS': np.sqrt((np.array(value_extracted[k])**2).mean())}
                    # output22 = {
                    #     'EnergyData':{
                    #         'AvgValue_CNCServoApparentEnergyDelivered': output22_['CNCServoApparentEnergyDelivered']['AvgValue'],
                    #         'RMS_CNCServoActiveEnergyDelivered': output22_['CNCServoActiveEnergyDelivered']['RMS']
                    #         },
                    #         'CNCServoAxis01ServoAdjustmentRealCurrent':{
                    #             'RMS': output22_['CNCServoAxis01ServoAdjustmentRealCurrent']['RMS'],
                    #             'AvgValue': output22_['CNCServoAxis01ServoAdjustmentRealCurrent']['AvgValue']
                    #             }
                    #         }
                    # print('-----------------output22=',output22)
                    # # print("@@@@@@@@@@ startTime1 = ", startTime1)
                    # # print("@@@@@@@@@@ startTime11 = ", startTime11)
                    # # print("@@@@@@@@@@ startTime2-startTime1 = ", startTime2-startTime1)
                    # # print("@@@@@@@@@@ startTime2-startTime11 = ", startTime2-startTime11)
                    # # print("@@@@@@@@@@ startTime11-startTime1 = ", startTime11-startTime1)

                    # await module_client.send_message_to_output(json.dumps(output22), "output22")
                    # print('00000000000000000000000000come here-test1')


                    # --------ML prediction------------#
                    # Load in input data
                    values = value_extracted[var_for_MLprediction]
                    timestamps = measureTime_extracted[var_for_MLprediction]

                    # datetimeFormat = '%Y-%m-%dT%H:%M:%S.%fZ'
                    # diff_time = datetime.datetime.strptime(measureTime_extracted[var_for_MLprediction][-1], datetimeFormat) - datetime.datetime.strptime(measureTime_extracted[var_for_MLprediction][0], datetimeFormat)
                    # diff_time_sec = diff_time.days * 24 * 60 * 60 * 60 + diff_time.seconds + diff_time.microseconds / 1000000
                    # print('00000000000000000000000000come here-test11')
                    df_data_raw = pd.DataFrame(values,columns=['Data'])
                    print('JJJJJJJ came here lian 3')
                    
                    # print('----------------df_data=',df_data)
                    #testing data
                    # df_data = pd.read_csv("rawdatatest4.csv")  # can be commented after testing.
                    # print('00000000000000000000000000come here-test2')
                    print('df_data_raw=',df_data_raw)
                    print('measureTime_extracted=',timestamps)
                    print('Come here lian - 4')

                    if df_data_raw.empty:
                        pred_result0 = -1   # no data received
                    else:
                        # remove peaks from raw data
                        ind = df_data_raw[(df_data_raw['Data']>48) & (df_data_raw['Data']<65)].index.tolist()
                        df_data = df_data_raw['Data'][ind]
                        print('Come here lian - 5')

                        print('df_data=', df_data)
                        print('Come here lian - 6')

                        # Calculate features for ML prediction
                        if feature_flag =="Mean":
                            data_feature = df_data['Data'].mean(skipna=True).astype(float)
                            data = {"Mean": data_feature}
                            print(data)
                        elif feature_flag == "RMS":
                            value_temp = np.array(df_data)
                            rms_axis01 = np.sqrt((value_temp**2).mean())
                            data_feature = rms_axis01
                            data = {"RMS": data_feature}

                        MESSAGE_BLOCK_COUNTER = MESSAGE_BLOCK_COUNTER + 1
                        print('Come here lian - 7')

                        X_test = data_feature
                        print("data_feature",data_feature)

                        if feature_flag =="Mean":
                            saved_model = pickle.load(open('servocurrentaxis01mean.pkl','rb'))
                        elif feature_flag == "RMS":
                            saved_model = pickle.load(open('servocurrentaxis01rms.pkl','rb'))

                        print('Come here lian - 8')

                        pred_result = saved_model.predict(X_test.reshape(1, -1))[0]
                        # print('00000000000000000000000000come here-test7')
                        # print('    pred_result=', pred_result)
                        print('Come here lian - 9')
                        if pred_result=='Good Condition':
                            pred_result0 = 0
                        elif pred_result=='Bad Condition':
                            pred_result0 = 1

                    # output21 = {'Feature': data, "Output_pred": pred_result0}
                    output21 = {"Output_pred": pred_result0}
                    # print('00000000000000000000000000come here-test8')
                    print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
                    print("Prediction Input and Output:", output21)
                    print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
                    # startTime5 = time.time()
                    # print("@@@@@@@@@@ startTime2-startTime1 = ", startTime2-startTime1)
                    # print("@@@@@@@@@@ startTime3-startTime2 = ", startTime3-startTime2)
                    # print("@@@@@@@@@@ startTime4-startTime3 = ", startTime4-startTime3)
                    # print("@@@@@@@@@@ startTime5-startTime4 = ", startTime5-startTime4)

                    # Output prediction results and send to IoT edge Hub
                    print("----------Forwarding mesage to output 21")
                    await module_client.send_message_to_output(json.dumps(output21), "output21")
                    print('Come here lian - 10')
                except Exception as e:
                    print("Unexpected error: %s " % e)

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

        print("The sample is now waiting for messages. ")

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
        print("Unexpected error %s " % e)
        raise

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()

    # If using Python 3.7 or above, you can use following code instead:
    # asyncio.run(main())