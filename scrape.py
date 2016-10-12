#!/usr/bin/python3

# testing asyncio

import asyncio
import aiohttp
import sys
import signal
import datetime
import time
import aiomysql
import os
import functools
import aiofiles
from random import randint
import json
from pprint import pprint

url = 'https://cdip.ucsd.edu/data_access/justdar.cdip'
# url = 'http://localhost:63342/async/justdar.cdip.html'

loop = asyncio.get_event_loop()

async def write_lastvalue(filename='/var/scripts/python3/lastvalue.json'):
    print("entering write_lastvalue")
    jsondata = json.dumps(lastvalue)
    try:
        async with aiofiles.open(filename, 'w') as f:
            await f.write(jsondata)
            print('wrote data')
        return True
    except IOError:
        return False

def buoyjson():
    buoystarttime = {}
    jdata = """[
    {"id": 46218,"name": "Harvest","timeZone": -8,"latitude": 34.45,"longitude": -120.78,"cdip": "071","updateTime": 25},
    {"id": 46223,"name": "Dana Point","timeZone": -8,"latitude": 33.46,"longitude": -117.77,"cdip": null,"updateTime": null},
    {"id": 46224,"name": "Oceanside Offshore","timeZone": -8,"latitude": 33.18,"longitude": -117.47,"cdip": "045","updateTime": 24},
    {"id": 46225,"name": "Torrey Pines Outer","timeZone": -8,"latitude": 32.93,"longitude": -117.39,"cdip": 100,"updateTime": 10},
    {"id": 46216,"name": "Goleta Point","timeZone": -8,"latitude": 34.33,"longitude": -119.80,"cdip": 107,"updateTime": 15},
    {"id": 46042,"name": "West Monterey Bay","timeZone": -8,"latitude": 36.75,"longitude": -122.42,"cdip": 185,"updateTime": 24},
    {"id": 46237,"name": "San Francisco Bar","timeZone": -8,"latitude": 37.78,"longitude": -122.60,"cdip": 142,"updateTime": 15},
    {"id": 51201,"name": "Waimea Bay, Oahu","timeZone": -10,"latitude": 21.67,"longitude": -158.12,"cdip": null,"updateTime": 7},
    {"id": 51101,"name": "Northwestern Hawaii","timeZone": -10,"latitude": 24.32,"longitude": -162.06,"cdip": null,"updateTime": null},
    {"id": 41113,"name": "Cape Canaveral","timeZone": -5,"latitude": 28.40,"longitude": -80.53,"cdip": 143,"updateTime": 13},
    {"id": "TPC52","name": "Offshore Baja Sur","timeZone": -7,"latitude": 20.00,"longitude": -117.33,"cdip": null,"updateTime": null},
    {"id": 46232,"name": "Point Loma","timeZone": -8,"latitude": 32.43,"longitude": -117.33,"cdip": 191,"updateTime": 14},
    {"id": 46222,"name": "San Pedro","timeZone": -8,"latitude": 33.62,"longitude": -118.32,"cdip": "092","updateTime": 6},
    {"id": 46231,"name": "Mission Bay","timeZone": -8,"latitude": 32.75,"longitude": -117.50,"cdip": 220,"updateTime": 3},
    {"id": 44095,"name": "Oregon Inlet, NC","timeZone": -5,"latitude": 35.75,"longitude": -75.33,"cdip": 192,"updateTime": 3},
    {"id": 51207,"name": "Kaneohe Bay, Oahu","timeZone": -10,"latitude": 21.47,"longitude": -157.75,"cdip": null,"updateTime": 6},
    {"id": 51204,"name": "Barbers Point, Oahu","timeZone": -10,"latitude": 21.28,"longitude": -158.12,"cdip": null,"updateTime": 6},
    {"id": 41002,"name": "South Hatteras","timeZone": -5,"latitude": 31.86,"longitude": -74.84,"cdip": null,"updateTime": null},
    {"id": 46012,"name": "Half Moon Bay","timeZone": -8,"latitude": 37.36,"longitude": -122.88,"cdip": null,"updateTime": null},
    {"id": "NADI","name": "Fiji","timeZone": 12,"latitude": -18.00,"longitude": 176.25,"cdip": null,"updateTime": null},
    {"id": 62085,"name": "Cadiz, Spain","timeZone": 1,"latitude": 36.48,"longitude": -6.97,"cdip": null,"updateTime": null},
    {"id": "NW-NHC76","name": "Puerto Escondido","timeZone": -6,"latitude": 15.50,"longitude": -98.00,"cdip": null,"updateTime": null},
    {"id": 41114,"name": "Fort Pierce","timeZone": -5,"latitude": 27.55,"longitude": -80.22,"cdip": 134,"updateTime": 27},
    {"id": 41112,"name": "Fernandina Beach","timeZone": -5,"latitude": 30.71,"longitude": -81.29,"cdip": 132,"updateTime": 26},
    {"id": "MIA57","name": "St. Petersburg","timeZone": -5,"latitude": 27.59,"longitude": -82.93,"cdip": 214,"updateTime": 0},
    {"id": "AGULHAS_FA","name": "Jeffrey\'s Bay","timeZone": 2,"latitude": -34.97,"longitude": 22.17,"cdip": null,"updateTime": null},
    {"id": 51206,"name": "Hilo, Hawaii","timeZone": -10,"latitude": 19.78,"longitude": -154.97,"cdip": 188,"updateTime": 3},
    {"id": 46206,"name": "Tofino","timeZone": -8,"latitude": 48.84,"longitude": -125.99,"cdip": null,"updateTime": null},
    {"id": "V14046","name": "Skeleton Bay","timeZone": 1,"latitude": -23.61,"longitude": 13.58,"cdip": null,"updateTime": null},
    {"id": 51205,"name": "Pauwela, Maui","timeZone": -10,"latitude": 21.02,"longitude": -156.42,"cdip": null,"updateTime": 10}]"""
    # pauwela 187, kaneohe 198, barbers 165, waimea 106,

    data = json.loads(jdata)
    for i in data:
        if i['cdip'] is not None:
            buoystarttime[str(i['cdip'])] = i['updateTime']
    return buoystarttime

buoystarttime = buoyjson()

def load_lastvalue(filename='/var/scripts/python3/lastvalue.json'):
    with open(filename) as f:
        return json.load(f)

async def start_buoy(buoystarttime):
    """fire up mysql and get buoy list"""
    query = "SELECT buoy, cdip from buoys where cdip is NOT NULL"  # or something
    print('entering start_buoy')
    return buoyjson() # result

def next_buoy(buoystarttime):
    now = datetime.datetime.now().minute
    if now >= 30:
        now -= 30
    nearesttime = ('',60)
    while True:
        for k,v in buoystarttime.items():
            if v - now > 0 and v - now < nearesttime[1]:
                nearesttime = (k,v)
        yield nearesttime

nb = next(next_buoy(buoystarttime))

async def fetch(url, sess, buoy='191', datatype='xy', st='S01', extras=''):
    print(buoy, 'entering fetch')
    params = ''.join([buoy, '+', datatype, '+', st, extras])
    async with sess.get(url, params=params) as resp:
        return await resp.text()
    # except aiohttp.ClientError as e:
    #     print(e)
    #     # return e
    # except IOError as e:
    #     print(e)
        # return e

async def schedule_fetch(buoy, starttime):
    print(buoy, starttime, "schedule_fetch...")
    await asyncio.sleep(clock_time(starttime, True))
    data = await fetch(url,session, buoy)
    print(data[11:21])
    return data, buoy

async def reschedule_fetch(buoy):
    if buoy in ['165', '106', '188', '187', '134']: # hawaiian buoys generally only report large swells
        f = fib(10)
    else:
        f = fib(30)
    if test_flag:
        f = [1.0/60]
    for t in f: # todo use .as_completed() or ensure_future to avoid blocking
        try:
            timeout = t * 60
            print(timeout)
            await asyncio.sleep(timeout)
            datastr = await fetch(url, session, buoy)
            if len(datastr) > 12:
                data = await process_series(datastr)
                if data['series']:
                    return await output_series(**data)
        except asyncio.TimeoutError as e:
            print(e[1])
        except IOError as eIO:
            print(eIO)
        await asyncio.sleep(0)
        # finally:
        #     return {'series': None, 'buoy': buoy}
    else:
        print("Exhausted attempts to get data for", buoy)
        return await output_series(**{'series': None, 'buoy': buoy})

async def test_half():
    print('entering test_half')
    now_minutes = datetime.datetime.utcnow().minute
    tasks = [schedule_fetch(bs[0],bs[1]) for bs in {('191', now_minutes), ('134', now_minutes-1)}]
    for task in asyncio.as_completed(tasks, timeout=90):
        try:
            datastr, buoy = await task
            print(len(datastr), "^datastr received^")
            if len(datastr) > 12:
                data = await process_series(datastr)
                if data['series'] is None:
                    data = await reschedule_fetch(buoy)
                else:
                    outputSuccess = await output_series(**data)
                    print(outputSuccess)
            else:
                raise TimeoutError
        except TimeoutError as e:
            print('no data for', buoy, e)
            await asyncio.sleep(50)
            # data = await reschedule_fetch(buoy)

            # outputSuccess = await output_series(**data)
            # print(outputSuccess)
        print(data)
        await asyncio.sleep(0)

async def half_hourly():
    print("entering half_hourly")
    tasks = [schedule_fetch(bs[0],bs[1]) for bs in buoystarttime.items()]
    now = datetime.datetime.now()
    print('it`s', now.strftime("%H:%M:%S"))
    for task in asyncio.as_completed(tasks, timeout=1800):
        try:
            datastr, buoy = await task
            print(len(datastr), "^datastr received^")
            if len(datastr) > 12:
                data = await process_series(datastr)
                if data['series'] is None:
                    data = await reschedule_fetch(buoy)
                else:
                    outputSuccess = await output_series(**data)
                    print(outputSuccess)
            else:
                raise TimeoutError
        except TimeoutError as e:
            print('no data for', buoy, e)
            await asyncio.sleep(50)
            # data = await reschedule_fetch(buoy)

            # outputSuccess = await output_series(**data)
            # print(outputSuccess)
        print(data)
        await asyncio.sleep(0)

async def process_series(rawdata):
    """Take data received from url and create 2-d list"""
    global lastvalue
    # todo speed up with numpy
    print('entering process_series ', end="")
    data = rawdata.splitlines()
    buoy = data[1].split()[1][:3]
    print(buoy, end='')
    series = []
    epoch = datetime.datetime.utcfromtimestamp(0)
    i , dataflag = 0, False
    while i < len(data):
        if data[i] == '--------------------------------------------------' or dataflag:
            dataflag = True
            break
        i += 1
    if dataflag:
        raw_timestamp = data[i+1].split()[0]
        if lastvalue[buoy] is None or lastvalue[buoy] != raw_timestamp:  # or test_flag is True:
            # global lastvalue tracks buoys' last update
            lastvalue[buoy] = raw_timestamp
            while i < len(data) - 2:   # avoid last newline and make up for i+1 below (i is always 2 behind row)
                values = data[i + 1].split()
                values[0] = (datetime.datetime.strptime(values[0][0:14], '%Y%m%d%H%M%S') - epoch).total_seconds() * 1000
                values[1:] = [round(.0328 * int(x), 3) for x in values[1:]]
                series.append(values)
                i += 1
        else:
            print(" not updated", lastvalue[buoy], raw_timestamp)
            return {'series': None, 'buoy': buoy}

    print("process_series produced:", lastvalue[buoy], raw_timestamp, series[0][0])
    return {'series': series, 'buoy': buoy}

async def output_series(series, buoy):
    path = "/var/www/js/"

    if series:
        print(series[0], buoy, "entering output_series")
        newdata = ["""[new Date(%s),%s,%s,%s],\n""" % tuple(i) for i in series]
        # print(newdata)
        try:
            async with aiofiles.open(os.path.join(path, buoy + ".js"), 'r') as f:
                olddata = await f.readlines()
                print('old data opened ')
                olddata[:] = olddata[1:-1]
        except IOError as e:
            print('new file', e)
            olddata = []

        dataLength = len(olddata)
        if dataLength > 300:
            starttime = str(series[1][0])
            for i in reversed(olddata[1:-300]):
                if starttime in i:
                    newdata = []
                    break
            output = olddata + newdata
            if len(output) >= (2304 * 4):
                output = output[2304:]
        else:
            output = newdata

        output.insert(0, 'var series = [ \n')
        output.append(']')

        try:
            async with aiofiles.open(os.path.join(path, buoy + ".js"), 'w') as f:
                await f.write(''.join(output))
                print('wrote data ', output[1])
            return True
        except IOError:
            return False
    else:
        return False

def clock_time(buoytime, sleep=False):
    print("entering clock_time ", end='')
    looptime = loop.time()
    now = datetime.datetime.now()
    nexttime = now
    buoytime +=1 # give cdip a minute to update
    if now.minute > buoytime:
        buoytime2 = buoytime + 30
        if now.minute > buoytime2:
            nexttime = nexttime.replace(hour=now.hour+1, minute=buoytime)
        else:
            timeshift = 30 - now.minute + buoytime
            nexttime += datetime.timedelta(minutes=timeshift)
    else:
        nexttime = nexttime.replace(minute=buoytime)
    if sleep:
        max_wait = 15
        if test_flag:
            max_wait = 2
        nexttime += datetime.timedelta(seconds=randint(1,max_wait)) #nexttime.replace(second=randint(1,20))
    print(nexttime)
    diff = nexttime.timestamp() - now.timestamp()
    if sleep:
        return diff
    else:
        return looptime + diff

def nonzerofib(maximum):
    print('nonzerofib')
    a, b = 1, 1
    while a < maximum:
        yield a
        a, b = b, a + b

def fib(maximum):
    print("yields fibonnaci sequence until sequence's sum equals maximum")
    a, b = 1, 1
    c = 0
    while c < maximum:
        yield a
        a, b = b, a + b
        c += a

def signal_handler(signal, frame):
    loop.stop()

# lastvalue = {}.fromkeys(buoystarttime)
lastvalue = load_lastvalue()

#
# session = aiohttp.ClientSession()
# t = asyncio.ensure_future(half_hourly())
# loop.run_forever()
#
# t = asyncio.ensure_future(test_half())
# loop.run_until_complete(t)
test_flag=False
def test(afunction=test_half(), flag=True):

    global test_flag, session
    loop = asyncio.get_event_loop()
    conn = aiohttp.TCPConnector(verify_ssl=False)
    with aiohttp.ClientSession(connector=conn) as session:
        test_flag = flag
        t = asyncio.ensure_future(afunction)
        loop.run_until_complete(t)
        jwrite = asyncio.ensure_future(write_lastvalue())
        loop.run_until_complete(jwrite)
        # return t.result()
        # loop.close()



#
# with aiohttp.ClientSession() as session:
#     # signal.signal(signal.SIGINT, signal_handler)
#
#     # for signame in ('SIGINT', 'SIGTERM'):
#     #     loop.add_signal_handler(getattr(signal, signame), functools.partial(loop.stop(), signame))
#     t = asyncio.ensure_future(half_hourly())
#     loop.run_forever()






##############
#   testing
##############
# async def reschedule(when, buoy):
#     global loop, session
#     loop.call_at(clock_time(when), fetch(url,session,buoy=buoy))
#
#
# async def outer(sess):
#     print('entering outer')
#     tasks = []
#     global buoystarttime
#     buoystarttime = await start_buoy()
#     for b in buoystarttime.keys():
#         task = asyncio.ensure_future(fetch(url, sess, buoy=b))
#         tasks.append(task)
#     # data0 = await asyncio.gather(*tasks,
#     for data0 in asyncio.as_completed(tasks, timeout=60):
#         try:
#             d = await data0
#             if len(d) > 14:
#                 data = d.splitlines()
#                 b = data[1].split()[1][0:3]
#                 rawdate = data[14].split()[-2]
#                 print(b, rawdate)
#                 if check_date(rawdate, b):
#                     series = await process_series(data)
#                     # print(series)
#                     try:
#                         out = asyncio.ensure_future(output_series(series, b))
#                     except asyncio.Error as e:
#                         print(e)
#                 else:
#                     pass
#             else:
#                 print('error fetching URL for {}'.format(b))
#         except (TimeoutError, OSError) as e:
#             print(e)
#
# def check_date(rawdate, buoy):
#     global buoystarttime
#     date = datetime.datetime.strptime(rawdate, '%Y%m%d%H%M%S')
#     if not buoystarttime[buoy] or date - buoystarttime[buoy] + datetime.timedelta(minutes=25) > 0:
#         buoystarttime[buoy] = date
#         return date
#     else:
#         return False
#     # if rawdate != buoystarttime[buoy]:
#     #     return rawdate
#
# async def r():
#     print('r')
#     return 'foo'
