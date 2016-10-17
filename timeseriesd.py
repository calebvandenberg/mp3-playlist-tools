#!/usr/bin/python3

"""
timeseriesd is a polite asynchronous client daemon for retrieving data
 from Scripps Institute's Coastal Data Information Program available at http://cdip.ucsd.edu
Timeseries is constructed by checking for updates approximately every 30 minutes and appending to a JSON-like file
Data is stored 4 cycles (2 hours), allowing for clear visualization online

State is maintained by importing/ exporting to JSON files

Fetching data is scheduled generally every 30 minutes.
If data is not updated, fetch is rescheduled with decreasing frequency.

Output is intended for use with dygraphs.js

"""

import aiofiles
import aiohttp
import asyncio
import datetime
import json
import logging
import os
from random import randint
import signal
# import aiomysql
# import functools
# import warnings


def buoyjson():
    """Buoy data dump from database for testing"""
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


async def write_json(data, filename='/var/scripts/python3/lastvalue.json'):
    l.info("entering write_json")
    jsondata = json.dumps(data)
    try:
        async with aiofiles.open(filename, 'w') as f:
            await f.write(jsondata)
            l.warning('wrote data')
        return True
    except (OSError, IOError):
        return False


def load_json(filename='/var/scripts/python3/lastvalue.json'):
    try:
        with open(filename) as f:
            return json.load(f)
    except (OSError, IOError) as e:
        l.error(e)
        return False

async def fetch(url, sess, buoy='191', datatype='xy', st='S01', extras=''):
    """
        Fetch retrieves raw data
    :param url: http or https url
    :param sess: aiohttp.CLientSession connection
    :param buoy: CDIP's buoy ID
    :params datatype, st, extras: extra args needed to pull data from CDIP
    :return string or None
    """
    l.info(buoy + ' entering fetch')
    params = ''.join([buoy, '+', datatype, '+', st, extras])
    try:
        async with sess.get(url, params=params) as resp:
            return await resp.text()
    except (IOError, aiohttp.ClientError) as e:
        l.error(e)


async def schedule_fetch(buoy, starttime):
    """
        Waits until buoy's update time, then fetches data.
        If successful, sends data to be processed and output, then repeats in 29 minutes
        Otherwise intiiates reschedule_fetch coroutine

    :param buoy: the CDIP ID
    :param starttime: the minute (0-29) when buoy typically updates
    :return: boolean success / fail
    """
    global unresponsive
    l.warning(' '.join([buoy, str(starttime), "schedule_fetch..."]))
    await asyncio.sleep(clock_time(starttime, True))
    raw_data = await fetch(url,session, buoy)
    l.warning(raw_data[11:21])
    if len(raw_data) > 12:
        data = await process_series(raw_data)
        if data['series']:
            asyncio.ensure_future(output_series(**data))
            asyncio.ensure_future(schedule_fetch(buoy, 1740)) # 29 minutes
            if buoy in unresponsive:
                unresponsive.remove(buoy)
            return True
        else:
            asyncio.ensure_future(reschedule_fetch(buoy))
            return False
    else:
        l.warning('no data for ' + buoy)
        asyncio.ensure_future(reschedule_fetch(buoy))
        return False


async def reschedule_fetch(buoy):
    """
        Reschedules a fetch, with decreasing frequency via fibonacci sequence
        Buoys known to be unresponsive are given 2 tries
        If successful, sends data to be processed and output, then repeats in 30 minutes
        If not successful, repeats in 30 + timout minutes
    :param buoy: the CDIP ID
    :return 0
    """
    global unresponsive
    if buoy in unresponsive:
        f = fib_sum(1)
    else:
        f = fib_sum(61)
    # if test_flag:
        # f = [1.0/60]
    for t in f:
        try:
            timeout = t * 60
            l.info(timeout)
            await asyncio.sleep(timeout)
            datastr = await fetch(url, session, buoy)
            if len(datastr) > 12:
                data = await process_series(datastr)
                if data['series']:
                    if buoy in unresponsive:
                        unresponsive.remove(buoy)
                    asyncio.ensure_future(output_series(**data))
                    asyncio.ensure_future(schedule_fetch(buoy, 1800))
        except (asyncio.TimeoutError, IOError) as e:
            l.error(e)
        # finally:
        #     return {'series': None, 'buoy': buoy}
    else:
        l.error("Exhausted attempts to get data for " + buoy)
        if buoy not in unresponsive:
            unresponsive.append(buoy)
        asyncio.ensure_future(schedule_fetch(buoy, 1800 + timeout))


async def process_series(rawdata):
    """
        Take data received from url and create 2-d list
    :param rawdata: string from fetch
    :return dict processed_data {
            'buoy': CDIP id
            'series': list[0:3] or None
             }
    """
    global lastvalue
    # todo speed up with numpy?
    l.info('entering process_series ')
    data = rawdata.splitlines()
    buoy = data[1].split()[1][:3]
    processed_data = {'buoy': buoy, 'series': None}
    # print(processed_data['buoy']
    series = []
    epoch = datetime.datetime.utcfromtimestamp(0)
    i = 0
    while i < len(data):
        if data[i] == '--------------------------------------------------':
            break
        i += 1
    else:
        l.critical('---data not found----')
        return processed_data
    raw_timestamp = data[i+1].split()[0]
    if lastvalue[buoy] is None or lastvalue[buoy] != raw_timestamp:  # or test_flag is True:
        # global lastvalue tracks buoys' last update
        lastvalue[buoy] = raw_timestamp
        while i < len(data) - 2:   # avoid last newline and make up for i+1 below (i is always 2 behind row)
            values = data[i + 1].split()
            values[0] = (datetime.datetime.strptime(values[0][0:14], '%Y%m%d%H%M%S') - epoch).total_seconds() * 1000
            if len(series) > 0 and values[0] == series[-1][0]:
                values[0] += 500
            values[1:] = [round(.0328 * int(x), 3) for x in values[1:]]
            series.append(values)
            i += 1
    else:
        l.warning(' '.join(["^not updated", raw_timestamp]))
        return processed_data

    l.warning("process_series produced:" + str(series[0][0]))
    processed_data['series'] = series
    return processed_data


async def output_series(series, buoy):
    """
        Output to Javascript array (starts with var = []) file for dygraphs plot
    :param series: 2-d list from processed_data
    :param buoy: CDIP id
    :return: boolean succes/fail
    """
    path = "/var/www/js/"

    if series:
        l.info(' '.join([str(series[0]), buoy, "entering output_series"]))
        newdata = ["""[new Date(%s),%s,%s,%s],\n""" % tuple(i) for i in series]
        # l.info(newdata)
        try:
            async with aiofiles.open(os.path.join(path, buoy + ".js"), 'r') as f:
                olddata = await f.readlines()
                l.info('old data opened ')
                olddata[:] = olddata[1:-1]
        except (OSError,IOError) as e:
            l.warning('new file' + e[1])
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
                l.info('wrote data ' + output[1])
            return True
        except (OSError, IOError):
            return False
    else:
        return False


def clock_time(buoytime, sleep=False):
    """
        Calculates when to call an async function
    :param buoytime: The minute of an hour when a buoy's data is available
        OR number of minutes to sleep
        since buoys update twice an hour, values 0-29 preferred for minute
    :param boolean sleep: If clock_time is intended to be used with asyncio.sleep
    :return if sleep: returns seconds differential between buoytime and now
    :return else: returns absolute time of loop plus buoytime
    """
    l.warning("entering clock_time ")
    looptime = loop.time()
    now = datetime.datetime.now()
    nexttime = now
    # buoytime -=15 # give cdip a minute to update
    if buoytime > 60:
        return buoytime
    if buoytime < 0:
        buoytime += 30
    if buoytime > 29:
        buoytime -= 30
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
    l.warning(nexttime)
    diff = nexttime.timestamp() - now.timestamp()
    if sleep:
        return diff
    else:
        return looptime + diff


def nonzerofib(maximum):
    """Yields fibonacci sequence starting at 1"""
    l.info('nonzerofib')
    a, b = 1, 1
    while a < maximum:
        yield a
        a, b = b, a + b


def fib_sum(maximum):
    """yields fibonnaci sequence until sequence's sum equals maximum"""
    l.debug('fib')
    a, b = 1, 1
    c = 0
    while c < maximum:
        yield a
        a, b = b, a + b
        c += a


async def end_loop():
    """write state to disk and close loop"""
    writes = [write_json(i[0], i[1]) for i in
              ((lastvalue,'/var/scripts/python3/lastvalue.json'),
               (unresponsive, '/var/scripts/python3/unresponsive.json'))]
    await asyncio.gather(*writes)
    session.close()
    loop.stop()


# GLOBALS todo put in a class
l = logging.getLogger()
l.setLevel(level=logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
l.addHandler(ch)
loop = asyncio.get_event_loop()
url = 'https://cdip.ucsd.edu/data_access/justdar.cdip'
buoystarttime = buoyjson()
lastvalue = load_json()
if not lastvalue:
    lastvalue = buoyjson()
unresponsive = load_json('/var/scripts/python3/unresponsive.json')
if not unresponsive:
    l.warn("didn't load unresponsive.json")
    unresponsive = ['165', '186', '188', '187', '134']  # hawaiian buoys generally only report large swells
test_flag=False
con_ssl = aiohttp.TCPConnector(verify_ssl=False) # cdip doesn't provide valid cert
session = aiohttp.ClientSession(connector=con_ssl)


async def main():
    """
        Initial scheduling of tasks, exception handling
    """
    global session, buoystarttime
    tasks = [schedule_fetch(bs[0], bs[1]) for bs in buoystarttime.items()]
    now = datetime.datetime.now()
    l.warning('it`s ' + now.strftime("%H:%M:%S"))
    try:
        gathering = await asyncio.gather(*tasks)
        l.warning(gathering)
    except (aiohttp.ClientError, asyncio.CancelledError, asyncio.TimeoutError) as e:
        l.critical(e)
        if session.closed:
            session = aiohttp.ClientSession()
    except BaseException as e:
        l.critical(e)

if __name__ == "__main__":
    try:
        asyncio.ensure_future(main())
        for signame in ('SIGINT', 'SIGTERM'):
            loop.add_signal_handler(
                getattr(signal, signame),
                lambda: asyncio.ensure_future(end_loop()))
        loop.run_forever()
    except KeyboardInterrupt:
        asyncio.ensure_future(end_loop())
    finally:
        loop.close()


#   testing
##############
async def test_half():
    """Debugging function for half_hourly"""
    l.info('entering test_half')
    now_minutes = datetime.datetime.utcnow().minute
    tasks = [schedule_fetch(bs[0], bs[1]) for bs in {('191', now_minutes), ('134', now_minutes - 1)}]
    for task in asyncio.as_completed(tasks, timeout=90):
        try:
            datastr, buoy = await task
            l.info(len(datastr) + " char datastr received^")
            if len(datastr) > 12:
                data = await process_series(datastr)
                if data['series'] is None:
                    asyncio.ensure_future(reschedule_fetch(buoy))
                else:
                    asyncio.ensure_future(output_series(**data))
            else:
                raise TimeoutError
        except TimeoutError as e:
            l.error(' '.join(['no data for', buoy, e[1]]))
            # await asyncio.sleep(50)
            asyncio.ensure_future(reschedule_fetch(buoy))

            # outputSuccess = await output_series(**data)
            # l.error(outputSuccess)
        # l.error(data)
        await asyncio.sleep(0)


def test(afunction=test_half(), flag=True):
    """Wrapper to test loop without interference"""
    global test_flag, session
    loop = asyncio.get_event_loop()
    conn = aiohttp.TCPConnector(verify_ssl=False)
    with aiohttp.ClientSession(connector=conn) as session:
        test_flag = flag
        t = asyncio.ensure_future(afunction)
        loop.run_until_complete(t)
        jwrite = asyncio.ensure_future(write_json(lastvalue))
        loop.run_until_complete(jwrite)
        # return t.result()
        # loop.close()


################
#   spare parts
################

# async def half_hourly():
# global buoystarttime
#     l.info("entering half_hourly")
#     tasks = [schedule_fetch(bs[0],bs[1]) for bs in buoystarttime.items()]
#     now = datetime.datetime.now()
#     l.warning('it`s ' + now.strftime("%H:%M:%S"))
#     try:
#         gathering = await asyncio.gather(*tasks)
#         l.warning(gathering)
#     except asyncio.CancelledError as e:
#         l.critical('cancelled' + e[1])
#     except asyncio.TimeoutError as e:
#         l.critical('timeout' + e[1])
#     except BaseException:
#         l.critical(e)
#     finally:
#         # asyncio.ensure_future(half_hourly())
#         l.info("done-zo")
# for task in asyncio.as_completed(tasks, timeout=3000):
    #     try:
    #         datastr, buoy = await task
    #         l.info(len(datastr), "^datastr received^")
    #         if len(datastr) > 12:
    #             data = await process_series(datastr)
    #             if data['series']:
    #
    #
    #             else:
    #                 asyncio.ensure_future(reschedule_fetch(buoy))
    #
    #         else:
    #             raise TimeoutError
    #     except TimeoutError as e:
    #         l.info('no data for', buoy, e)
    #         # await asyncio.sleep(50)
    #         asyncio.ensure_future(reschedule_fetch(buoy))
            # data = await reschedule_fetch(buoy)

            # outputSuccess = await output_series(**data)
            # l.info(outputSuccess)
        # l.info(data)
        # await asyncio.sleep(0)
#
# with aiohttp.ClientSession() as session:
#     # signal.signal(signal.SIGINT, signal_handler)
#
#     # for signame in ('SIGINT', 'SIGTERM'):
#     #     loop.add_signal_handler(getattr(signal, signame), functools.partial(loop.stop(), signame))
#     t = asyncio.ensure_future(half_hourly())
#     loop.run_forever()

# def signal_handler(signal, frame):
#     loop.stop()

# lastvalue = {}.fromkeys(buoystarttime)
#
# session = aiohttp.ClientSession()
# t = asyncio.ensure_future(half_hourly())
# loop.run_forever()
#
# t = asyncio.ensure_future(test_half())
# loop.run_until_complete(t)

# async def start_buoy(buoystarttime):
#     """fire up mysql and get buoy list"""
#     query = "SELECT buoy, cdip from buoys where cdip is NOT NULL"  # or something
#     print('entering start_buoy')
#     return buoyjson() # result
#
# def next_buoy(buoystarttime):
#     now = datetime.datetime.now().minute
#     if now >= 30:
#         now -= 30
#     nearesttime = ('',60)
#     while True:
#         for k,v in buoystarttime.items():
#             if v - now > 0 and v - now < nearesttime[1]:
#                 nearesttime = (k,v)
#         yield nearesttime
#
# nb = next(next_buoy(buoystarttime))
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
