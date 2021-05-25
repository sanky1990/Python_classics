#Yesterday midnight to 11.59.59 whole day in miliseconds epoch generator

import datetime
def get_yesterday_epoch() -> (int, int):
    today = datetime.datetime.now()
    yesterday1 = today.date() - datetime.timedelta(days=3)
    yesterday = today.date() - datetime.timedelta(days=1)

    yesterday_midnight = datetime.datetime.combine(yesterday1, datetime.time.min)
    yesterday_235959 = datetime.datetime.combine(yesterday, datetime.time.max)
    yesterday_midnight_millis = int(yesterday_midnight.timestamp() * 1000)
    yesterday_235959_millis = int(yesterday_235959.timestamp() * 1000)
    print (yesterday_midnight)
    print(yesterday_235959)
    return yesterday_midnight_millis, yesterday_235959_millis
timerange1 = get_yesterday_epoch()
start = timerange1[0]
end = timerange1[1]
