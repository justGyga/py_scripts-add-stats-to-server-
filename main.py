from random import randint
import psycopg2
import datetime as DT
import configparser
from tqdm import tqdm


def getConfigsParser():
    config = configparser.ConfigParser()
    config.read("config.ini")
    return config


def getDateTimeList(date_fmt='%Y-%m-%d %H:%M:%S', array_len=51840, adapterId=None):
    array = []
    step = DT.timedelta(minutes=5)

    # Set the start_date to 180 days ago from the current day
    start_date = DT.datetime.now() - DT.timedelta(days=179)
    start_date = start_date.replace(hour=0, minute=0, second=5)

    # Set the last_date as the current day
    last_date = DT.datetime.now()
    last_date = last_date.replace(hour=23, minute=59, second=58)

    for i in range(array_len):
        array.append({"wattage": randint(0, 200),
                      "volt": randint(0, 200),
                      "temperature": randint(0, 200),
                      "createdAt": start_date.strftime(date_fmt),
                      "adapterId": adapterId})
        start_date += step
        if start_date > last_date:
            break
    return array


def getConnectionToPoPOSTGRESS(dbname, user, password, host="localhost", port="5432"):
    return psycopg2.connect(f"dbname={dbname} user={user} password={password} host={host} port={port}")


def postDataOnPOSTGRESS(connection, data):
    cursor = connection.cursor()
    sql_command = 'INSERT INTO \"adapter-statistic\"(wattage, volt, temperature, "createdAt", "adapterId") VALUES(%s,%s,%s,%s,%s)'
    try:
        for step_string in tqdm(data):
            cursor.execute(sql_command, (
                step_string["wattage"], step_string['volt'], step_string["temperature"], step_string["createdAt"],
                step_string["adapterId"]))
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        return error
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()
    return "Data successfully posted"


def printData(data):
    print(data)


if __name__ == '__main__':
    # Parse data
    configs = getConfigsParser()

    # Get data list
    iteration_num = int(configs["OTHER"]["iteration_num"])
    adapter_id = configs["ADAPTER"]["adapter_id"]
    data = getDateTimeList(adapterId=adapter_id, array_len=iteration_num)

    # Connect to DB
    dbname = configs["DB"]["db_name"]
    user = configs["DB"]["user"]
    password = configs["DB"]["password"]
    host = configs["DB"]["host"]
    port = configs["DB"]["port"]
    con = getConnectionToPoPOSTGRESS(dbname, user, password, host, port)

    # Post and print data
    printData(postDataOnPOSTGRESS(con, data))
