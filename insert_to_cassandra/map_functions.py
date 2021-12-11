from datetime import date, timedelta


def map_function_US(line):
    #print(line)
    data = line.split(",")
    #print(data)
    city = data[5]
    province = data[6]
    start_date = date(2020, 1, 22)
    size = len(data)
    prev_day_cases = int(data[13])
    for i in range(13, size):
        new_cases = (int(data[i])-prev_day_cases) if (int(data[i])-prev_day_cases) > 0 else 0
        #print(new_cases)
        yield(start_date, city, province, int(data[i]), new_cases)
        prev_day_cases = int(data[i])
        if(i == size):
            start_date = date(2020, 1, 22)
        else :
            start_date = start_date + timedelta(days=1)

def map_function_global(line):
    #print(line)
    total_columns = 674
    data = line.split(",")
    country = data[1]
    start_date = date(2020, 1, 22)
    size = len(data)
    extra_columns = size - total_columns
    prev_day_cases = 0
    for i in range(4 + extra_columns, size):
        new_cases = (int(data[i])-prev_day_cases) if (int(data[i])-prev_day_cases) > 0 else 0
        yield(start_date, country, int(data[i]), new_cases)
        prev_day_cases = int(data[i])

        if(i == size):
            start_date = date(2020, 1, 22)
        else:
            start_date = start_date + timedelta(days=1)

def map_function_deaths_US(line):
    #print(line)
    #print(line)
    data = line.split(",")
    #print(data)
    city = data[5]
    province = data[6]
    start_date = date(2020, 1, 22)
    size = len(data)
    prev_day_cases = 0
    for i in range(14, size):
        new_cases = (int(data[i])-prev_day_cases) if (int(data[i])-prev_day_cases) > 0 else 0
        yield(start_date, city, province, int(data[i]), new_cases)
        prev_day_cases = int(data[i])
        if(i == size):
            start_date = date(2020, 1, 22)
        else :
            start_date = start_date + timedelta(days=1)
