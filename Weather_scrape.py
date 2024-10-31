import requests
from datetime import datetime, timedelta
import pickle
import logging
import os


def determine_dates(data):
    '''
    Determines the start date and end date for the api request given the data we already have
    '''
    # Determine end date
    now = datetime.now()
    if now.hour >= 12:
        end_date =  now.date()
    else:
        end_date = (now - timedelta(days=1)).date()
        

    # Determine start date
    if not data:
        # If we have no data, the start_date is set to 7 days ago
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    else:
        # Otherwise, we need to check how far we have data
        current_date = datetime.now()
        seven_days_ago = current_date - timedelta(days=7)
        city = 'Stockholm, Sweden' # Doesn't matter which one, just picked on we know is there if we have data
        if city in data:
            latest_date = find_latest_date(data)
            
            # Calculate the day after the most recent date
            day_after_recent = latest_date + timedelta(days=1)
            
            # Set start_date to either the day after or 7 days ago, whichever is more recent
            if day_after_recent < seven_days_ago:
                start_date = seven_days_ago.strftime('%Y-%m-%d')
            else:
                start_date = day_after_recent.strftime('%Y-%m-%d')
        
    #start_date = '2024-09-11'
    
    return start_date, end_date.strftime('%Y-%m-%d')
    
    
def find_latest_date(data):
    city = 'Stockholm, Sweden'
    try:
        date_strings = data[city].keys()
        date_objects = [datetime.strptime(date_str, '%Y-%m-%d') for date_str in date_strings]
        latest_date = max(date_objects)
        
        return latest_date
    except KeyError:
        print('No data available')
        quit()

def api_request(city, start_date, end_date):
    '''
    Completes a specified api request with new weather data, returns a json response
    '''
    BASE_URL = 'http://api.weatherapi.com/v1'
    
    API_KEY = os.getenv('API_KEY')
    API_METHOD = '/history.json'
    
    
    hour = '12' # Noon for each day
    API_PARAMETER = f'&dt={start_date}&end_dt={end_date}&hour={hour}'
    
    url = f"{BASE_URL}{API_METHOD}?key={API_KEY}&q={city}{API_PARAMETER}"
    
    # Make API-request
    response = requests.get(url)
    
    return response


def collect(response, days, data):
    '''
    Takes a json response and collects the data for the given dates
    Updates the data dictionary
    '''
    location = f"{response['location']['name']}, {response['location']['country']}"
    
    try:
        dates_dict = data[location]
    except KeyError:
        dates_dict = {}

        
    for index in range(days):
        #date = response['forecast']['forecastday'][index]['hour'][0]['time']
        date = response['forecast']['forecastday'][index]['date']
        
        temp = response['forecast']['forecastday'][index]['hour'][0]['temp_c']
        windspeed = round(float(response['forecast']['forecastday'][index]['hour'][0]['wind_kph']) /3.6, 1)
        gust = round(float(response['forecast']['forecastday'][index]['hour'][0]['gust_kph'])/3.6, 1)
        wind_degree = response['forecast']['forecastday'][index]['hour'][0]['wind_degree']
        wind_dir = response['forecast']['forecastday'][index]['hour'][0]['wind_dir']
        air_pressure = response['forecast']['forecastday'][index]['hour'][0]['pressure_mb']
        humidity = response['forecast']['forecastday'][index]['hour'][0]['humidity']
        uv = response['forecast']['forecastday'][index]['hour'][0]['uv']
        
    
        # Add all datapoints to a dictionary
        datapoints = {}
    
        datapoints['temp'] = temp
        datapoints['windspeed'] = windspeed
        datapoints['gust'] = gust
        datapoints['wind_degree'] = wind_degree
        datapoints['wind_dir'] = wind_dir
        datapoints['air_pressure'] = air_pressure
        datapoints['humidity'] = humidity
        datapoints['uv'] = uv
    
        dates_dict[date] = datapoints
    
    data[location] = dates_dict
    
    return data
    
def update(data, cities, logger):
    start_date, end_date = determine_dates(data)
    if start_date > end_date:
        # If the start_date passes the end_date, it means data is up to date. 
        logger.info('Nothing to update, most recent data available')
        return data, False
    elif start_date == end_date:
        # Means we are only missing the last day, and in that case we decrement the start date
        # by one and retrieve the last two days worth of data, for simplicity
    
        start_date = (datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

    logger.info('Sending API-requests...')
    for city in cities:
        response = api_request(city, start_date, end_date)
        if response.status_code == 200:
            # How many days between start_date and end_date?
            date_start = datetime.strptime(start_date,'%Y-%m-%d')
            date_end = datetime.strptime(end_date, '%Y-%m-%d')
            days = date_end - date_start
            
            
            data = collect(response.json(), days.days+1, data)
        else:
            logger.info('API-request failed')
            print('API-request failed')
            return data, False
   
    logger.info('API-requests successful!')
    return data, True

def load_data(filename, logger):
    logger.info('Loading data...')
    
    # Load the pickle file
    with open(filename, "rb") as file:
        data = pickle.load(file)
    
    return data

def save_data(data, filename, logger):
    logger.info('Saving data...')
    
    # Update file locally
    with open(filename, 'wb') as file:
        pickle.dump(data,file)

    logger.info('Save complete!')
        
    
    
    
def main():
    
    #Remember to include countries when referencing the locations in 'data'
    # Stockholm, Sweden
    # London, United Kingdom
    # New York, United States of America
    # Los Angeles, United States of America
    # New Delhi, India
    # Tokyo, Japan

    log_filename = 'weather_log.log'

    #Set up logging
    logger = setup_logging(log_filename)


    cities = ['Stockholm', 'London', 'New York', 'Los Angeles', 'New Delhi', 'Tokyo']
    
    
    data = load_data('data.pickle', logger)
    
    logger.info('Updating data...')
    data, status = update(data, cities, logger)
    logger.info('Data updated successfully!')

    save_data(data, 'data.pickle', logger)
    

def setup_logging(log_filename):
    # Create a logger
    logger = logging.getLogger("Weather_log")
    if not logger.hasHandlers():
        logger.setLevel(logging.INFO) # Set the minimum level to capture (INFO and above)

        #define log format
        formatter = logging.Formatter(f'%(asctime)s - %(levelname)s  - %(message)s')

        # Create a file handler (using RotateFileHandler to manage file size)
        file_handler = logging.FileHandler(log_filename, mode='a')
        file_handler.setFormatter(formatter)

        # Add the handler to the logger
        logger.addHandler(file_handler)

    return logger
    

    
if __name__ == '__main__':
    log_filename = 'weather_log.log'
    logger = setup_logging(log_filename)
    main()

