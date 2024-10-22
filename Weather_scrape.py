import requests
from datetime import datetime, timedelta
import pickle
from azure.storage.blob import BlobServiceClient
import sys
import logging
import os
import atexit
import matplotlib.pyplot as plt
import numpy as np
from dotenv import load_dotenv


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

def download_from_azure(filename,logger):
    logger.info('Downloading data from Azure...')
    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING') 
    container_name = os.getenv('AZURE_STORAGE_CONTAINER_NAME') 
    
    blob_name = filename
    
    # Create a BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get the BlobClient for the file (blob) you want to download
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    # Download the blob (file) content
    with open(filename, "wb") as file:
        download_stream = blob_client.download_blob()
        file.write(download_stream.readall())

    # Now you can load the pickle file
    with open(filename, "rb") as file:
        data = pickle.load(file)
    
    logger.info('Download complete!')
    
    return data

def upload_to_azure(data, filename, logger):
    logger.info('Saving data locally...')
    
    # Update file locally
    with open(filename, 'wb') as file:
        pickle.dump(data,file)

    logger.info('Local save complete!')

    logger.info('Uploading data to Azure...')
        
    # Now we upload to Azure
    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING') 
    # Initialize the BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)   
    container_name = os.getenv('AZURE_STORAGE_CONTAINER_NAME') 
    
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
    
    # Upload the file
    with open(filename, 'rb') as f:
        blob_client.upload_blob(f, overwrite=True)

    logger.info('Data uploaded successfully')
    
    
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

    log_exists = download_log_from_azure(logger, log_filename)

    if not log_exists:
        logger.info('Creating a new log file locally as it not available in Azure')

    cities = ['Stockholm', 'London', 'New York', 'Los Angeles', 'New Delhi', 'Tokyo']
    
    
    data = download_from_azure('data.pickle', logger)
    
    
    if len(sys.argv) > 1:
        arguments = sys.argv[1:]
        for argument in arguments:
            if argument == '-update':
                logger.info('Updating data...')
                data, status = update(data, cities, logger)
                logger.info('Data updated successfully!')
                if not status:
                    quit()
                upload_to_azure(data, 'data.pickle',logger)
            elif argument == '-status':
                logger.info('Data availability check...')
                print(f"\nData available between 2024-09-10 - {find_latest_date(data).strftime('%Y-%m-%d')}")
            elif argument == '-menu':
                data = menu(data, cities, logger)
            elif argument == '-plot':
                logger.info('Generating data plot...')
                location = 'Stockholm, Sweden'
                temp_plot(data, location)
    else:
        print('\nNo valid argument passed\nSyntax: python3 Weather_scrape.py <argument>')
        print('\nList of arguments:\n1. \"-update\" to update the data\n2. \"-status\" to view data availability\n3. \"-menu\" to view the menu')

    
def temp_plot(data, location):
    #Extract time, temp and humidity data
    times = list(data[location].keys())
    temp = [data[location][date]['temp'] for date in times]
    humidity = [data[location][date]['humidity'] for date in times]
    
    fig, ax1 = plt.subplots()
    
    # Plot temperature data
    ax1.plot(times, temp, marker='o', markersize=5, markerfacecolor='red', linestyle='-', color='blue', linewidth=2)
    ax1.set_xlabel('Time')
    ax1.set_ylabel('Temperature (°C)', color='blue')
    ax1.set_title(f'Temperature and Humidity Over Time in {location}')
    ax1.grid(True, linestyle='--', color='gray', alpha=0.7)
    ax1.tick_params(axis='x', rotation=45)
    ax1.tick_params(axis='y', labelcolor='blue')
    
    # Add a horizontal line at freezing point (0°C)
    ax1.axhline(0, color='lightblue', linewidth=1.5, linestyle='--', label='Freezing Point')   
    
    # Annotate the maximum temperature dynamically
    max_temp = max(temp)
    max_time = times[temp.index(max_temp)]
    y_offset = -5 if max_temp > 0.8 * max(temp) else 5  # Adjust based on the height

    ax1.annotate(f'Max Temp: {max_temp}°C', xy=(max_time, max_temp), xytext=(max_time, max_temp + y_offset),
                arrowprops=dict(facecolor='black', shrink=0.05), ha='center')
    
    # Create a second y-axis for the humidity data
    ax2 = ax1.twinx()

    # Plot the humidity as a bar/shaded area in the background
    ax2.fill_between(times, 0, humidity, color='lightgray', alpha=0.5)

    # Set the label for the secondary y-axis (humidity)
    ax2.set_ylabel('Humidity (%)', color='gray')
    ax2.set_ylim(0, 100)  # Humidity is from 0% to 100%
    ax2.tick_params(axis='y', labelcolor='gray')

    # Display the plot
    plt.show()
    
    pass
        
def upload_logger_to_azure(logger, log_filename):
    try:
        logger.info('Uploading log to Azure...')

        connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING') 
        container_name = os.getenv('AZURE_STORAGE_CONTAINER_NAME') 
        

        # Initialize the BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=log_filename)

        # Upload the log file
        with open(log_filename, 'rb') as log_file:
            blob_client.upload_blob(log_file, overwrite=True)

        logger.info('Log uploaded successfully to Azure')
        logger.info('Program terminated')

    except Exception as e:
        logger.error(f'Failed to upload log to Azure: {e}')

def download_log_from_azure(logger, log_filename):
    try:
        logger.info('Checking for log on Azure...')

        connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING') 
        container_name = os.getenv('AZURE_STORAGE_CONTAINER_NAME') 

        # Initialize the BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=log_filename)

        if blob_client.exists():
            logger.info('Log found. Downloading from Azure...')

            # Download the file
            with open(log_filename, 'wb') as log_file:
                download_stream = blob_client.download_blob()
                log_file.write(download_stream.readall())
            
            logger.info('Log downloaded successfully from Azure')
            return True
        else:
            logger.info('No log found in Azure')
            return False

    except Exception as e:
        logger.warning(f'Log not found on Azure or failed to download: {e}')
        return False



def setup_logging(log_filename):
    # Create a logger
    logger = logging.getLogger("Weather_log")
    if not logger.hasHandlers():
        logger.setLevel(logging.INFO) # Set the minimum level to capture (INFO and above)

        username = os.getlogin()

        #define log format
        formatter = logging.Formatter(f'%(asctime)s - %(levelname)s - [User: {username}] - %(message)s')

        # Create a file handler (using RotateFileHandler to manage file size)
        file_handler = logging.FileHandler(log_filename, mode='a')
        file_handler.setFormatter(formatter)

        # Add the handler to the logger
        logger.addHandler(file_handler)

    return logger
    
def setup_exit_handling(log_filename, logger):
    #Register the function to upload the log on exit
    atexit.register(upload_logger_to_azure, log_filename=log_filename, logger=logger)
    
def menu(data, cities, logger):
    print('\nMENU:\n1. Update Data\n2. View data\n')
    choice = input('Enter choice (1/2): ')
    if choice == '1':
        data = update(data, cities, logger)
        upload_to_azure(data, 'data.pickle', logger)
    
        print(f"Data available between 2024-09-10 - {find_latest_date(data).strftime('%Y-%m-%d')}")
        
    elif choice == '2':
        try:
            locations = list(data.keys())
            print('\nWhat location would you like to view data for?')
            for i in range(len(locations)):
                print(f"{i+1}. {locations[i]}")
                
            chosen_location = locations[int(input('Enter choice: '))-1]
            
            print('\nFor which date?')                
            print(f"Data available between 2024-09-10 - {find_latest_date(data).strftime('%Y-%m-%d')}")
            chosen_date = input('Enter date (ÅÅÅÅ-MM-DD): ').strip()
            print(f"\nLocation: {chosen_location}")
            print(f"Date: {chosen_date}\n")
            print(f"Temp: {data[chosen_location][chosen_date]['temp']} C")
            print(f"Windspeed: {data[chosen_location][chosen_date]['windspeed']} m/s")
            print(f"Gust: {data[chosen_location][chosen_date]['gust']} m/s")
            print(f"Wind dir: {data[chosen_location][chosen_date]['wind_degree']} {data[chosen_location][chosen_date]['wind_dir']}")
            print(f"Pressure: {data[chosen_location][chosen_date]['air_pressure']} mb")
            print(f"Humidity: {data[chosen_location][chosen_date]['humidity']} %")
            print(f"UV-index: {data[chosen_location][chosen_date]['uv']}")
                
            
        except KeyError:
            print('No data available')
            quit()
    return data
    

    
if __name__ == '__main__':
    load_dotenv()
    log_filename = 'weather_log.log'
    logger = setup_logging(log_filename)
    # Ensure log is uploaded when the program terminates
    setup_exit_handling(log_filename='weather_log.log', logger=setup_logging('weather_log.log'))
    main()

