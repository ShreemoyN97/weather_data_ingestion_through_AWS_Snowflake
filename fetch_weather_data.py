import json
from datetime import datetime
import requests  
import boto3
from decimal import Decimal

# Initialize a connection to DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table("weather")  # Replace "weather" with your actual table name

def get_weather_data(city):  
    """
    Fetches weather data for the given city using WeatherAPI.
    
    Args:
        city (str): Name of the city to fetch weather data for.

    Returns:
        dict: JSON response containing weather details.
    """
    api_url = "http://api.weatherapi.com/v1/current.json"
    params = {  
        "q": city,    
        "key": "<api_key>"  # Replace <api_key> with your actual API key
    }  
    response = requests.get(api_url, params=params)  
    data = response.json()  
    return data  

def lambda_handler(event, context):
    """
    AWS Lambda function to fetch weather data for multiple cities and store it in DynamoDB.
    
    Args:
        event (dict): The event data passed to the function.
        context (object): AWS Lambda context object.
    
    Returns:
        None
    """

    # List of cities to fetch weather data for
    cities = ["Boston", "New York", "Mumbai", "Seattle", "Hartford", 
              "London", "Dallas", "Miami", "Tampa", "Delhi"]
    
    for city in cities:
        data = get_weather_data(city)  # Fetch weather data for the city
    
        # Extract required weather parameters from API response
        temp = data['current']['temp_c']
        wind_speed = data['current']['wind_mph']
        wind_dir = data['current']['wind_dir']
        pressure_mb = data['current']['pressure_mb']
        humidity = data['current']['humidity']
    
        # Print the fetched data for debugging/logging purposes
        print(city, temp, wind_speed, wind_dir, pressure_mb, humidity)
        
        # Get the current timestamp in ISO format
        current_timestamp = datetime.utcnow().isoformat()
        
        # Create a dictionary object to store in DynamoDB
        item = {
            'city': city,
            'time': str(current_timestamp),  # Storing time as string for compatibility
            'temp': temp,
            'wind_speed': wind_speed,
            'wind_dir': wind_dir,
            'pressure_mb': pressure_mb,
            'humidity': humidity
        }

        # Convert floating point values to Decimal type for DynamoDB compatibility
        item = json.loads(json.dumps(item), parse_float=Decimal)
        
        # Insert data into DynamoDB
        table.put_item(Item=item)
