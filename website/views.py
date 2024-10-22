

@app.route('/plot/temp/<location>')
def show_temp(location):
    location = location.replace("+", " ")

    if location not in data:
        return f"No data over this location, {location}"

    # Extract time and temperature data
    times = list(data[location].keys())
    temp = [data[location][date]['temp'] for date in times]

    # Create the temperature plot
    fig, ax = plt.subplots()

    ax.plot(times, temp, marker='o', markersize=5, markerfacecolor='red', linestyle='-', color='blue', linewidth=2)
    ax.grid(True, linestyle='--', color='gray', alpha=0.7)
    ax.set_xlabel('Time')
    ax.set_ylabel('Temperature (°C)')
    ax.set_title(f'Temperature Over Time in {location}')
    ax.tick_params(axis='x', rotation=45)

    # Add a horizontal line at freezing point (0°C)
    ax.axhline(0, color='lightblue', linewidth=1.5, linestyle='--', label='Freezing Point')

    # Annotate the maximum temperature dynamically
    max_temp = max(temp)
    max_time = times[temp.index(max_temp)]

    # Determine where to place the annotation based on the y-value
    y_offset = -5 if max_temp > 0.8 * max(temp) else 5  # Adjust based on the height
    ax.annotate(f'Max Temp: {max_temp}°C', xy=(max_time, max_temp), xytext=(max_time, max_temp + y_offset),
                arrowprops=dict(facecolor='black', shrink=0.05),
                ha='center')

    # Adjust the layout and save the plot to a BytesIO object
    plt.tight_layout()
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    return send_file(img, mimetype='image/png')

@app.route('/plot/wind/<location>')
def show_wind(location):
    location = location.replace("+", " ")


    if location not in data:
      return f"No data over this location, {location}"

    # Extract time and wind speed data
    times = list(data[location].keys())
    speeds = [data[location][date]['windspeed'] for date in times]

    # Create the wind speed plot
    fig, ax = plt.subplots()

    ax.plot(times, speeds, marker='o')
    ax.grid(True, linestyle='--', color='gray', alpha=0.7)
    ax.set_xlabel('Time')
    ax.set_ylabel('Wind Speed (m/s)')
    ax.set_title('Wind Speed Over Time')
    ax.tick_params(axis='x', rotation=45)

    # Save the plot to a BytesIO object and return it as a response
    img = io.BytesIO()
    plt.tight_layout()
    plt.savefig(img, format='png')
    img.seek(0)
    return send_file(img, mimetype='image/png')