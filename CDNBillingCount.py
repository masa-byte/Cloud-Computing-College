# Define the CDN pricing
price_per_request = 0.001  # EUR per served request
price_per_gb = 0.08  # EUR per GB of transferred data


def calculate_cdn_billing(file_path):
    total_bytes = 0.0
    total_requests = 0.0

    # Read the file and parse the data
    with open(file_path, "r") as file:
        for line in file:
            # Split the line by space and extract the key (bytes or requests) and value
            key, value = line.strip().split()
            value = float(value)

            if key == "bytes":
                total_bytes = value
            elif key == "requests":
                total_requests = value

    # Calculate the price
    total_gb = total_bytes / (1024**3)  # Convert bytes to GB (base 2)
    price_for_data = total_gb * price_per_gb
    price_for_requests = total_requests * price_per_request

    # Output the results
    print(f"Total GB transferred: {total_gb:.2f} GB")
    print(f"Price for data transferred: {price_for_data:.2f} EUR")
    print(f"Total requests made: {total_requests:.0f}")
    print(f"Price for requests: {price_for_requests:.2f} EUR")
    print(f"Total CDN cost: {price_for_data + price_for_requests:.2f} EUR")

    # Save the results to a file
    with open("full-result.txt", "w") as output:
        output.write(f"Total GB transferred: {total_gb:.2f} GB\n")
        output.write(f"Price for data transferred: {price_for_data:.2f} EUR\n")
        output.write(f"Total requests made: {total_requests:.0f}\n")
        output.write(f"Price for requests: {price_for_requests:.2f} EUR\n")
        output.write(f"Total CDN cost: {price_for_data + price_for_requests:.2f} EUR\n")


# Call the function with the path to your file
calculate_cdn_billing("result.txt")
