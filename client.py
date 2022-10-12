from dremio_helper import  *
from pyarrow import flight

if __name__ == "__main__":
    client_auth_middleware = DremioClietnAithMiddleFactory()
    client_cookie_middleware = CookieMiddlewareFactory()

    headers = []

    ## Establish initial Connection 
    client = flight.FlightClient("grpc+tcp://159.65.129.122:32010", middleware=[client_auth_middleware, client_cookie_middleware],**{})

    bearer_token = client.authenticate_basic_token('username', 'pwd', flight.FlightCallOptions(headers=headers))
    headers.append(bearer_token)

    # The query to execute.
    query = 'SELECT avg(CONVERT_TO_INTEGER("Age", 1,1,0)) FROM "Staging S3".postproc.athlete."1_0_0.parquet" group by Sex'

    # Construct FlightDescriptor for the query result set.
    flight_desc = flight.FlightDescriptor.for_command(query)

    # Retrieve the schema of the result set.
    options = flight.FlightCallOptions(headers=headers)
    schema = client.get_schema(flight_desc, options)

    # Get the FlightInfo message to retrieve the Ticket corresponding
    # to the query result set.
    flight_info = client.get_flight_info(flight.FlightDescriptor.for_command(query), options)

    # Retrieve the result set as a stream of Arrow record batches.
    reader = client.do_get(flight_info.endpoints[0].ticket, options)

    # Print results.
    print(reader.read_pandas())
