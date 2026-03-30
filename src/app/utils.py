#####################################################
## Importing Necessary Python Libraries
#####################################################

# Import libraries required for data processing
import csv
import json
import numpy as np
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from io import StringIO, BytesIO
import requests
from shapely.geometry import box
from shapely import wkt
from shapely.geometry import mapping
import geopandas as gpd
import zipfile
import tempfile
import os
import pandas as pd
import geojson
from pathlib import Path


# Import libraries associated with FastAPI and BIGQUERY
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
from fastapi import HTTPException, Response
from fastapi.responses import JSONResponse, StreamingResponse
from typing import Callable, Any
from fastapi.routing import APIRoute
from starlette.datastructures import QueryParams
from fastapi import Request


#####################################################
## Setting credentials and initializing BigQuery client
#####################################################
## Comment out the credential resolution function and client initialization for local deployment
# def _resolve_google_credentials_path() -> str:
#     if "GOOGLE_CREDENTIALS_PATH" in os.environ:
#         return os.environ["GOOGLE_CREDENTIALS_PATH"]

#     default_path = Path(__file__).resolve().parents[2] / "credentials" / "secret-credentials.json"
#     if default_path.exists():
#         return str(default_path)

#     raise FileNotFoundError(
#         "Google credentials not found. Set GOOGLE_CREDENTIALS_PATH "
#         "or add credentials/secret-credentials.json in project root."
#     )

# GOOGLE_CREDENTIALS_PATH = _resolve_google_credentials_path()
# credentials = service_account.Credentials.from_service_account_file(GOOGLE_CREDENTIALS_PATH)
project_id = os.environ.get("GCP_PROJECT_ID")
client_1 = bigquery.Client(
    project=project_id,
    location='us-central1',
    # credentials=credentials # Use credentials for local deployment
)
client_2 = bigquery.Client(
    project=project_id,
    location='US',
    # credentials=credentials # Use credentials for local deployment
)
# Define the storage client to access the GCS bucket
storage_client = storage.Client(
    # credentials=credentials, # Use credentials for local deployment
    project=project_id)


#####################################################
# ## Defining Necessary Utility Functions
#####################################################

# Define a function to extract reach_id(s) from a given hydroshare_file_url composed against a hydroshare_id
def hydroshare_url_to_reach_id(hydroshare_file_url: str):
    """ Extract a list of reach_id(s) that corresponds to a particular hydroshare_file_url composed against a hydroshare_id """
    try:
        # Make an API request to HydroShare to retrieve data
        hydroshare_data_response = requests.get(hydroshare_file_url)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Error retrieving response against hydroshare_id: {str(e)}")
    try: 
        # Parse the response as JSON   
        hydroshare_data = hydroshare_data_response.json()
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Error parsing response from the resource against the hydroshare_id: {str(e)}")
    try:
        # Locate the id field in the JSON
        hydroshare_data_keys = hydroshare_data[0].keys()
        id_key = None
        for key in hydroshare_data_keys:
            if key.lower() in ['reach_id', 'id', 'comid', 'feature_id', 'station_id']:
                id_key = key
                break
        if id_key is None:
            raise HTTPException(status_code=422, detail="Could not locate a field corresponding to reach_id in the resource data. Kindly ensure that the resource data contains a field with name like reach_id, id, comid, feature_id, or station_id.")
        # Extract comids from the HydroShare data as a Python list
        reach_id = [item.get(id_key) for item in hydroshare_data]
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Error extracting reach_id from the resource data against the hydroshare_id: {str(e)}")
    # Return a list of reach_id(s) corresponding to the given hydroshare_file_url
    return reach_id 

# Define a function to estimate the amount of data to be processed in bytes for a given query without actually running the query
def bytes_to_be_processed(query, job_config, endpoint):
    """ Estimate the amount of data to be processed in bytes for a given query"""
    # Select the appropriate BigQuery client based on the endpoint
    if endpoint in ['geometries', 'return-periods', 'analyses-assim', 'forecasts']: 
        client = client_2
    else:
        client = client_1
    # Configure the job to be a dry run
    job_config_dry = bigquery.QueryJobConfig(dry_run=True, 
                                             use_query_cache=False, 
                                             labels={'job_timeout_ms': '7200000'})
    job_config = merge_job_configs(job_config, job_config_dry)
    # Run the dry run query through the BigQuery client
    try:
        query_job = client.query(query, job_config=job_config)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"The implementation of the query formulated against your request FAILED: {str(e)}")
    # Return the amount of total bytes (integer) to be processed with this query
    return query_job.total_bytes_processed

# Define a function to make objects JSON serializable
def make_serializable(obj):
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    # Handle other types if needed
    return str(obj)

# Define a function to make API request to BigQuery client and retrieve data
def run_query(query, job_config, endpoint):
    """ Make API request to BigQuery client and retrieve data """
    # Select the appropriate BigQuery client based on the endpoint
    if endpoint in ['geometries', 'return-periods', 'analyses-assim', 'forecasts']:
        client = client_2
    else:
        client = client_1
    # Make the query request
    try:
        query_job = client.query(query, job_config=job_config)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"The implementation of the query formulated against your request FAILED: {str(e)}")
    # Access query response
    results = query_job.result()
    # Returns a RowIterator object containing the query results
    return results

def output_formatter(results_df, output_format, caller_endpoint, metadata):
    """ Format the results in requested output format tailored to endpoints. """
    # Return JSON response immediately for empty responses to avoid unnecessary processing and potential errors in formatting
    if results_df.empty:
        return JSONResponse(status_code=200, content={"detail": "No data found for the given parameters. The query response contains an empty dataset."})
    # Replace NaN values with None to ensure JSON compliance
    results_df = results_df.replace({np.nan: None})
    # Convert the DataFrame to a list of dictionaries for easier formatting in different output types
    results_dict_list = results_df.to_dict(orient='records')
    # Formulate output file name based on the endpoint
    output_file_name = 'nwm_' + caller_endpoint + '_' + datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    # Curate the API response in JSON format
    if output_format == 'json':
        if metadata is None:
            # Return results without metadata as a JSON response
            response = JSONResponse(content=json.loads(json.dumps(results_dict_list, default=make_serializable)))
        else:
            # Embed metadata into the results
            results_with_metadata = {
                "data": results_dict_list,
                "metadata": metadata  
            }
            # Return results with metadata as a JSON response
            response = JSONResponse(content=json.loads(json.dumps(results_with_metadata, default=make_serializable)))  
    # Curate the API response in CSV format
    elif output_format == 'csv':
        # Create a StringIO object to write CSV data into memory
        csv_output = StringIO()
        # Write metadata at the beginning if available
        if metadata is not None:
            csv_output.write(metadata)
        # Write the data from results_dict_list to the StringIO object in CSV format
        csv_writer = csv.DictWriter(csv_output, fieldnames=results_dict_list[0].keys())
        # Write header to the CSV file
        csv_writer.writeheader()
        # Write rows to the CSV file
        csv_writer.writerows(results_dict_list)
        # Create a StreamingResponse with the CSV content and appropriate headers for file download
        response = StreamingResponse(
            iter([csv_output.getvalue()]),
            media_type="text/csv")
        # Close the StringIO object after creating the response
        csv_output.close()
    # Curate the API response in GeoJSON format
    elif output_format == 'geojson':
        # Create an empty list to hold GeoJSON features
        features = []
        for row in results_dict_list:
            # Convert the WKT geometry to a GeoJSON geometry
            geometry = mapping(wkt.loads(row.get('geometry')))
            # Get the reach ID from the row
            reach_id = row.get('reach_id')
            # Prepare the GeoJSON properties with custom ID
            if caller_endpoint in ['analyses-assim', 'retrospectives', 'forecasts']:
                # Prepare the GeoJSON properties with reach_id included
                data_as_properties = {key: value for key, value in row.items() if key not in ['geometry']}
                # Create a GeoJSON feature with the geometry and properties, and auto-assigned ID
                feature = geojson.Feature(geometry=geometry, properties=data_as_properties)
            elif caller_endpoint in ['geometries', 'percentile-flows', 'flow-metrics', 'return-periods']:
                # Prepare the GeoJSON properties with reach_id excluded
                data_as_properties = {key: value for key, value in row.items() if key not in ['geometry', 'reach_id']}
                # Create a GeoJSON feature with the geometry and properties setting the feature ID to reach_id
                feature = geojson.Feature(geometry=geometry, properties=data_as_properties, id=reach_id)
            # Append the feature to the list of features
            features.append(feature)
        # Create a GeoJSON FeatureCollection from the list of features
        if metadata is not None:
            # Embed metadata into the GeoJSON response
            geojson_response = geojson.FeatureCollection(features, metadata=metadata)
        else:
            geojson_response = geojson.FeatureCollection(features)
        # Serialize the GeoJSON response to a string with custom serialization for non-serializable objects
        geojson_string = json.dumps(geojson_response, default=make_serializable)
        # Prepare the GeoJSON response for returning
        response = JSONResponse(content=json.loads(geojson_string))
    # Curate the API response in Shapefile format
    elif output_format == 'shapefile':
        # Convert the GeoJSON response to a GeoPandas DataFrame
        geo_data_frame = gpd.GeoDataFrame(results_df, 
                                            geometry=results_df['geometry'].apply(wkt.loads),
                                            crs="EPSG:4326"
                                            )
        # Write shapefile to a temp directory, then zip it
        with tempfile.TemporaryDirectory() as tmpdir:
            shp_path = os.path.join(tmpdir, output_file_name + '.shp')
            geo_data_frame.to_file(shp_path, driver='ESRI Shapefile')
            # Create a zip in memory
            zip_buffer = BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w') as zf:
                # Add all shapefile components to the zip
                for ext in ['.shp', '.shx', '.dbf', '.prj', '.cpg']:
                    fpath = os.path.join(tmpdir, output_file_name + ext)
                    if os.path.exists(fpath):
                        zf.write(fpath, arcname=output_file_name + ext)
                if metadata is not None:
                    # Insert metadata as a JSON file in the zip
                    metadata_string = json.dumps(metadata, indent=2, default=make_serializable)
                    zf.writestr(output_file_name + '.json', metadata_string.encode("utf-8"))
            # Seek to the beginning of the zip buffer before returning
            zip_buffer.seek(0)
            # Prepare the StreamingResponse with appropriate headers for file download
            headers = {"Content-Disposition": f'attachment; filename="{output_file_name}.zip"'}
            # Return the zip file as a streaming response
            response = StreamingResponse(zip_buffer, media_type="application/zip", headers=headers)
    # Curate the API response in GeoPackage format
    elif output_format == 'geopackage':
        # Convert the GeoJSON response to a GeoPandas DataFrame
        geo_data_frame = gpd.GeoDataFrame(results_df,
                                            geometry=results_df['geometry'].apply(wkt.loads),
                                            crs="EPSG:4326"
                                            )
        # Convert time field to datetime format
        geo_data_frame['time'] = pd.to_datetime(geo_data_frame['time'])
        if 'reference_time' in geo_data_frame.columns:
            # Convert reference_time field to datetime format if exists
            geo_data_frame['reference_time'] = pd.to_datetime(geo_data_frame['reference_time'])
        # Prepare a zip file in memory containing the GeoPackage and metadata
        if metadata is not None:
            with tempfile.TemporaryDirectory() as tmpdir:
                gpkg_path = os.path.join(tmpdir, f"{output_file_name}.gpkg")
                geo_data_frame.to_file(gpkg_path, driver="GPKG", layer="nwm_data")
                zip_buffer = BytesIO()
                with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                    zf.write(gpkg_path, arcname=f"{output_file_name}.gpkg")
                    metadata_string = json.dumps(metadata, indent=2, default=make_serializable)
                    zf.writestr(f"{output_file_name}_metadata.json", metadata_string.encode("utf-8"))
                zip_buffer.seek(0)
                return StreamingResponse(
                    zip_buffer,
                    media_type="application/zip",
                    headers={"Content-Disposition": f"attachment; filename={output_file_name}.zip"}
                )
        # Prepare only the GeoPackage file for download without metadata
        else:
            file_buffer = BytesIO()
            geo_data_frame.to_file(file_buffer, driver="GPKG",)
            file_buffer.seek(0)
            return Response(
                content=file_buffer.getvalue(),
                media_type="application/octet-stream", # Use a generic binary type
                headers={"Content-Disposition": f"attachment; filename={output_file_name}.gpkg"}
            )
    return response

# Construct a SQL query to retrieve geometry data
def geometry_query(reach_id, hydroshare_id,
                   bounding_box: tuple, geom_filter, lon, lat,
                   with_buffer, lowest_stream_order, ordered,
                   output_status: str = 'geom_only'):
    """Construct a SQL query to retrieve geometry data based on various filtering parameters."""
    # Checking the requested order of output and creating corresponding query substring
    if ordered:
        order_status = "ORDER BY reach_id"
    else:
        order_status = ""
    # Add extra criteria to query if 'lowest_stream_order' is provided a value
    if lowest_stream_order is not None:
        stream_order_criteria = f"AND streamOrder >= {lowest_stream_order}"
    else:
        stream_order_criteria = ""
    # Modify the selection crieteria based on output_status
    if output_status == 'id_only':
        SELECTion_clause = "SELECT streams.station_id AS reach_id"
    elif output_status == 'geom_only':
        SELECTion_clause = "SELECT streams.station_id AS reach_id, streams.geometry as geometry"
    else:
        SELECTion_clause = """SELECT 
                                    streams.station_id AS reach_id,
                                    streams.streamOrder AS stream_order,
                                    streams.Shape_Length AS shape_length,
                                    streams.geometry AS geometry"""
    ## Customize for geometry filtering parameters (geom_filter, bounding_box, lat and lon) for specifying data extent
    if geom_filter or bounding_box or (lon and lat):
        # Convert the bounding_box to WKT polygon geometry string
        if bounding_box:
            geography_string = str(box(bounding_box[0], bounding_box[1], bounding_box[2], bounding_box[3]))
            geography_format = 'wkt'
        # Convert the lat and lon pair to WKT point geometry string
        elif lon and lat:
            geography_string = 'POINT(' + str(lon) + ' ' + str(lat) + ')'
            geography_format = 'wkt'
        # Grab geometry filter
        else:
            geography_string, geography_format = geom_filter # receive tuple from validator
        # Assign different parser function for different input format of geography, identifying point geometry exclusively
        is_point = False
        if geography_format=='geojson':
            parser_function = "ST_GEOGFROMGEOJSON"
            if "point" in geography_string.lower():
                is_point = True
        elif geography_format=='wkb':
            parser_function = "ST_GEOGFROMWKB"
            if geography_string.startswith('0101000000'):
                is_point = True
        elif geography_format=='wkt':
            parser_function = "ST_GEOGFROMTEXT"
            if "point" in geography_string.lower():
                is_point = True
        # Write the GoogleSQL query for Point geography as parameter
        # with the flexibility to access the nearby reach if does not coincide
        if is_point and (with_buffer is None):
            query = f"""
                {SELECTion_clause},
                    ST_DISTANCE(streams.geometry, {parser_function}(@geography_string_param)) AS distance
                FROM
                    `bigquery-public-data.national_water_model.stream_network` AS streams
                ORDER BY distance
                LIMIT 1
            """
            # Configure the query job with the geography string parameter for point geometry
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "geography_string_param", 
                        "STRING", 
                        geography_string),],)
        # Write the GoogleSQL query for all other geography as parameter, but without buffer option
        elif with_buffer is None:
            query = f"""
                {SELECTion_clause}
                FROM
                    `bigquery-public-data.national_water_model.stream_network` AS streams
                WHERE
                    ST_INTERSECTS({parser_function}(@geography_string_param)
                    , geometry)
                    {stream_order_criteria}
                {order_status}
                    """
            # Configure the query job with the geography string parameter for all other geography
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "geography_string_param", 
                        "STRING", 
                        geography_string),],
                use_query_cache=True)
        # Write the GoogleSQL query for all other geography as parameter with buffer
        else:
            query = f"""
                {SELECTion_clause}
                FROM
                    `bigquery-public-data.national_water_model.stream_network` AS streams
                WHERE
                    ST_INTERSECTS(
                        ST_BUFFER(ST_GEOGFROMTEXT(@geography_string_param), @with_buffer_param), 
                        geometry
                    )
                    {stream_order_criteria}
                {order_status}
                    """
            # Configure the query job with the geography string parameter and buffer parameter 
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "geography_string_param", 
                        "STRING", 
                        geography_string),
                    bigquery.ScalarQueryParameter(
                        "with_buffer_param", 
                        "FLOAT64", 
                        with_buffer),],)
    ## Customize for hydroshare_id and reach_id parameters with direct mapping
    elif hydroshare_id or reach_id:
        if hydroshare_id:
            # Extract corresponding reach_id(s) from the given hydroshare resource URL
            reach_id = hydroshare_url_to_reach_id(hydroshare_id)
        else:
            reach_id = reach_id
      # Write the GoogleSQL query for extracting data using reach_id
        query = f"""
            {SELECTion_clause}
            FROM
                `bigquery-public-data.national_water_model.stream_network` AS streams
            WHERE
                station_id IN UNNEST(@reach_id_param)
                {stream_order_criteria}
            {order_status}
            """
        # Configure the query job with the reach_id parameter
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter(
                    "reach_id_param", 
                    "INT64", 
                    reach_id),],)
    ## Raise error when the parameters are not sufficient to run a query
    else:
        raise HTTPException(status_code=400, detail='Your supplied parameters are not sufficient to run a query. Kindly try at least one of the parameters - bounding_box, geography, lat and lon, hydroshare_id, reach_id')
    # Return the formulated query and the corresponding job configuration
    return query, job_config

# Extract reach_id(s) from the given parameters
def reach_id_extractor(reach_id, hydroshare_id,
                   bounding_box, geom_filter, lon, lat,
                   with_buffer, lowest_stream_order, ordered):
    """ Extract a list of reaches corresponding to the given parameters"""
    # When the filtering is based on a spatial extent defined by bounding_box, geom_filter, or lat and lon parameters
    if bounding_box or geom_filter or (lat and lon) or lowest_stream_order:
        # Obtain the geometry query to extract reach_id(s) only
        subquery_output_status = 'id_only'
        geometry_subquery, job_config = geometry_query(reach_id, hydroshare_id,
                                        bounding_box, geom_filter, lon, lat,
                                        with_buffer, lowest_stream_order, ordered,
                                        output_status=subquery_output_status)
        # Run the geometry query to extract reach_id(s) corresponding to the given spatial filtering parameters
        response_data = run_query(geometry_subquery, job_config=job_config, endpoint='geometries')

        # Create a list of JSON objects with the selected columns
        results = []
        for row in response_data:
        # Convert the BigQuery Row object to a dictionary
            json_obj = dict(row.items())
            results.append(json_obj)
        # Extract reach_id(s) from the results
        reaches = list()
        for station_id_dict in results:
            reaches.append(str(station_id_dict['reach_id']))
    # Extract reach_id(s) from the hydroshare_id
    elif hydroshare_id:
        results = hydroshare_url_to_reach_id(hydroshare_id)
        # Extract reach_id(s) from the results
        reaches = list()
        for reach_id in results:
            reaches.append(reach_id)
    # Map reach_id(s) directly
    elif reach_id:
        reaches = reach_id
    # Raise error when the parameters are not sufficient to run a query
    else:
        raise HTTPException(status_code=400, detail='Your supplied parameters are not sufficient to run a query. Kindly try at least one of the parameters - bounding_box, geography, lat and lon, hydroshare_id, reach_id')
    # Returns a list of reach_id(s) corresponding to the given parameters
    return reaches

# Extract reach_id(s) and geometry from the given parameters
def reach_id_geom_extractor(reach_id, hydroshare_id,
                            bounding_box, geom_filter, lon, lat,
                            with_buffer, lowest_stream_order, ordered):
    """ Extract a list of (reach_id, geometry) tuples"""
    # Obtain the geometry query to extract reach_id(s) and geometry
    subquery_output_status = 'geom_only'
    geometry_subquery, job_config = geometry_query(reach_id, hydroshare_id,
                                                    bounding_box, geom_filter, lon, lat,
                                                    with_buffer, lowest_stream_order, ordered,
                                                    output_status=subquery_output_status)
    # Run the geometry query to extract reach_id(s) and geometry corresponding to the given parameters
    response_data = run_query(geometry_subquery, job_config=job_config, endpoint='geometries')

    # Create a list of JSON objects with the selected columns
    results_dict = {}
    for row in response_data:
    # Convert the BigQuery Row object to a dictionary
        json_obj = dict(row.items())
        id_geom_tup = (json_obj['reach_id'], json_obj['geometry'])
        results_dict[id_geom_tup[0]] = id_geom_tup[1]
    # Returns a dictionary with reach_id as key and geometry as value corresponding to the given parameters
    return results_dict

# Extract geometry for a given HUC
def huc_to_geom_extractor(huc):
    """ Extract geometry for a given HUC """
    # Formulate the query to extract geometry for the given HUC
    huc_geom_query = f"""
        SELECT *
        FROM
            `nwm-retro-migration.national_water_model.huc_data`
        WHERE
            huc = '{huc}'
    """
    # Configure the query job
    job_config = bigquery.QueryJobConfig()
    # Run the query to extract geometry for the given HUC
    huc_data_response = next(run_query(huc_geom_query, job_config=job_config, endpoint='huc_data'))
    # Convert the response to a dictionary with huc as key and geometry as value and return it
    huc_data = dict(huc_data_response.items())
    return huc_data

# Extract reach_id(s) corresponding to USGS gage_id(s)
def gages_to_reach_id_extractor(gage_id):
    """ Extract reach_id(s) corresponding to USGS gage_id(s)"""
    # Formulate the query to extract reach_id(s) for the given gage_id(s)
    gage_data_query = """
    SELECT *
    FROM
        `nwm-retro-migration.national_water_model.gages_data`
    WHERE
        gage_id IN UNNEST(@gage_id_param)
    """
    gage_id = tuple(gage_id)
    # Configure the query job with the gage_id parameter
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "gage_id_param", 
                "STRING", 
                gage_id  # This must be a Python list
            ),
        ]
    )
    # Run the query to extract reach_id(s) for the given gage_id(s)
    gage_data_response = run_query(gage_data_query, job_config=job_config, endpoint='gages_data')
    gage_data = gage_data_response.to_dataframe() 
    # Return the gage data as a DataFrame, or raise an HTTPException if no gage data is found for the given gage_id(s)
    if gage_data.empty:
        raise HTTPException(status_code=200, detail=f"No gage found for the provided gage_id(s). Please check your input and try again.")
    return gage_data
    
# Define a function to merge multiple JobConfig objects into one
def merge_job_configs(*configs):
    """Merge multiple JobConfigs"""
    merged = {}
    for config in configs:
        merged.update(config.to_api_repr())
    return bigquery.QueryJobConfig.from_api_repr(merged)

# Define a function to extract the latest reference time for a given forecast type
def get_latest_reference_time_for_forecast(forecast_type):
    # tables_dict = {'short_range': 'short_range_channel_rt',
    #                'medium_range': 'medium_range_channel_rt',
    #                'long_range': 'long_range_channel_rt'}
    # bq_dataset = 'bigquery-public-data.national_water_model'
    # table_id = tables_dict[forecast_type]
    # latest_bqtable_update_time_query = f"""
    # SELECT
    #     TIMESTAMP_MILLIS(last_modified_time) AS last_modified_timestamp
    # FROM `{bq_dataset}.__TABLES__`
    # WHERE table_id = '{table_id}';
    # """
    # job_config = bigquery.QueryJobConfig(
    #     use_query_cache=False
    # )
    # latest_bqtable_update_time = run_query(latest_bqtable_update_time_query, job_config, endpoint='forecasts')
    # latest_update_time = latest_bqtable_update_time.to_dataframe()['last_modified_timestamp'][0].replace(tzinfo=timezone.utc)
    # Consider the current time as the latest update time to avoid delay
    latest_update_time = datetime.now(timezone.utc)
    # Grab the month string of the latest update time
    latest_update_month_str = latest_update_time.strftime("%Y%m")
    # Grab the date string of the latest update time
    latest_update_date_str = latest_update_time.strftime("%Y%m%d")
    # Define the GCS bucket containing NWM parquets
    bucket_name = "national-water-model-parq"
    # Track and manage retries
    run_again = True
    run_counter = 0
    # Loop to check for the latest reference time
    while run_again == True:
        # For first three tries, limit to day granularity
        if run_counter < 3:
            folder = f"channel_rt/{forecast_type}/nwm.{latest_update_date_str}"
        # After three tries, expand to month granularity to find the latest reference time
        else:
            folder = f"channel_rt/{forecast_type}/nwm.{latest_update_month_str}"
        # List all blobs in the folder
        blobs = list(storage_client.list_blobs(bucket_name, prefix=folder))
        # Get the last blob appear in ascending order, which corresponds to the latest reference time
        if blobs:
            latest_rf_blob_name = max(blobs, key=lambda b: b.name).name
        else:
            latest_rf_blob_name = None
        if latest_rf_blob_name is not None:
            # Rewrite the latest reference time string on required format
            latest_reference_time_raw = latest_rf_blob_name.split('/')[-1].split('.')
            latest_reference_time_str = f"{latest_reference_time_raw[1][:4]}-{latest_reference_time_raw[1][4:6]}-{latest_reference_time_raw[1][6:]}"+ " "+ latest_reference_time_raw[2][1:3] + ":00:00 UTC"
            run_again = False
        # If no blob is found, keep looking back until find the latest reference time
        else:
            # Look back one day at a time for the first three tries
            if run_counter < 3:
                latest_update_time = latest_update_time - relativedelta(days=1)
                latest_update_date_str = latest_update_time.strftime("%Y%m%d")
            # Look back one month at a time after that to find the latest reference time
            else:
                latest_update_time = latest_update_time - relativedelta(months=1)
                latest_update_month_str = latest_update_time.strftime("%Y%m")
            run_counter += 1
    # Return the latest reference time string
    return latest_reference_time_str

# Define a function to extract the latest reference time for forecasts based on the given datasets parameter
def extract_latest_time_for_forecasts(datasets):
    # Get the latest reference time for short range forecast
    if datasets is None or 'forecasts_short_range' in datasets:
        srf_latest_time = get_latest_reference_time_for_forecast('short_range')
    else:
        srf_latest_time = None
    # Get the latest reference time for medium range forecast
    if datasets is None or 'forecasts_medium_range' in datasets:
        mrf_latest_time = get_latest_reference_time_for_forecast('medium_range')
    else:
        mrf_latest_time = None
    # Get the latest reference time for long range forecast
    if datasets is None or 'forecasts_long_range' in datasets:
        lrf_latest_time = get_latest_reference_time_for_forecast('long_range')
    else:
        lrf_latest_time = None
    # Return the latest reference time for each forecast type as a tuple
    return srf_latest_time, mrf_latest_time, lrf_latest_time

# Define a dictionary to hold endpoint information for metadata generation
endpoint_information_dict = {
    "geometries": ["This endpoint allows users to retrieve the geometries of stream reaches.", 
                   "bigquery-public-data.national_water_model.stream_network"],
    "analyses-assim": ["Access the analysis-assimilation simulation data for one or more of its offset runs against one or more reaches.", 
                       "bigquery-public-data.national_water_model.analysis_assim_channel_rt"],
    "forecasts": ["Access the forecast configuration data for one of the three forecast types at a certain reference time against one or more reaches.", 
                  {'long_range': "bigquery-public-data.national_water_model.long_range_channel_rt",
                   'medium_range': "bigquery-public-data.national_water_model.medium_range_channel_rt",
                   'short_range': "bigquery-public-data.national_water_model.short_range_channel_rt"}],
    "flow-metrics": ["Access the streamflow metrics (or indices or indicators) data derived from the NOAA National Water Model Retrospective 3.0 dataset, against one or more reaches.", 
                     "nwm-retro-migration.national_water_model.nwm_streamflow_indices"],
    "percentile-flows": ["Access the streamflow magnitude data corresponding to specified or all available percentiles, derived from the daily aggregation of the NOAA National Water Model Retrospective 3.0 dataset, against one or more reaches.", 
                         "nwm-retro-migration.national_water_model.nwm_streamflow_indices"],
    "return-periods": ["Access the flood return periods data of one or more reaches derived from the National Water Model retrospective dataset.", 
                       "bigquery-public-data.national_water_model.flood_return_periods"],
    "retrospectives": ["Access the retrospective simulation data from its version 3.0 run of NOAA National Water Model, against one or more reaches.", 
                       "nwm-retro-migration.national_water_model.nwm_retrospective_3_0"],
    "reaches/<reach_id>": ["Access a compiled set of data for a specific reach identified by its NWM reach_id, in geojson format.", 
                           {'geometry': "bigquery-public-data.national_water_model.stream_network", 
                            'analyses_assim': "bigquery-public-data.national_water_model.analysis_assim_channel_rt",
                            'forecasts_short_range': "bigquery-public-data.national_water_model.short_range_channel_rt",
                            'forecasts_medium_range': "bigquery-public-data.national_water_model.medium_range_channel_rt",
                            'forecasts_long_range': "bigquery-public-data.national_water_model.long_range_channel_rt",
                            'flow_metrics': "nwm-retro-migration.national_water_model.nwm_streamflow_indices",
                            'percentile_flows': "nwm-retro-migration.national_water_model.nwm_streamflow_indices",
                            'return_periods': "bigquery-public-data.national_water_model.flood_return_periods"
                            }],
    }

# Define a function to generate metadata for a given endpoint
def dynamic_metadata_generator(api_docs_object, endpoint, user_input_parameters, actual_request_parameters, output_fields, n_records, n_reaches, n_timesteps, output_model, output_format):
    """ Generate metadata for a given endpoint and query parameters """
    # Define initial empty metadata structures
    output_fields_schema = {}
    output_fields_lines = [""]
    # Collect the schema information for the output fields based on the output model
    for field, field_info in output_model.__fields__.items():
            if field in output_fields:
                if output_format in ['json', 'geojson', 'shapefile', 'geopackage']:
                    output_fields_schema[field] = {
                        "type": str(field_info.annotation),
                        "description": field_info.description,
                        "unit": field_info.json_schema_extra.get('unit', None) if field_info.json_schema_extra else None,
                    }
                elif output_format == 'csv':
                    field_type = str(field_info.annotation)
                    desc = field_info.description
                    unit_str = f" [{field_info.json_schema_extra.get('unit', None)}]" if field_info.json_schema_extra else ""
                    commented_line = f"#    {field}: {field_type}{unit_str} {desc}"
                    output_fields_lines.append(commented_line)
    # Compile the metadata information into a structured JSON format based on the requested output format
    if output_format in ['json', 'geojson', 'shapefile', 'geopackage']:
        metadata = {
            "api_title": api_docs_object['info']['title'],
            "api_summary": api_docs_object['info']['summary'],
            "api_version": api_docs_object['info']['version'],
            "endpoint": endpoint,
            "description": endpoint_information_dict.get(endpoint)[0],
            "immediate_data_source": endpoint_information_dict.get(endpoint)[1],
            "user_request_parameters": user_input_parameters,
            "applied_parameters": actual_request_parameters,
            "access_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "number_of_reaches": n_reaches,
            "number_of_timesteps": n_timesteps,
            "number_of_records": n_records,
            "output_fields": output_fields_schema
            }
    # For CSV format, compile the metadata information into a commented string format to be included at the beginning of the CSV file
    elif output_format == 'csv':
        metadata = f"""
# Metadata Information:
# --------------------------------------------------
# api_title: {api_docs_object['info']['title']},
# api_summary: {api_docs_object['info']['summary']},
# api_version: {api_docs_object['info']['version']},
# endpoint: {endpoint},
# description: {endpoint_information_dict.get(endpoint, ["No description available for this endpoint.", None])[0]},
# user_request_parameters: {user_input_parameters},
# applied_parameters: {actual_request_parameters},
# immediate_data_source: {endpoint_information_dict.get(endpoint)[1]},
# accessed_at: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")},
# number_of_reaches: {n_reaches},
# number_of_timesteps: {n_timesteps},
# number_of_records: {n_records},
# output_fields: {"\n".join(output_fields_lines)}
# -----------------DATA-STARTS-BELOW----------------\n"""
    # Return the curated metadata
    return metadata

# Define a function to generate static metadata for a given endpoint based on the API documentation and the input/output models
def static_metadata_generator(api_docs_object, endpoint, input_model, output_model):
    """ Generate metadata for a given endpoint and query parameters """
    # Collect the schema information for the output fields based on the output model
    output_fields_schema = {}
    for field, field_info in output_model.__fields__.items():
                    output_fields_schema[field] = {
                        "type": str(field_info.annotation),
                        "description": field_info.description,
                        "unit": field_info.json_schema_extra.get('unit', None) if field_info.json_schema_extra else None,
                        "example": field_info.examples[0] if field_info.examples else None
                    }
    # Collect the schema information for the input fields based on the input model
    input_fields_schema = {}
    for field, field_info in input_model.model_fields.items():
                    input_fields_schema[field] = {
                        "type": str(field_info.annotation),
                        "description": field_info.default.description if field_info.default and hasattr(field_info.default, 'description') else None,
                        "default": field_info.default.default if field_info.default and hasattr(field_info.default, 'examples') else None,
                        "example": field_info.default.examples[0] if field_info.default and hasattr(field_info.default, 'examples') and field_info.default.examples else None
                    }
    # Curate the metadata information
    metadata = {
        "api_title": api_docs_object['title'],
        "api_summary": api_docs_object['summary'],
        "api_version": api_docs_object['version'],
        "endpoint": endpoint,
        "description": endpoint_information_dict.get(endpoint)[0],
        "immediate_data_source": endpoint_information_dict.get(endpoint)[1],
        "input_parameters": input_fields_schema,
        "output_fields": output_fields_schema
        }
    # Return the curated metadata
    return metadata

# Define a function to convert the query response to a formatted API response
def query_response_to_api_response(query_response, query_params, caller_endpoint, api_docs, OutputModel, user_params, geometries_dict):
    """ Convert the query response to a formatted API response based on the requested output format and caller endpoint. """
    # Convert the query response to a Pandas DataFrame if it's not already in that format
    if isinstance(query_response, pd.DataFrame):
        results_df = query_response
    else:
        # Convert the query response to a Pandas DataFrame
        results_df = query_response.to_dataframe()
    # Generate some metadata from the retrieved data
    if query_params.metadata is True:
        request_params, output_fields, n_records, n_reaches = (query_params.model_dump(), results_df.columns.tolist(), results_df.shape[0], results_df['reach_id'].nunique())
        if caller_endpoint in ['analyses-assim', 'retrospectives', 'forecasts']:
            n_timesteps = results_df['time'].nunique() if 'time' in results_df.columns else None
        else:
            n_timesteps = None
        metadata = dynamic_metadata_generator(api_docs, caller_endpoint, user_params, request_params, output_fields, n_records, n_reaches, n_timesteps, OutputModel, query_params.output_format)
    else:
        metadata = None
    # Add geometry to the results DataFrame if the requested output format requires geometry
    if query_params.output_format in ['geojson', 'shapefile', 'geopackage'] and 'geometry' not in results_df.columns and 'reach_id' in results_df.columns:
        if 'geometry' not in results_df.columns:
            results_df['geometry'] = results_df['reach_id'].map(geometries_dict)
    # Convert the time field to the requested time zone
    if 'time' in results_df.columns:
        results_df['time'] = results_df['time'].dt.tz_convert(query_params.time_zone).dt.strftime('%Y-%m-%dT%H:%M:%S%z')
        # Convert the reference_time field to the requested time zone
        if 'reference_time' in results_df.columns:
            results_df['reference_time'] = results_df['reference_time'].dt.tz_convert(query_params.time_zone).dt.strftime('%Y-%m-%dT%H:%M:%S%z')
    # Format the output based on the requested output format
    api_response = output_formatter(results_df, query_params.output_format, caller_endpoint, metadata)
    # Return the formatted API response
    return api_response

# Define a function to extract either reach_id_list or reach_id_geom_dict based on the requested output format
def get_reach_id_or_geometry(query_params):
    if query_params.output_format in ['shapefile', 'geojson', 'geopackage']:
        # Extract both reach_id and geometry for the given parameters when the requested output format requires geometry
        reach_id_geom_dict = reach_id_geom_extractor(reach_id=query_params.reach_id, 
                                                    hydroshare_id=query_params.hydroshare_id,
                                                    bounding_box=query_params.bounding_box, 
                                                    geom_filter=query_params.geom_filter, 
                                                    lon=query_params.lon, lat=query_params.lat,
                                                    with_buffer=query_params.with_buffer, 
                                                    lowest_stream_order=query_params.lowest_stream_order, 
                                                    ordered=query_params.ordered)

        reach_id_list = list(reach_id_geom_dict.keys())
    elif query_params.output_format in ['json', 'csv']:
        # Extract only reach_id_list for non-geometry output formats
        reach_id_list = reach_id_extractor(reach_id=query_params.reach_id, 
                                    hydroshare_id=query_params.hydroshare_id,
                                    bounding_box=query_params.bounding_box, 
                                    geom_filter=query_params.geom_filter, 
                                    lon=query_params.lon, lat=query_params.lat,
                                    with_buffer=query_params.with_buffer, 
                                    lowest_stream_order=query_params.lowest_stream_order, 
                                    ordered=query_params.ordered)
        reach_id_geom_dict = None
    # Return the extracted reach_id_list and reach_id_geom_dict
    return reach_id_list, reach_id_geom_dict
        
# Define a function to generate the API response for the reachwise endpoint
def reachwise_response_generator(geojson_feature_dict, query_params, caller_endpoint, api_docs, OutputModel, user_params):
    # Embed metadata
    if query_params.metadata is True:
        n_records = dict()
        for key in geojson_feature_dict['properties'].keys():
            n_record = len(geojson_feature_dict['properties'][key]) if isinstance(geojson_feature_dict['properties'][key], list) else 1
            n_records[key] = n_record
        n_timesteps = n_records
        request_params, output_fields, n_records, n_reaches = (query_params.model_dump(), geojson_feature_dict['properties'].keys(), n_records, 1)
        metadata = dynamic_metadata_generator(api_docs, caller_endpoint, user_params, request_params, output_fields, n_records, n_reaches, n_timesteps, OutputModel, 'geojson')
        geojson_feature_dict['metadata'] = metadata
    else:
        metadata = None
    # Return the modified GeoJSON feature dictionary
    return geojson_feature_dict

# Define a custom APIRoute class to scrub the API key
class SecureRoute(APIRoute):
    def get_route_handler(self) -> Callable:
        original_handler = super().get_route_handler()
        async def custom_handler(request: Request) -> Any:
            params_dict = dict(request.query_params)
            # Extract and scrub the API key
            _ = params_dict.pop("key", None)
            # Update the request scope without API key
            request.scope["query_string"] = str(QueryParams(params_dict)).encode("utf-8")
            # Proceed with the original handler
            return await original_handler(request)
        # Return the custom handler that includes API key scrubbing and extra parameter validation
        return custom_handler