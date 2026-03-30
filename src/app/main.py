#####################################################
## Importing Necessary Python Libraries
#####################################################

# Import libraries required for data processing
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json

# Import libraries associated with FastAPI and BIGQUERY
from fastapi import FastAPI, HTTPException, Request, Depends, status, Security
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.openapi.utils import get_openapi
from fastapi.security.api_key import APIKeyQuery
from google.cloud import bigquery

# Import libraries for data validation and modeling
from pydantic import ValidationError
from app.validator_models import GeometriesWithoutTimeParams, ReturnPeriodsParams, AnalysesAssimParams, ForecastsParams, RetrospectivesParams, FlowMetricsParams, FlowPercentilesParams, ReachesParams
from app.validator_models import GeometriesOutputModel, ReturnPeriodsOutputModel, FlowMetricsOutputModel, FlowPercentilesOutputModel, AnalysesAssimOutputModel, ForecastsOutputModel, RetrospectivesOutputModel, ReachesOutputModel
from app.utils import SecureRoute
from app.utils import geometry_query, run_query, bytes_to_be_processed, reach_id_geom_extractor, huc_to_geom_extractor, gages_to_reach_id_extractor
from app.utils import static_metadata_generator, query_response_to_api_response, get_reach_id_or_geometry
from app.utils import make_serializable, reachwise_response_generator


#####################################################
## API Details and Hard Limits
#####################################################
api_details = {'title': "CIROH National Water Model API",
               'version': "2.0.0",
               'summary': "This is the version 2.0.0 of the CIROH National Water Model API with new endpoints, derived datasets, and enhanced capabilities.",
               'description': "This API dedicatedly serves National Water Model (NWM) data leveraging the efficient retrieval from corresponding BigQuery datasets. It includes endpoints for accessing natively generated operational ('analyses-assim', 'forecasts') and retrospective ('retrospectives') datasets, derived data products ('flow-metrics', 'percentile-flows' and 'return-periods'), flowline geometry ('geometries') data, and reach-based data compilation. Beside reach identifier based extraction, this API also supports geospatial queries, allowing users to define spatial boundaries, location, or alternative identifiers for their data requests. Moreover, the API can extensively validate user inputs with customized error messages and can serve static and dynamic metadata. Please refer to the individual endpoint documentation for more details on how to use each of them to find your required NWM data."
               }
static_metadata_dict = {'geometries': static_metadata_generator(api_details, 'geometries', GeometriesWithoutTimeParams, GeometriesOutputModel),
                        'flow-metrics': static_metadata_generator(api_details, 'flow-metrics', FlowMetricsParams, FlowMetricsOutputModel),
                        'percentile-flows': static_metadata_generator(api_details, 'percentile-flows', FlowPercentilesParams, FlowPercentilesOutputModel),
                        'analyses-assim': static_metadata_generator(api_details, 'analyses-assim', AnalysesAssimParams, AnalysesAssimOutputModel),
                        'return-periods': static_metadata_generator(api_details, 'return-periods', ReturnPeriodsParams, ReturnPeriodsOutputModel),
                        'forecasts': static_metadata_generator(api_details, 'forecasts', ForecastsParams, ForecastsOutputModel),
                        'retrospectives': static_metadata_generator(api_details, 'retrospectives', RetrospectivesParams, RetrospectivesOutputModel),
                        'reaches': static_metadata_generator(api_details, 'reaches/<reach_id>', ReachesParams, ReachesOutputModel)
                        }

HARD_LIMITS_ON_BYTES_TO_BE_PROCESSED = {
    'geometries': 10e+9,
    'return-periods': 10e+9,
    'flow-metrics': 10e+9,
    'percentile-flows': 10e+9,
    'analyses-assim': 4e+13,
    'forecasts': 4e+13,
    'retrospectives': 4e+13,
}

ALLOWED_PARAMS = {
    '/': set(),
    'docs': set(),
    'openapi.json': set(),
    'geometries': set(GeometriesWithoutTimeParams.model_fields.keys()),
    'return-periods': set(ReturnPeriodsParams.model_fields.keys()),
    'flow-metrics': set(FlowMetricsParams.model_fields.keys()),
    'percentile-flows': set(FlowPercentilesParams.model_fields.keys()),
    'analyses-assim': set(AnalysesAssimParams.model_fields.keys()),
    'forecasts': set(ForecastsParams.model_fields.keys()),
    'retrospectives': set(RetrospectivesParams.model_fields.keys()),
    'reaches': set(ReachesParams.model_fields.keys())
}

#####################################################
## Setting Up the API Application Instance
#####################################################

# Create an app instance of the class FastAPI
app = FastAPI()

# Implement the custom route class
app.router.route_class = SecureRoute

# Define the API key query parameter for API key-based authentication with the Gateway
api_key_scheme = APIKeyQuery(name="key", auto_error=False)

# Customize the documentation page as per the OpenAPI framework
def openapi_docs():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title=api_details['title'],
        version=api_details['version'],
        summary=api_details['summary'],
        description=api_details['description'],
        routes=app.routes,
    )
    openapi_schema["info"]["x-logo"] = {
        "url": "https://ciroh.ua.edu/wp-content/uploads/2022/08/CIROHLogo_200x200.png"
    }
    app.openapi_schema = openapi_schema
    return app.openapi_schema

# Add the custom documentation in the generated API application
app.openapi = openapi_docs

# Implement a middleware to deny extra parameters in the user request
@app.middleware("http")
async def validate_model_params(request: Request, call_next):
    # Extract incoming query parameter keys
    incoming_keys = set(request.query_params.keys())
    # Determine the allowed keys based on the endpoint being accessed
    match request.url.path:
        case "/":
            allowed_keys = ALLOWED_PARAMS['/']
        case "/docs":
            allowed_keys = ALLOWED_PARAMS['docs']
        case "/openapi.json":
            allowed_keys = ALLOWED_PARAMS['openapi.json']
        case "/forecasts":
            allowed_keys = ALLOWED_PARAMS['forecasts']
        case "/analyses-assim":
            allowed_keys = ALLOWED_PARAMS['analyses-assim']
        case "/retrospectives":
            allowed_keys = ALLOWED_PARAMS['retrospectives']
        case "/flow-metrics":
            allowed_keys = ALLOWED_PARAMS['flow-metrics']
        case "/percentile-flows":
            allowed_keys = ALLOWED_PARAMS['percentile-flows']
        case "/return-periods":
            allowed_keys = ALLOWED_PARAMS['return-periods']
        case "/geometries":
            allowed_keys = ALLOWED_PARAMS['geometries']
        case _ if request.url.path.startswith("/reaches/"):
            allowed_keys = ALLOWED_PARAMS['reaches']
    # Add the API key parameter to the allowed keys for all endpoints
    allowed_keys.add("key")
    # Identify any extra keys that are not allowed
    extra_keys = incoming_keys - allowed_keys
    # If there are extra keys, raise an HTTPException
    if extra_keys:
        raise HTTPException(
            status_code=400, 
            detail=f"Extra parameters not allowed: {extra_keys}"
        )
    # If there are no extra keys, proceed with the request
    return await call_next(request)


#####################################################
## Defining and Redirecting the Base endpoint
#####################################################

# Create path operation decorator for the DOCUMENTATION API
@app.get("/")

# Define the ROOT function
def root():
    # Redirect the base endpoint to the documentation (Swagger UI) page
    return RedirectResponse("/docs")


#####################################################
## Defining the FORECAST API Endpoint
#####################################################

# Create path operation decorator for the FORECAST API
@app.get("/forecasts")

# Define the FORECAST function
def forecast_configuration_data(user_request: Request, 
                                forecast_params: ForecastsParams = Depends(), 
                                _ = Security(api_key_scheme)):
    """
    Access the forecast configuration data for one of the three forecast types at a certain reference time 
    against one or more reaches from the National Water Model operational dataset either in geospatial
    (geojson or geopackage) or structured (csv or json) format by filtering the data for one or more of 
    the available ensemble members, as well as by filtering the reaches through geospatial location, extent, 
    or direct identifier-based parameters.
    """
    endpoint = 'forecasts'
    
    # Grab user requested parameters
    user_params = user_request.query_params
    
    # Return static metadata when there are no query parameters in the request
    if not user_params:
        metadata = static_metadata_dict.get(endpoint)
        return JSONResponse(content=metadata)
    
    else:
        huc = forecast_params.huc
        # Transfer the huc code (if provided) to geometry filter in the input parameters
        if huc:  
            huc_data = huc_to_geom_extractor(huc)
            forecast_params.geom_filter = (huc_data['geometry'], 'wkt')
            forecast_params.huc = (huc, huc_data['hu_name'])
        # Transfer the gage_id (if provided) to reach_id in the input parameters
        if forecast_params.gage_id:
            gages_data = gages_to_reach_id_extractor(forecast_params.gage_id)
            forecast_params.reach_id = gages_data['reach_id'].tolist()
            forecast_params.gage_id = gages_data.to_dict(orient='records')

        # Map forecast type to BigQuery table
        forecast_table_dict = {
            'long_range': 'bigquery-public-data.national_water_model.long_range_channel_rt',
            'medium_range': 'bigquery-public-data.national_water_model.medium_range_channel_rt',
            'short_range': 'bigquery-public-data.national_water_model.short_range_channel_rt'}

        # Get the forecast table based on the forecast type
        forecast_table = forecast_table_dict.get(forecast_params.forecast_type)

        # Extract reach_id list and geometry dictionary based on the input parameters
        reach_id_list, reach_id_geom_dict = get_reach_id_or_geometry(forecast_params)

        # Extract the reference time for the forecast data query
        reference_time = forecast_params.reference_time
        
        # Write query if ensemble parameter is provided
        ensemble = forecast_params.ensemble
        if ensemble:
            query = f"""
                    SELECT
                        feature_id AS reach_id,
                        reference_time,
                        time,
                        ensemble,
                        streamflow,
                        velocity
                    FROM
                        `{forecast_table}`
                    WHERE
                        feature_id IN UNNEST(@reach_id_list)
                        AND reference_time = @reference_time
                        AND ensemble IN UNNEST(@ensemble_list)
                    """
            #  Add the sorting statement if ordered is True
            if forecast_params.ordered:
                query += "    ORDER BY reach_id, reference_time, time;"
            else:
                query += ";"
            # Configure the query parameters for the forecast data query with ensemble filtering
            job_config = bigquery.QueryJobConfig()
            job_config.query_parameters = [
                bigquery.ArrayQueryParameter("reach_id_list", "INT64", reach_id_list),
                bigquery.ScalarQueryParameter("reference_time", "TIMESTAMP", reference_time),
                bigquery.ArrayQueryParameter("ensemble_list", "INT64", ensemble)
            ]
        # Write query for the average ensemble (default) case
        else:
            query = f"""
                    SELECT
                        feature_id AS reach_id,
                        reference_time,
                        time,
                        'average' AS ensemble,
                        AVG(streamflow) AS streamflow,
                        AVG(velocity) AS velocity
                    FROM
                        `{forecast_table}`
                    WHERE
                        feature_id IN UNNEST(@reach_id_list)
                        AND reference_time = @reference_time
                    GROUP BY
                        feature_id, reference_time, time
                    """  
            #  Add the sorting statement if ordered is True      
            if forecast_params.ordered:
                query += "    ORDER BY reach_id, reference_time, time;"
            else:
                query += ";"
            # Configure the query parameters for the forecast data query without ensemble filtering
            job_config = bigquery.QueryJobConfig()
            job_config.query_parameters = [
                bigquery.ArrayQueryParameter("reach_id_list", "INT64", reach_id_list),
                bigquery.ScalarQueryParameter("reference_time", "TIMESTAMP", reference_time)
            ]
        
        # Raise error when the data to processed for the query exceeds limit
        if bytes_to_be_processed(query, job_config, endpoint='forecasts') > HARD_LIMITS_ON_BYTES_TO_BE_PROCESSED[endpoint]: 
            raise HTTPException(status_code=413, detail=f'You have exceeded the allowable limit on processed data of {HARD_LIMITS_ON_BYTES_TO_BE_PROCESSED[endpoint]} bytes against your query. Please try again with reduced spatial or time extent of data')
        
        # Run the SQL query and retrieve data response from BigQuery client
        query_response = run_query(query, job_config=job_config, endpoint='forecasts')
        
        # Process the query response and return the API response in the specified output format
        return query_response_to_api_response(query_response, forecast_params, endpoint, openapi_docs(), ForecastsOutputModel, user_params, reach_id_geom_dict)


#####################################################
## Defining the ANALYSIS-ASSIMILATION API Endpoint
#####################################################

# Create path operation decorator for the Analysis-Assimilation API Endpoint
@app.get("/analyses-assim")

# Define the Analysis-Assimilation API function
def analysis_assimilation_configuration_data(user_request: Request, 
                                             aa_params: AnalysesAssimParams = Depends(),
                                             _ = Security(api_key_scheme)):
    """
    Access the analysis-assimilation simulation data for one or more of its offset runs against one or
    more reaches from the National Water Model operational dataset either in geospatial (geojson or geopackage) 
    or structured (csv or json) format by filtering the data based on time window and run offset,
    as well as by filtering the reaches through geospatial location, extent, or direct identifier-based 
    parameters.
    """
    endpoint = 'analyses-assim'
    # Grab user requested parameters
    user_params = user_request.query_params
    
    # Return static metadata when there are no query parameters in the request
    if not user_params:
        metadata = static_metadata_dict.get(endpoint)
        return JSONResponse(content=metadata)
    else:
        huc = aa_params.huc
        # Transfer the huc code (if provided) to geometry filter in the input parameters
        if huc:  
            huc_data = huc_to_geom_extractor(huc)
            aa_params.geom_filter = (huc_data['geometry'], 'wkt')
            aa_params.huc = (huc, huc_data['hu_name'])
        # Transfer the gage_id (if provided) to reach_id in the input parameters
        if aa_params.gage_id:
            gages_data = gages_to_reach_id_extractor(aa_params.gage_id)
            aa_params.reach_id = gages_data['reach_id'].tolist()
            aa_params.gage_id = gages_data.to_dict(orient='records')
        
        # Extract reach_id list and geometry dictionary based on the input parameters
        reach_id_list, reach_id_geom_dict = get_reach_id_or_geometry(aa_params)
        
        # Assemble the main body of the analysis-assimilation data SQL query
        query = """
            SELECT
                feature_id AS reach_id,
                time,
                streamflow,
                velocity
            FROM
                `bigquery-public-data.national_water_model.analysis_assim_channel_rt`
            WHERE
                feature_id IN UNNEST(@reach_id_list)
                AND forecast_offset = @run_offset
                AND time >= @start_time
                AND time <= @end_time
        """
        
        #  Add the sorting statement if order_by_comid is True
        if aa_params.ordered:
            query += "    ORDER BY reach_id, time;"
        else:
            query += ";"
        
        # Configure the query parameters for the analysis-assimilation data query
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("reach_id_list", "INT64", reach_id_list),
                bigquery.ScalarQueryParameter("run_offset", "INT64", aa_params.run_offset),
                bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", aa_params.start_time),
                bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", aa_params.end_time),
            ]
        )

        # Raise error when the data to processed for the query exceeds limit
        if bytes_to_be_processed(query, job_config, endpoint=endpoint) > HARD_LIMITS_ON_BYTES_TO_BE_PROCESSED[endpoint]: 
            raise HTTPException(status_code=413, detail=f'You have exceeded the allowable limit on processed data of {HARD_LIMITS_ON_BYTES_TO_BE_PROCESSED[endpoint]} bytes against your query. Please try again with reduced spatial or time extent of data')

        # Run the SQL query and retrieve data response from BigQuery client
        query_response = run_query(query, job_config=job_config, endpoint=endpoint)

        # Process the query response and return the API response in the specified output format
        return query_response_to_api_response(query_response, aa_params, endpoint, openapi_docs(), AnalysesAssimOutputModel, user_params, reach_id_geom_dict)


#####################################################
## Defining the Retrospective 3.0 API Endpoint
#####################################################

# Create path operation decorator for the Retrospective 3.0 API Endpoint
@app.get("/retrospectives")

# Define the Retrospective 3.0 API function
def retrospective_3_0_data(user_request: Request, 
                           rt3_params: RetrospectivesParams = Depends(),
                           _ = Security(api_key_scheme)):    
    """
    Access the retrospective simulation data from its version 3.0 run of NOAA National Water Model,
    against one or more reaches either in geospatial (geojson or geopackage) or structured (csv or json)
    format by filtering the data based on time window (start and end time), as well as by filtering the 
    reaches through geospatial location, extent, or direct identifier-based parameters.
    """
    endpoint = 'retrospectives'
    
    # Grab user requested parameters
    user_params = user_request.query_params
    
    # Return static metadata when there are no query parameters in the request
    if not user_params:
        metadata = static_metadata_dict.get(endpoint)
        return JSONResponse(content=metadata)
    
    else:
        huc = rt3_params.huc
        # Transfer the huc code (if provided) to geometry filter in the input parameters
        if huc:  
            huc_data = huc_to_geom_extractor(huc)
            rt3_params.geom_filter = (huc_data['geometry'], 'wkt')
            rt3_params.huc = (huc, huc_data['hu_name'])
            
        # Transfer the gage_id (if provided) to reach_id in the input parameters
        if rt3_params.gage_id:
            gages_data = gages_to_reach_id_extractor(rt3_params.gage_id)
            rt3_params.reach_id = gages_data['reach_id'].tolist()
            rt3_params.gage_id = gages_data.to_dict(orient='records')

        # Extract reach_id_list and geometries_dict from the input parameters
        reach_id_list, geometries_dict = get_reach_id_or_geometry(rt3_params)

        # Assemble the main body of the retrospective 3.0 data SQL query
        query = """
            SELECT
                feature_id AS reach_id,
                time,
                streamflow,
                velocity
            FROM
                `nwm-retro-migration.national_water_model.nwm_retrospective_3_0`
            WHERE
                feature_id IN UNNEST(@reach_id_list)
                AND time >= @start_time
                AND time <= @end_time
        """
        
        # Add the sorting statement if order_by_comid is True
        if rt3_params.ordered:
            query += "    ORDER BY reach_id, time;"
        else:
            query += ";"
        
        # Configure the query parameters for the retrospective 3.0 data query
        job_config = bigquery.QueryJobConfig()
        job_config.query_parameters = [
            bigquery.ArrayQueryParameter("reach_id_list", "INT64", reach_id_list),
            bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", rt3_params.start_time),
            bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", rt3_params.end_time)
        ]
        
        # Raise error when the data to processed for the query exceeds limit
        if bytes_to_be_processed(query, job_config, endpoint='retrospectives') > 10000e+9:
            raise HTTPException(status_code=413, detail='You have exceeded the allowable limit of processed data against your query. Please try again with reduced spatial or time extent of data')
        
        # Run the SQL query and retrieve data response from BigQuery client
        query_response = run_query(query, job_config=job_config, endpoint='retrospectives')

        # Process the query response and return the API response in the specified output format
        return query_response_to_api_response(query_response, rt3_params, endpoint, openapi_docs(), RetrospectivesOutputModel, user_params, geometries_dict)


#####################################################
## Defining the Streamflow Metrics API Endpoint
#####################################################

# Create path operation decorator for the STREAMFLOW METRICS API Endpoint
@app.get("/flow-metrics")

# Define the Streamflow Metrics API function
def streamflow_metrics_data(user_request: Request, 
                            metrics_params: FlowMetricsParams = Depends(),
                            _ = Security(api_key_scheme)):
    """
    Access the streamflow metrics (or indices or indicators) data derived from the NOAA National Water Model
    Retrospective 3.0 dataset, against one or more reaches either in geospatial (geojson or shapefile) or
    structured (csv or json) format by filtering down to specific or all available metrics, as well as by
    filtering the reaches through geospatial location, extent, or direct identifier-based parameters.
    """
    endpoint = 'flow-metrics'
    
    # Grab user requested parameters
    user_params = user_request.query_params
    
    # Return static metadata when there are no query parameters in the request
    if not user_params:
        metadata = static_metadata_dict.get(endpoint)
        return JSONResponse(content=metadata)
    
    else:
        huc = metrics_params.huc
        # Transfer the huc code (if provided) to geometry filter in the input parameters
        if huc:  
            huc_data = huc_to_geom_extractor(huc)
            metrics_params.geom_filter = (huc_data['geometry'], 'wkt')
            metrics_params.huc = (huc, huc_data['hu_name'])
        # Transfer the gage_id (if provided) to reach_id in the input parameters
        if metrics_params.gage_id:
            gages_data = gages_to_reach_id_extractor(metrics_params.gage_id)
            metrics_params.reach_id = gages_data['reach_id'].tolist()
            metrics_params.gage_id = gages_data.to_dict(orient='records')
        
        # Customize extracted fields based on metrics chosen
        requested_metrics = metrics_params.metrics
        selected_fields = "reach_id," + requested_metrics

        # Extract reach_id list and geometry dictionary based on the input parameters
        reach_id_list, reach_id_geom_dict = get_reach_id_or_geometry(metrics_params)

        # Assemble the main body of the streamflow indices data SQL query
        query = f"""
            SELECT
                    {selected_fields}
            FROM
                `nwm-retro-migration.national_water_model.nwm_streamflow_indices` AS streamflow_indices_table
            WHERE streamflow_indices_table.reach_id in UNNEST(@reach_id_list)
        """
        
        if metrics_params.ordered:
            #  Add the sorting statement if order_by_comid is True
            query += "    ORDER BY reach_id;"
        else:
            query += ";"
        
        # Configure the query parameters for the streamflow metrics data query
        job_config = bigquery.QueryJobConfig()
        job_config.query_parameters = [
            bigquery.ArrayQueryParameter("reach_id_list", "INT64", reach_id_list)
        ]

        # Raise error when the data to processed for the query exceeds limit
        if bytes_to_be_processed(query, job_config=job_config, endpoint='flow-metrics') > HARD_LIMITS_ON_BYTES_TO_BE_PROCESSED[endpoint]:
            raise HTTPException(status_code=413, detail='You have exceeded the allowable limit of processed data against your query. Please try again with reduced spatial or time extent of data')

        # Run the SQL query and retrieve data response from BigQuery client
        query_response = run_query(query, job_config=job_config, endpoint='flow-metrics')

        # Process the query response and return the API response in the specified output format
        return query_response_to_api_response(query_response, metrics_params, endpoint, openapi_docs(), FlowMetricsOutputModel, user_params, reach_id_geom_dict)


#####################################################
## Defining the Percentile Streamflows API Endpoint
#####################################################

# Create path operation decorator for the PERCENTILE STREAMFLOWS API Endpoint
@app.get("/percentile-flows")

# Define the Percentile Streamflows API function
def percentile_streamflows_data(user_request: Request, 
                                percentile_params: FlowPercentilesParams = Depends(),
                                _ = Security(api_key_scheme)):
    """
    Access the streamflow magnitude data corresponding to specified or all available percentiles, derived from 
    the daily aggregation of the NOAA National Water Model Retrospective 3.0 dataset, against one or more reaches
    either in geospatial (geojson or shapefile) or structured (csv or json) format by filtering the reaches through 
    geospatial location, extent, or direct identifier-based parameters.
    """
    endpoint = 'percentile-flows'
    
    # Grab user requested parameters
    user_params = user_request.query_params
    
    # Return static metadata when there are no query parameters in the request
    if not user_params:
        metadata = static_metadata_dict.get(endpoint)
        return JSONResponse(content=metadata)
    else:
        huc = percentile_params.huc
        # Transfer the huc code (if provided) to geometry filter in the input parameters
        if huc:  
            huc_data = huc_to_geom_extractor(huc)
            percentile_params.geom_filter = (huc_data['geometry'], 'wkt')
            percentile_params.huc = (huc, huc_data['hu_name'])
            
        # Transfer the gage_id (if provided) to reach_id in the input parameters
        if percentile_params.gage_id:
            gages_data = gages_to_reach_id_extractor(percentile_params.gage_id)
            percentile_params.reach_id = gages_data['reach_id'].tolist()
            percentile_params.gage_id = gages_data.to_dict(orient='records')
        
        # Extract reach_id list and geometry dictionary based on the input parameters
        reach_id_list, reach_id_geom_dict = get_reach_id_or_geometry(percentile_params)
        
        # Assemble the main body of the streamflow indices data query 
        query = """
            SELECT
                reach_id, nth_percentile_flows
                FROM
                `nwm-retro-migration.national_water_model.nwm_streamflow_indices` AS streamflow_indices_table
            WHERE streamflow_indices_table.reach_id in UNNEST(@reach_id_list) 
        """
        
        if percentile_params.ordered:
            #  Add the sorting statement if order_by_comid is True
            query += "    ORDER BY reach_id;"
        else:
            query += ";"
            
        # Configure the query parameters for the percentile streamflows data query
        job_config = bigquery.QueryJobConfig()
        job_config.query_parameters = [
            bigquery.ArrayQueryParameter("reach_id_list", "INT64", reach_id_list)
        ]
        
        # Raise error when the data to processed for the query exceeds limit
        if bytes_to_be_processed(query, job_config=job_config, endpoint=endpoint) > HARD_LIMITS_ON_BYTES_TO_BE_PROCESSED[endpoint]:
            raise HTTPException(status_code=413, detail=f'You have exceeded the allowable limit on processed data of {HARD_LIMITS_ON_BYTES_TO_BE_PROCESSED[endpoint]} bytes against your query. Please try again with reduced spatial or time extent of data')

        # Run the SQL query and retrieve data response from BigQuery client
        query_response = run_query(query, job_config=job_config, endpoint=endpoint)                         
        
        # Curate the percentile streamflows data from query response
        results_df = query_response.to_dataframe() 
        results_df.replace({np.nan: None}, inplace=True)
        percentiles_only_df = pd.DataFrame(results_df['nth_percentile_flows'].tolist(), 
                            columns=['min', 'perc_2', 'perc_5', 
                                    'perc_10', 'perc_20', 'perc_25', 
                                    'perc_30', 'perc_50', 'perc_75', 
                                    'perc_90', 'perc_95', 'perc_99', 
                                    'max'], 
                            index=results_df.index)
        if percentile_params.percentiles:
            select_cols = []
            col_mapper = {0: 'min', 2: 'perc_2', 5: 'perc_5', 
                        10: 'perc_10', 20: 'perc_20', 25: 'perc_25', 
                        30: 'perc_30', 50: 'perc_50', 75: 'perc_75', 
                        90: 'perc_90', 95: 'perc_95', 99: 'perc_99', 
                        100: 'max'}
            for col in percentile_params.percentiles:
                if col in col_mapper:
                    select_cols.append(col_mapper[col])
            final_results_df = pd.concat([results_df.drop('nth_percentile_flows', axis=1), percentiles_only_df.loc[:, select_cols]], axis=1)
        else:
            final_results_df = pd.concat([results_df.drop('nth_percentile_flows', axis=1), percentiles_only_df], axis=1)
        
        # Return the API response in the specified output format
        return query_response_to_api_response(final_results_df, percentile_params, endpoint, openapi_docs(), FlowPercentilesOutputModel, user_params, reach_id_geom_dict)


#####################################################
## Defining the RETURN PERIOD API Endpoint
#####################################################

# Create path operation decorator for the RETURN PERIOD API Endpoint
@app.get("/return-periods")

# Define the RETURN PERIOD API function
def flood_return_periods_data(user_request: Request, 
                              rp_params: ReturnPeriodsParams = Depends(),
                              _ = Security(api_key_scheme)):
    """
    Access the flood return periods data of one or more reaches derived from the National Water Model
    retrospective dataset either in geospatial (geojson or shapefile) or tabular (csv or json) format 
    by filtering the return periods from the six available return periods (2, 5, 10, 25, 50, and 100 year)
    as well as by filtering the reaches through geospatial location, extent, or direct identifier-based 
    parameters.
    """
    endpoint = 'return-periods'
    
    # Grab user requested parameters
    user_params = user_request.query_params
    
    # Return static metadata when there are no query parameters in the request
    if not user_params:
        metadata = static_metadata_dict.get(endpoint)
        return JSONResponse(content=metadata)
    
    else:
        huc = rp_params.huc
        # Transfer the huc code (if provided) to geometry filter in the input parameters
        if huc:  
            huc_data = huc_to_geom_extractor(huc)
            rp_params.geom_filter = (huc_data['geometry'], 'wkt')
            rp_params.huc = (huc, huc_data['hu_name'])
            
        # Transfer the gage_id (if provided) to reach_id in the input parameters
        if rp_params.gage_id:
            gages_data = gages_to_reach_id_extractor(rp_params.gage_id)
            rp_params.reach_id = gages_data['reach_id'].tolist()
            rp_params.gage_id = gages_data.to_dict(orient='records')
            
        # Customize extracted fields based on return_periods chosen
        selected_fields = "feature_id AS reach_id"
        tabs = "\t    "
        if rp_params.return_periods == None:
        # Extract all six return periods data by default
            selected_fields = (selected_fields +
                               (f",\n{tabs}return_period_2,\n{tabs}return_period_5," +
                                f"\n{tabs}return_period_10,\n{tabs}return_period_25," +
                                f"\n{tabs}return_period_50, \n{tabs}return_period_100"))
        else:
            # Extract only the listed return periods data
            requested_return_periods = rp_params.return_periods.split(",")
            for return_period in requested_return_periods:
                selected_fields = selected_fields + f",\n{tabs}return_period_{return_period}"

        # Curate the geoemtry subquery and configuration based on only reach_id or reach_id with geometry requirement as per the output format
        # Grab reach_id as well as geometry for spatial data output format
        if rp_params.output_format in ['geojson', 'shapefile']:
            subquery_output_status = 'geom_only'
            selected_fields += ", geometry"
        # Grab only reach_id for structured data output format as geometry is not required
        elif rp_params.output_format in ['json', 'csv']:
            subquery_output_status = 'id_only'
        geometry_subquery, job_config = geometry_query(reach_id=rp_params.reach_id, 
                                                   hydroshare_id=rp_params.hydroshare_id,
                                                   bounding_box=rp_params.bounding_box, 
                                                   geom_filter=rp_params.geom_filter, 
                                                   lon=rp_params.lon, lat=rp_params.lat,
                                                   with_buffer=rp_params.with_buffer, 
                                                   lowest_stream_order=rp_params.lowest_stream_order, 
                                                   ordered=rp_params.ordered,
                                                   output_status=subquery_output_status
                                                   )
        
        # Assemble the main body of the flood return-period data query
        query = f"""
            WITH selectedGeometry AS ({geometry_subquery})
            SELECT
                {selected_fields}
            FROM
                `bigquery-public-data.national_water_model.flood_return_periods` AS return_periods_table, selectedGeometry
            WHERE return_periods_table.feature_id in (selectedGeometry.reach_id)
        """

        if rp_params.ordered:
            #  Add the sorting statement if order_by_comid is True
            query += "    ORDER BY reach_id;"
        else:
            query += ";"

        # Raise error when the data to processed for the query exceeds limit
        if bytes_to_be_processed(query, job_config, 'return-periods') > HARD_LIMITS_ON_BYTES_TO_BE_PROCESSED[endpoint]:
            raise HTTPException(status_code=413, detail=f'You have exceeded the allowable limit on processed data of ({HARD_LIMITS_ON_BYTES_TO_BE_PROCESSED[endpoint]}) bytes against your query. Please try again with reduced spatial or time extent of data')

        # Run the SQL query and retrieve data response from BigQuery client
        response_data = run_query(query, job_config=job_config, endpoint='return-periods')
        
        # Process the query response and return the API response in the specified output format
        return query_response_to_api_response(response_data, rp_params, endpoint, openapi_docs(), ReturnPeriodsOutputModel, user_params, None)


#####################################################
## Defining the GEOMETRY API Endpoint
#####################################################

# Create path operation decorator for the Geometry API
@app.get("/geometries")

# Define the GEOMETRY API function
def reach_geometry_data(user_request: Request, 
                        geom_params: GeometriesWithoutTimeParams = Depends(),
                        _ = Security(api_key_scheme)): #TestedOK
    """
    Access the geometries data of one or more reaches from the National Water Model 
    either in geospatial (geojson or shapefile) or structured (csv or json) format by filtering 
    through geospatial location, extent, or direct identifier-based parameters.
    """
    endpoint = 'geometries'
    
    # Grab user requested parameters
    user_params = user_request.query_params
    
    # Return static metadata when there are no query parameters in the request
    if not user_params:
        metadata = static_metadata_dict.get(endpoint)
        return JSONResponse(content=metadata)
    
    else:
        huc = geom_params.huc
        # Transfer the huc code (if provided) to geometry filter in the input parameters
        if huc:  
            huc_data = huc_to_geom_extractor(huc)
            geom_params.geom_filter = (huc_data['geometry'], 'wkt')
            geom_params.huc = (huc, huc_data['hu_name'])
        
        # Transfer the gage_id (if provided) to reach_id in the input parameters
        if geom_params.gage_id:
            gages_data = gages_to_reach_id_extractor(geom_params.gage_id)
            geom_params.reach_id = gages_data['reach_id'].tolist()
            geom_params.gage_id = gages_data.to_dict(orient='records')
        
        # Formulate the geometry query and job configuration from the input parameters
        query_output_status = 'full'
        query, job_config = geometry_query(reach_id=geom_params.reach_id,
                                        hydroshare_id=geom_params.hydroshare_id,
                                        bounding_box=geom_params.bounding_box, 
                                        geom_filter=geom_params.geom_filter, 
                                        lon=geom_params.lon, 
                                        lat=geom_params.lat,
                                        with_buffer=geom_params.with_buffer, 
                                        lowest_stream_order=geom_params.lowest_stream_order, 
                                        ordered=geom_params.ordered,
                                        output_status=query_output_status
                                        )
        
        # Raise error when the data to processed for the query exceeds limit
        if bytes_to_be_processed(query, job_config, 'geometries') > HARD_LIMITS_ON_BYTES_TO_BE_PROCESSED[endpoint]:
            raise HTTPException(status_code=413, detail=f'You have exceeded the allowable limit of bytes to be processed ({HARD_LIMITS_ON_BYTES_TO_BE_PROCESSED[endpoint]}) against your query. Please try again with reduced spatial or time extent of data')
        
        # Run the SQL query and retrieve data response from BigQuery client
        query_response = run_query(query, job_config, 'geometries')
        
        # Process the query response and return the API response in the specified output format
        return query_response_to_api_response(query_response, geom_params, endpoint, openapi_docs(), GeometriesOutputModel, user_params, None)


@app.get("/reaches/{reach_id}")
def reachwise_compiled_data(user_request: Request, 
                            reach_params: ReachesParams = Depends(),
                            _ = Security(api_key_scheme)):
    """
    Access a compiled set of data for a specific reach identified by its NWM reach_id, in geojson format.
    The compiled data includes the reach geometry and can include one, more, or all of streamflow metrics,
    percentile flows, forecasts of different types, analysis-assimilation, and flood return periods data.
    The data is retrieved against a time of reference to look ahead for forecasts and look back for 
    analysis-assimilation data.
    """
    endpoint = 'reaches'
    user_params = user_request.query_params
    # Return static metadata when path variable reach_id is 0
    if reach_params.reach_id == 0:
        metadata = static_metadata_dict.get(endpoint)
        return JSONResponse(content=json.loads(json.dumps(metadata, default=make_serializable)))
    # Proceed with actual data retrieval when reach_id is not 0
    else:
        # Always extract the geometry for the reach_id
        reach_id_geom = reach_id_geom_extractor(reach_id=reach_params.reach_id, 
                                                        hydroshare_id=None,
                                                        bounding_box=None, 
                                                        geom_filter=None, 
                                                        lon=None, lat=None,
                                                        with_buffer=None, 
                                                        lowest_stream_order=None, 
                                                        ordered=None)
        geometry = reach_id_geom[reach_params.reach_id[0]]
        
        # Retrieve streamflow indices data when flow_metrics or flow_percentiles is included in the request
        if 'flow_metrics' in reach_params.include or 'flow_percentiles' in reach_params.include:
            metrics_query = """
                SELECT
                    *
                FROM
                    `nwm-retro-migration.national_water_model.nwm_streamflow_indices`
                WHERE
                    reach_id = @reach_id
            """
            ind_job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("reach_id", "INT64", reach_params.reach_id[0])
                ]
            )
            metrics_response = run_query(metrics_query, job_config=ind_job_config, endpoint='flow-metrics')
            metrics_df = metrics_response.to_dataframe()
            # Curate percentile_flows data from the retrieved streamflow indices data
            if 'flow_percentiles' in reach_params.include:
                percentiles = metrics_df['nth_percentile_flows'].tolist()[0]
                metrics_df = metrics_df.drop('nth_percentile_flows', axis=1)
                percentiles_df = pd.DataFrame([percentiles], columns=['min', 'perc_2', 'perc_5', 
                                                                    'perc_10', 'perc_20', 'perc_25', 
                                                                    'perc_30', 'perc_50', 'perc_75', 
                                                                    'perc_90', 'perc_95', 'perc_99', 
                                                                    'max'])
        # Retrieve flood return periods data when return_periods is included in the request
        if 'return_periods' in reach_params.include:
            rp_query = """
                SELECT
                    * EXCEPT(feature_id),
                FROM
                    `bigquery-public-data.national_water_model.flood_return_periods`
                WHERE
                    feature_id = @reach_id
            """
            rp_job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("reach_id", "INT64", reach_params.reach_id[0])
                ]
            )
            rp_response = run_query(rp_query, job_config=rp_job_config, endpoint='return-periods')
            rp_df = rp_response.to_dataframe()
        # Retrieve short range forecast data when forecasts_short_range is included in the request
        if 'forecasts_short_range' in reach_params.include:
            short_range_query = """
                SELECT
                    reference_time,
                    time,
                    streamflow,
                    velocity
                FROM
                    `bigquery-public-data.national_water_model.short_range_channel_rt`
                WHERE
                    feature_id = @reach_id
                    AND reference_time = @reference_time
            """
            srf_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("reach_id", "INT64", reach_params.reach_id[0]),
                    bigquery.ScalarQueryParameter("reference_time", "TIMESTAMP", reach_params.reference_time[0])
                ]
            )
            short_range_response = run_query(short_range_query, job_config=srf_config, endpoint='forecasts')
            short_range_df = short_range_response.to_dataframe()
            
        # Retrieve medium range forecast data when forecasts_medium_range is included in the request
        if 'forecasts_medium_range' in reach_params.include:
            medium_range_query = """
                SELECT
                    reference_time,
                    time,
                    streamflow,
                    velocity
                FROM
                    `bigquery-public-data.national_water_model.medium_range_channel_rt`
                WHERE
                    feature_id = @reach_id
                    AND reference_time = @reference_time
            """
            mrf_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("reach_id", "INT64", reach_params.reach_id[0]),
                    bigquery.ScalarQueryParameter("reference_time", "TIMESTAMP", reach_params.reference_time[1])
                ]
            )
            medium_range_response = run_query(medium_range_query, job_config=mrf_config, endpoint='forecasts')
            medium_range_df = medium_range_response.to_dataframe()
        # Retrieve long range forecast data when forecasts_long_range is included in the request
        if 'forecasts_long_range' in reach_params.include:
            long_range_query = """
                SELECT
                    reference_time,
                    time,
                    streamflow,
                    velocity
                FROM
                    `bigquery-public-data.national_water_model.long_range_channel_rt`
                WHERE
                    feature_id = @reach_id
                    AND reference_time = @reference_time
            """
            lrf_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("reach_id", "INT64", reach_params.reach_id[0]),
                    bigquery.ScalarQueryParameter("reference_time", "TIMESTAMP", reach_params.reference_time[2])
                ]
            )
            long_range_response = run_query(long_range_query, job_config=lrf_config, endpoint='forecasts')
            long_range_df = long_range_response.to_dataframe()
            
        # Retrieve analysis-assimilation data when analyses_assim is included in the request
        if 'analyses_assim' in reach_params.include:
            # start_time set to 30 days before the reference time for look back
            start_time = datetime.strptime(reach_params.reference_time[2], "%Y-%m-%d %H:%M:%S UTC") - timedelta(days=30)
            # end_time set to the reference time for look back
            end_time = datetime.strptime(reach_params.reference_time[2], "%Y-%m-%d %H:%M:%S UTC")
            aa_query = """
                SELECT
                    forecast_offset AS run_offset,
                    time,
                    streamflow,
                    velocity
                FROM
                    `bigquery-public-data.national_water_model.analysis_assim_channel_rt`
                WHERE
                    feature_id = @reach_id
                    AND time >= @start_time
                    AND time <= @end_time
            """
            aa_job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("reach_id", "INT64", reach_params.reach_id[0]),
                    bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", start_time),
                    bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", end_time)
                ]
            )
            aa_response = run_query(aa_query, job_config=aa_job_config, endpoint='analyses-assim')
            aa_df = aa_response.to_dataframe()
        # Convert the time columns to the requested time zone if it's not UTC
        if reach_params.time_zone != 'UTC':
            if 'analyses_assim' in reach_params.include:
                aa_df['time'] = aa_df['time'].dt.tz_convert(reach_params.time_zone).dt.strftime('%Y-%m-%dT%H:%M:%S%z')
            if 'forecasts_long_range' in reach_params.include:
                long_range_df['reference_time'] = long_range_df['reference_time'].dt.tz_convert(reach_params.time_zone).dt.strftime('%Y-%m-%dT%H:%M:%S%z')
                long_range_df['time'] = long_range_df['time'].dt.tz_convert(reach_params.time_zone).dt.strftime('%Y-%m-%dT%H:%M:%S%z')
            if 'forecasts_medium_range' in reach_params.include:
                medium_range_df['reference_time'] = medium_range_df['reference_time'].dt.tz_convert(reach_params.time_zone).dt.strftime('%Y-%m-%dT%H:%M:%S%z')
                medium_range_df['time'] = medium_range_df['time'].dt.tz_convert(reach_params.time_zone).dt.strftime('%Y-%m-%dT%H:%M:%S%z')
            if 'forecasts_short_range' in reach_params.include:
                short_range_df['reference_time'] = short_range_df['reference_time'].dt.tz_convert(reach_params.time_zone).dt.strftime('%Y-%m-%dT%H:%M:%S%z')
                short_range_df['time'] = short_range_df['time'].dt.tz_convert(reach_params.time_zone).dt.strftime('%Y-%m-%dT%H:%M:%S%z')
        
        # Construct the GeoJSON feature with the geometry and properties based on the included data types in the request
        geojson_feature = {
            "type": "Feature",
            "geometry": geometry,
            "properties": {'reach_id': reach_params.reach_id}
        }
        if 'flow_metrics' in reach_params.include:
            metrics = metrics_df.to_dict(orient='records')[0]
            metrics.pop('reach_id', None)
            geojson_feature["properties"]["flow_metrics"] = metrics
        if 'flow_percentiles' in reach_params.include:
            percentiles = percentiles_df.to_dict(orient='records')[0]
            geojson_feature["properties"]["flow_percentiles"] = percentiles
        if 'return_periods' in reach_params.include:
            rp_dict = rp_df.to_dict(orient='records')[0]
            geojson_feature["properties"]["return_periods"] = rp_dict
        if 'analyses_assim' in reach_params.include:
            aa_dict = aa_df.to_dict(orient='records')
            geojson_feature["properties"]["analyses_assim"] = aa_dict
        if 'forecasts_short_range' in reach_params.include:
            short_range_dict = short_range_df.to_dict(orient='records')
            geojson_feature["properties"]["forecasts_short_range"] = short_range_dict
        if 'forecasts_medium_range' in reach_params.include:
            medium_range_dict = medium_range_df.to_dict(orient='records')
            geojson_feature["properties"]["forecasts_medium_range"] = medium_range_dict
        if 'forecasts_long_range' in reach_params.include:
            long_range_dict = long_range_df.to_dict(orient='records')
            geojson_feature["properties"]["forecasts_long_range"] = long_range_dict
        
        # Format the geojson_feature and return the API response in geojson format
        geojson_feature = reachwise_response_generator(geojson_feature, reach_params, 'reaches/<reach_id>', openapi_docs(), ReachesOutputModel, user_params)
        # Generate the API response in geojson format with the compiled data for the reach_id and return it as the API response
        response = JSONResponse(content=json.loads(json.dumps(geojson_feature, default=make_serializable)))
        return response


# Define a custom exception handler for validation errors to format the error messages in a more readable way
@app.exception_handler(ValidationError)
async def validation_exception_handler(request, exc: ValidationError):
    errors = exc.errors()
    formatted_errors = []
    for error in errors:
        formatted_errors.append({
            "loc": error["loc"],
            "msg": error["msg"],
            "type": error["type"]
        })
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": formatted_errors}
    )


# Define a custom exception handler for HTTP exceptions to return the error messages in JSON format  
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )


# Define a custom exception handler for general exceptions to return a generic error message
@app.exception_handler(Exception)
async def general_exception_handler(request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc)}
    )
