#####################################################
## Import required libraries
#####################################################


from __future__ import annotations
from enum import Enum
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
from dateutil import parser as date_parser
from dateutil.relativedelta import relativedelta
import json
from shapely import wkb, wkt, geometry
from shapely.errors import ShapelyError
from datetime import datetime, timezone
import requests
from fastapi import Query, Path
import pytz
import re
from typing import Optional
from app.utils import extract_latest_time_for_forecasts, get_latest_reference_time_for_forecast


#####################################################
## Define Input Models
#####################################################


class StreamorderOptions(int, Enum):
    one = 1
    two = 2
    three = 3
    four = 4
    five = 5
    six = 6
    seven = 7
    eight = 8
    nine = 9
    ten = 10

class GeometriesParams(BaseModel):
    reach_id: Optional[str] = Field(Query(default=None, description="A string containing comma-separated National Water Model reach identifiers. Single reach_id is also allowed.",
                                            example="1891586,2927567,3134443,3589508",
                                            examples=["1891586,2927567,3134443,3589508", "1891586"]),)
    gage_id: Optional[str] = Field(Query(default=None, description="A string containing comma-separated USGS gage identifiers. Single gage_id is also allowed.",
                                          example="13309220,13042500,13295000",
                                          examples=["13309220,13042500,13295000", "13309220"]),)
    hydroshare_id: Optional[str] = Field(Query(default=None, description="A 32-character hexadecimal string representing the HydroShare resource identifier containing a JSON file with specific filename and format.",
                                         example="643dc03878704a30849536e302bdb2c0"),)
    bounding_box: Optional[str] = Field(Query(default=None, 
                                                description="A string of comma-separated four coordinates (as per WGS84) representing the bounding box in the format: 'min_lon,min_lat,max_lon,max_lat'",
                                                example="-111.705,40.160,-111.582,40.331",
                                                examples=["-111.705,40.160,-111.582,40.331"]),)
    geom_filter: Optional[str] = Field(Query(default=None, description="A string containing GeoJSON, WKT, or WKB representation of the geometry to filter within. Supported geometry types are: Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon, and GeometryCollection.",
                                            example='POLYGON ((-111.93525458166658 40.40221765026246, -111.93525458166658 40.37069805865761, -111.88137384735077 40.37069805865761, -111.88137384735077 40.40221765026246, -111.93525458166658 40.40221765026246))',
                                            examples=['POLYGON ((-111.93525458166658 40.40221765026246, -111.93525458166658 40.37069805865761, -111.88137384735077 40.37069805865761, -111.88137384735077 40.40221765026246, -111.93525458166658 40.40221765026246))',
                                                        '{"type": "Polygon", "coordinates": [[[-111.93525458166658, 40.40221765026246], [-111.93525458166658, 40.37069805865761], [-111.88137384735077, 40.37069805865761], [-111.88137384735077, 40.40221765026246], [-111.93525458166658, 40.40221765026246]]]}',
                                                        '010300000001000000050000004c6c0836dbfb5bc028e032de7b3344404c6c0836dbfb5bc00450b308732f4440f67ada6d68f85bc00450b308732f4440f67ada6d68f85bc028e032de7b3344404c6c0836dbfb5bc028e032de7b334440',
                                                        'MULTIPOLYGON ( ((-111.93525458166658 40.40221765026246, -111.93525458166658 40.37069805865761, -111.88137384735077 40.37069805865761, -111.88137384735077 40.40221765026246, -111.93525458166658 40.40221765026246)), ((-111.92525458166658 40.39221765026246, -111.92525458166658 40.38069805865761, -111.89137384735077 40.38069805865761, -111.89137384735077 40.39221765026246, -111.92525458166658 40.39221765026246)) )',
                                                        '{ "type": "MultiPolygon", "coordinates": [ [ [ [ -111.93525458166658, 40.40221765026246 ], [ -111.93525458166658, 40.37069805865761 ], [ -111.88137384735077, 40.37069805865761 ], [ -111.88137384735077, 40.40221765026246 ], [ -111.93525458166658, 40.40221765026246 ] ] ], [ [ [ -111.92525458166658, 40.39221765026246 ], [ -111.92525458166658, 40.38069805865761 ], [ -111.89137384735076, 40.38069805865761 ], [ -111.89137384735076, 40.39221765026246 ], [ -111.92525458166658, 40.39221765026246 ] ] ] ] }',
                                                        '010600000002000000010300000001000000050000004c6c0836dbfb5bc028e032de7b3344404c6c0836dbfb5bc00450b308732f4440f67ada6d68f85bc00450b308732f4440f67ada6d68f85bc028e032de7b3344404c6c0836dbfb5bc028e032de7b33444001030000000100000005000000db2efe5e37fb5bc047651e3034324440db2efe5e37fb5bc0e5cac7b6ba30444066b8e4440cf95bc0e5cac7b6ba30444066b8e4440cf95bc047651e3034324440db2efe5e37fb5bc047651e3034324440',
                                                        'MULTILINESTRING ((-111.93525458166658 40.40221765026246, -111.88137384735077 40.40221765026246, -111.88137384735077 40.37069805865761), (-111.88137384735077 40.40221765026246, -111.92525458166658 40.39221765026246, -111.92525458166658 40.38069805865761) )',
                                                        'LINESTRING (-111.93525458166658 40.40221765026246, -111.93525458166658 40.37069805865761, -111.88137384735077 40.40221765026246)',
                                                        'POINT (-111.93525458166658 40.40221765026246)'
                                                      ]),)
    huc: Optional[str] = Field(Query(default=None, description="A string of digits with length corresponding to valid HUC levels (2, 4, 6, 8, 10, or 12) representing the HUC code to filter data for the reaches within.",
                               example="160202010500",
                               examples=["160202010500", '1602020105', '16020201', '160202', '1602', '16']),)
    lon: Optional[float] = Field(Query(default=None, description="Longitude coordinate (as per WGS84) as a decimal degree to filter the reach nearby to this point. Must be provided together with 'lat' parameter.",
                                 example=-111.786835,
                                 examples=[-111.786835, -123.837891, -76.234131]),)
    lat: Optional[float] = Field(Query(default=None, description="Latitude coordinate (as per WGS84) as a decimal degree to filter the reach nearby to this point. Must be provided together with 'lon' parameter.",
                                 example=40.176187,
                                 examples=[40.176187, 47.576526, 35.871247]),)
    with_buffer: Optional[float] = Field(Query(default=None, description="Buffer distance (0 to 1000 meters) to apply when filtering geometries.",
                                               example=100,
                                               examples=[100, 500.575]))
    lowest_stream_order: Optional[StreamorderOptions] = Field(Query(default=None, description="The lowest stream order (1 to 10) for the reaches to include (e.g. '10' for tenth-order streams only, '9' for ninth and tenth-order streams only, '2' for second and all upper-order streams, etc.).",
                                                                    example=2,
                                                                    examples=[1,2,3,4,5,6,7,8,9,10]),)
    output_format: Optional[str] = Field(Query(default="json", description="The output format of the response data. Supported formats are: 'geojson', 'shapefile', 'json', and 'csv'. Default is 'json'.",
                                               example="geojson",
                                               examples=["geojson", "shapefile", "json", "csv"]))
    ordered: Optional[bool] = Field(Query(default=False, description="Whether to order the reaches in the output by relevant fields.",
                                            example=True,
                                            examples=[True, False]))
    metadata: Optional[bool] = Field(Query(default=False, description="Whether to embed metadata with the actual data.",
                                            example=True,
                                            examples=[True, False]))

    @model_validator(mode="before")
    @classmethod
    def validate_combinations_of_filtering_params(cls, params):
        lowest_stream_order = (params.get('lowest_stream_order'))
        geom_filtering_params = ['geom_filter', 'bounding_box', 'huc', 'reach_id', 'gage_id', 'hydroshare_id', 'lon']
        geom_filtering_params_dict = {param: params.get(param) for param in geom_filtering_params}
        geom_filtering_params_count = sum(1 for v in geom_filtering_params_dict.values() if v is not None)
        if geom_filtering_params_count > 1:
            raise ValueError("Several conflicting filtering parameters were provided. Please provide only one of the following filtering options: 'geom_filter', 'bounding_box', 'huc', 'reach_id', 'gage_id', 'hydroshare_id', or 'lon' and 'lat' pair.")
        elif geom_filtering_params_count == 0 and all(value is not None for value in params.values()):
            raise ValueError("No reach filtering parameter provided. Please provide one of the following filtering options to specify the geometry or identifier to filter for: 'geom_filter', 'bounding_box', 'huc', 'reach_id', 'gage_id', 'hydroshare_id', or 'lon' and 'lat' pair.")
        if lowest_stream_order is not None and (geom_filtering_params_dict['reach_id'] is not None or geom_filtering_params_dict['hydroshare_id'] is not None or geom_filtering_params_dict['gage_id'] is not None):
            raise ValueError("'lowest_stream_order' is not compatible with 'reach_id', 'gage_id', or 'hydroshare_id' filtering parameters.")
        if lowest_stream_order is not None and (not any([geom_filtering_params_dict['huc'] is not None,
                                                         geom_filtering_params_dict['geom_filter'] is not None,
                                                         geom_filtering_params_dict['bounding_box'] is not None,])):
            raise ValueError("'lowest_stream_order' is only compatible with 'huc', 'geom_filter', or 'bounding_box' filtering parameters.")
        if params.get('with_buffer') is not None and ((not any([geom_filtering_params_dict['huc'] is not None,
                                                         geom_filtering_params_dict['geom_filter'] is not None,
                                                         geom_filtering_params_dict['bounding_box'] is not None,
                                                         params.get('lat') is not None and params.get('lon') is not None,
                                                         ])) or geom_filtering_params_dict['reach_id'] is not None or geom_filtering_params_dict['gage_id'] is not None or geom_filtering_params_dict['hydroshare_id'] is not None):
            raise ValueError("'with_buffer' is only applicable with 'geom_filter' or 'bounding_box' or 'huc' or 'lat' and 'lon' pair.")
        if ((params.get('lat') is None and geom_filtering_params_dict['lon'] is not None) or
            (params.get('lat') is not None and geom_filtering_params_dict['lon'] is None)):
            raise ValueError("Both 'lon' and 'lat' must be provided together. Please provide both parameters or neither of them to switch to other filtering.")
        if params.get('with_buffer') is not None and (geom_filtering_params_dict['reach_id'] is not None or geom_filtering_params_dict['gage_id'] is not None or geom_filtering_params_dict['hydroshare_id'] is not None):
            raise ValueError("'with_buffer' is only applicable with 'geom_filter' or 'bounding_box' or 'huc' or 'lat' and 'lon' pair.")
        return params

    @field_validator("bounding_box") 
    def validate_bounding_box(cls, bbox):
        if not bbox:
            return bbox
        else:
            bbox = bbox.strip().split(",")
        if len(bbox) != 4:
            raise ValueError("bounding_box doesn't match the required length of four comma-separated coordinates: min_lon,min_lat,max_lon,max_lat.")
        min_lon, min_lat, max_lon, max_lat = map(float, (coord.strip() for coord in bbox))
        if not (-180.0 <= min_lon <= 180.0):
            raise ValueError(f"bounding_box min_lon coordinate ({min_lon}) out of the valid range (-180.0 to 180.0).")
        if not (-180.0 <= max_lon <= 180.0):
            raise ValueError(f"bounding_box max_lon coordinate ({max_lon}) out of the valid range (-180.0 to 180.0).")
        if not (-90.0 <= min_lat <= 90.0):
            raise ValueError(f"bounding_box min_lat coordinate ({min_lat}) out of the valid range (-90.0 to 90.0).")
        if not (-90.0 <= max_lat <= 90.0):
            raise ValueError(f"bounding_box max_lat coordinate ({max_lat}) out of the valid range (-90.0 to 90.0).")
        if not (min_lon < max_lon and min_lat < max_lat):
            raise ValueError("bounding_box doesn't follow the required order of coordinates: min_lon,min_lat,max_lon,max_lat.")
        # Returns a tuple of floats for shapely box function
        return (min_lon, min_lat, max_lon, max_lat)
    
    @field_validator("lon")
    def validate_lon(cls, longitude):
        if longitude is None:
            return longitude
        elif not (-180.0 <= longitude <= 180.0):
            raise ValueError("'lon' goes beyond the expected range of -180.0 to 180.0.")
        else:
            # Return as string to embed in a WKT string
            return str(longitude)
        
    @field_validator("lat")
    def validate_lat(cls, latitude):
        if latitude is None:
            return latitude
        elif not (-90.0 <= latitude <= 90.0):
            raise ValueError("'lat' goes beyond the expected range of -90.0 to 90.0.")
        else:
            # Return as string to embed in a WKT string
            return str(latitude)

    @field_validator("geom_filter")
    def validate_geometry_filter(cls, geom):
        if not geom:
            return geom
        geom_str = geom.strip()
        if geom_str.startswith("{") and geom_str.endswith("}"):
            try:
                _geojson_parse_check = json.loads(geom_str)
            except Exception as e:
                raise ValueError(f"geom_filter param in GeoJSON format is invalid to parse: {e}")
            try:
                _geojson_geometry_check = geometry.shape(_geojson_parse_check)
                return geom_str, 'geojson'
            except Exception as e:
                raise ValueError(f"geom_filter param in GeoJSON format is an invalid geometry: {e}")
        elif geom_str.startswith("0") or geom_str.startswith("1"):
            try:
                _wkb_parse_check = wkb.loads(bytes.fromhex(geom_str))
                return geom_str, 'wkb'
            except Exception as e:
                raise ValueError(f"geom_filter param in WKB format is invalid to parse: {e}")
        elif geom_str[0].isdigit():
                raise ValueError("geom_filter param starting with a digit doesn't match the expected format of WKB")
        elif geom_str.startswith(("POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION")):
            try:
                _wkt_parse_check = wkt.loads(geom_str)
                # Return a tuple of strings for geography and its format
                return geom_str, 'wkt'
            except ShapelyError as e:
                raise ValueError(f"geom_filter param in WKT format is invalid to parse: {e}")

    @field_validator("huc")
    def validate_huc(cls, huc):
        if not huc:
            return huc
        huc_str = huc.strip()
        if not huc_str.isdigit():
            raise ValueError("huc doesn't match the string of prespecified digits. Find more info about valid HUC levels here: https://water.usgs.gov/themes/hydrologic-units/")
        if len(huc_str) not in {2, 4, 6, 8, 10, 12, 14, 16}:
            raise ValueError("huc doesn't match the length corresponding to valid HUC levels (2, 4, 6, 8, 10, 12, 14, or 16). Find more info about valid HUC levels here: https://water.usgs.gov/themes/hydrologic-units/")
        # Return as string (never as integer to avoid issues with leading zeros) for huc data query
        return huc_str
    
    @field_validator("reach_id")
    def validate_reach_id(cls, reaches):
        if not reaches:
            return reaches
        reach_list = [reach.strip() for reach in reaches.split(",")]
        digit_check = [reach.isdigit() for reach in reach_list]
        if not all(digit_check):
            raise ValueError(f"reach_id {reaches[not(digit_check)]} contains non-digit values. reach_id should be a string of digits only.")
        # Return as list of strings to embed into a SQL query string
        return reach_list

    @field_validator("gage_id")
    def validate_gage_id(cls, gages):
        if not gages:
            return gages
        gage_list = [gage.strip() for gage in gages.split(",")]
        digit_check = [gage.isdigit() for gage in gage_list]
        if not all(digit_check):
            raise ValueError(f"gage_id {gages[not(digit_check)]} contains non-digit values. gage_id should be a string of digits only.")
        # Return as string to embed into a SQL query string
        return gage_list

    @field_validator("hydroshare_id")
    def validate_hydroshare_id(cls, hydroshare_id):
        if hydroshare_id is None:
            return hydroshare_id
        hydroshare_id_str = hydroshare_id.strip().lower()
        hydroshare_id_pattern = r'^[0-9a-f]{32}$'
        if not bool(re.match(hydroshare_id_pattern, hydroshare_id_str)):
            raise ValueError("hydroshare_id provided is not a 32-character hexadecimal string.")
        hydroshare_resource_url = f"https://www.hydroshare.org/resource/{hydroshare_id}/data/contents"
        allowed_filenames = ["nwm_comids.json", "nwm_reach_ids.json", "nwm_reachids.json", "nwm_comid_list.json", "nwm_comids_list.json", "comids.json", "comids_list.json",
                         "nwm_reaches.json", "nwm_reachid_list.json", "nwm_reachids_list.json", "reaches.json", "reach_id_list.json", "reach_ids_list.json"]
        file_not_found = True
        for fname in allowed_filenames:
            hydroshare_file_url = f"{hydroshare_resource_url}/{fname}"
            r = requests.get(hydroshare_file_url, timeout=5)
            if r.status_code not in [404, 405] and 'application/json' in r.headers.get('content-type', ''):
                file_not_found = False
                return hydroshare_file_url
        if file_not_found:
            if requests.get(hydroshare_resource_url, timeout=5).status_code == 404:
                raise ValueError("hydroshare_id provided is not associated with an existing HydroShare resource.")
            raise ValueError(f"hydroshare_id provided is valid but no acceptable file found in the resource. Please make sure the resource contains a file named one of the following: {allowed_filenames}.")
    
    @field_validator("output_format")
    def clean_output_format_str(cls, file_format):
        file_format = file_format.strip().lower()
        return file_format
    
    @field_validator("lowest_stream_order")
    def get_value_of_order(cls, stream_order):
        if stream_order is None:
            return stream_order
        else:
            return stream_order.value
        
    @field_validator("with_buffer")
    def validate_with_buffer(cls, buffer):
        if buffer is None:
            return buffer
        elif buffer < 0 or buffer > 1000:
            raise ValueError("with_buffer must be between 0 and 1000 meters.")
        else:
            return buffer


class GeometriesWithTimeParams(GeometriesParams):
    time_zone: Optional[str] = Field(Query(default="UTC", 
                                           description="The time zone for the provided time parameters and outputs. Default is 'UTC'.",
                                           example="US/Mountain",
                                           examples=["UTC", "US/Mountain", "America/Los_Angeles", "US/Arizona"]),)

    @field_validator("time_zone")
    def validate_time_zone(cls, timezone):
        timezone = timezone.strip()
        if timezone == 'UTC' or timezone == 'utc':
            return timezone
        elif timezone not in pytz.all_timezones:
            raise ValueError(f"'{timezone}' is not a valid timezone. Please provide a valid timezone string (e.g. 'UTC', 'America/Los_Angeles', 'US/Mountain').")
        return timezone
    
    @field_validator("output_format")
    def validate_output_format(cls, file_format):
        if file_format == 'gpkg':
            file_format = 'geopackage'
        allowed_file_formats = {"csv", "json", "geojson", "geopackage"}
        if file_format not in allowed_file_formats:
            raise ValueError(f"output_format is not one of [{sorted(allowed_file_formats)}] allowed for this endpoint.")
        return file_format
    
    
class GeometriesWithTimeRangeParams(GeometriesWithTimeParams):
    start_time: Optional[str] = Field(Query(default=None, description="The start time of the time series.",
                                            example="2022-01-01T00:00:00",
                                            examples=["2022-01-01T00:00:00", ]),)
    end_time: Optional[str] = Field(Query(default=None, description="The end time of the time series.",
                                            example="2022-02-01T00:00:00",
                                            examples=["2022-02-01T00:00:00"]),)
    
    @field_validator("start_time")
    def validate_start_time(stime):
        if stime is None:
            return stime
        try:
            stime = date_parser.parse(stime)
        except Exception:
            raise ValueError("start_time is not a valid datetime string.")
        timezone = stime.tzinfo
        if timezone is not None:
                raise ValueError("start_time must be a naive datetime string without timezone information.")
        return stime
    
    @field_validator("end_time")
    def validate_end_time(etime):
        if etime is None:
            return etime
        try:
            etime = date_parser.parse(etime)
        except Exception:
            raise ValueError("end_time is not a valid datetime string.")
        if etime.tzinfo is not None:
            raise ValueError("end_time must be a naive datetime string without timezone information.")
        return etime

    @model_validator(mode="after")
    def convert_time_per_timezone(self):
        tz = pytz.timezone(self.time_zone)
        if self.start_time[1]=='local':
            start_time_aware = tz.localize(self.start_time[0])
            self.start_time = start_time_aware.astimezone(pytz.utc)
        else:
            self.start_time = self.start_time[0]
        if self.end_time[1]=='utc':
            self.end_time = (self.end_time[0], 'latest')
        else:
            end_time_aware = tz.localize(self.end_time[0])
            self.end_time = (end_time_aware.astimezone(pytz.utc), self.end_time[1])
        return self

    @model_validator(mode="after")
    def compare_start_end_time(self):
        if self.start_time >= self.end_time[0]:
            raise ValueError("start_time must be before end_time.")
        return self


class GeometriesWithoutTimeParams(GeometriesParams):
    @field_validator("output_format", mode="before")
    def validate_output_format_no_timeseries(cls, file_format):
        allowed_file_formats = {"geojson", "shapefile", "json", "csv", 'shp'}
        if file_format not in allowed_file_formats:
            raise ValueError(f"output_format is not one of [{sorted(allowed_file_formats)}] for this endpoint.")
        if file_format == 'shp':
            file_format = 'shapefile'
        return file_format


class ReturnPeriodsParams(GeometriesWithoutTimeParams):
    return_periods: Optional[str] = Field(Query(default=None, 
                                                description="A string containing combination of return periods: 2, 5, 10, 25, 50, and 100",
                                                example="10,50,100",
                                                examples=["10,50,100", "10"]),)

    @field_validator("return_periods")
    def validate_return_periods(cls, rps):
        if not rps:
            return rps
        rps_list = [rp.strip() for rp in rps.split(",")]
        allowed_rps = {"2", "5", "10", "25", "50", "100"}
        invalid_items = [rp for rp in rps_list if rp not in allowed_rps]
        if invalid_items:
            raise ValueError(
                f"return_periods {', '.join(invalid_items)} are not one of 2, 5, 10, 25, 50, or 100."
            )
        return ",".join(rps_list)
    

class AnalysesAssimParams(GeometriesWithTimeRangeParams):
    run_offset: Optional[str] = Field(Query(default="1", description="A string listing offsets to include. Supported run offsets are: 1, 2, and 3, which correspond to the most recent run (offset 1), the second most recent run (offset 2), and the third most recent run (offset 3).",
                                                example="1,2",
                                                examples=["1,2", "1"]),)
    start_time: Optional[str] =Field(Query(default=None, description="The start time of the analysis-assimilation timeseries. If not provided, it will default to one month before the last timestep of the analysis run.",
                                            example="2022-01-01T00:00:00",
                                            examples=["2022-01-01T00:00:00", "2022-01-01 00:00:00", "2022/01/01 00:00:00", "Jan 1 2022 0:00 AM"]),)
    end_time: Optional[str] =Field(Query(default=None, description="The end time of the analysis-assimilation timeseries. If not provided, it will default to the last timestep of the analysis run.",
                                            example="2022-02-01T00:00:00",
                                            examples=["2022-02-01T00:00:00", "2022-02-01 00:00:00", "2022/02/01 00:00:00", "Feb 1 2022 0:00 AM"]),)
    model_config = {"extra": "forbid"}
    
    @field_validator("run_offset")
    def validate_run_offset(cls, run_offset):
        run_offset_list = [offset.strip() for offset in run_offset.split(",")]
        allowed_offsets = ["1", "2", "3"]
        invalid_offsets = [roff for roff in run_offset_list if roff not in allowed_offsets]
        if invalid_offsets:
            raise ValueError(
                f"run_offset {', '.join(invalid_offsets)} are not one of 1, 2, or 3."
            )
        return ",".join(run_offset_list)
        
    @field_validator("start_time")
    def validate_start_time_for_analysis(cls, stime):
        if stime is None:
            current_time_utc = datetime.now(timezone.utc)
            month_ago_time_utc = current_time_utc - relativedelta(months=1)
            return (month_ago_time_utc, 'utc')
        else:
            return (stime, 'local')

    @field_validator("end_time")
    def validate_end_time_for_analysis(cls, etime):
        latest_etime = datetime.now(timezone.utc)
        if etime is None:
            return (latest_etime, 'utc')
        else:
            return (etime, latest_etime)

    @model_validator(mode="after")
    def validate_earliest_start_time_for_analysis(self):
        earliest_stime = datetime.strptime("2018-09-16T22:00:00+0000", "%Y-%m-%dT%H:%M:%S%z")
        if self.start_time < earliest_stime:
            raise ValueError(f"start_time precedes the earliest available time for analyses-assim dataset: {earliest_stime.strftime('%Y-%m-%dT%H:%M:%S')} UTC.")
        else:
            self.start_time = self.start_time.strftime("%Y-%m-%d %H:%M:%S UTC")
        if self.end_time[1]=='latest':
            self.end_time = self.end_time[0].strftime("%Y-%m-%d %H:%M:%S UTC")
        
        elif self.end_time[0] > self.end_time[1]:
            raise ValueError(f"end_time can be no later than the current time {self.end_time[1]}.")
        else:
            self.end_time = self.end_time[0].strftime("%Y-%m-%d %H:%M:%S UTC")
        return self


class ForecastOptions(str, Enum):
    short_range = "short_range"
    medium_range = "medium_range"
    long_range = "long_range"
    
class ForecastsParams(GeometriesWithTimeParams):
    forecast_type: Optional[ForecastOptions] = Field(Query(default=ForecastOptions.short_range, description="The type of forecast to retrieve as a string.",
                                                           example="short_range",
                                                           examples=["short_range", "medium_range", "long_range"]),)
    reference_time: Optional[str] = Field(Query(default=None, description="The reference time of the forecast to retrieve preferably in ISO format (e.g. '2024-01-01T00:00:00'). If not provided, the most recent forecast will be retrieved.",
                                          example="2024-01-01T00:00:00",
                                          examples=["2024-01-01T00:00:00", "2024-01-01 00:00:00", "2024/01/01 00:00:00", "Jan 1 2024 0:00 AM"]),)
    ensemble: Optional[str] = Field(Query(default=None, description="A string listing ensemble member(s), corresponding to the forecast type, to retrieve. Available ensemble member(s) are only 0 for short_range forecasts, are 0-5 (six) for medium_range forecasts, and are 0-3 (four) for long-range forecasts. If not provided, the average of all available ensemble members will be retrieved.",
                                            example="0,1,2",
                                            examples=["0,1,2,3", "0,1,2,3,4,5,6", "0"]),)

    @field_validator("reference_time")
    def validate_reference_time(cls, rftime):
        if rftime is None:
            return rftime
        try:
            rftime = date_parser.parse(rftime)
        except Exception:
            raise ValueError("reference_time must be a valid datetime string.")
        if rftime.tzinfo is not None:
            raise ValueError("reference_time must be a naive datetime string without timezone information.")
        return rftime
    
    @field_validator("ensemble")
    def validate_ensemble(cls, ens_str):
        if ens_str is None:
            return ens_str
        ensemble_list = [int(ens.strip()) for ens in ens_str.split(",")]
        return ensemble_list
    
    @model_validator(mode="after")
    def convert_rftime_per_timezone(self):
        timezone = pytz.timezone(self.time_zone)
        if self.reference_time is not None:
            rftime_naive = self.reference_time
            rftime_aware = timezone.localize(rftime_naive)
            self.reference_time = rftime_aware.astimezone(pytz.utc)
        self.time_zone = timezone
        return self

    @model_validator(mode="after")
    def validate_ensemble_and_rftime(self):
        ensemble = self.ensemble
        forecast_type = self.forecast_type
        rftime = self.reference_time
        if ensemble:
            allowed_ensembles = {"long_range": [0,1,2,3],
                                 "medium_range": [0,1,2,3,4,5],
                                 "short_range": [0]}
            for ens in ensemble:
                if ens not in allowed_ensembles[forecast_type]:
                    raise ValueError(f"ensemble is not one of {allowed_ensembles[forecast_type]} for the {forecast_type} forecast_type.")
            self.ensemble = ensemble
            
        earliest_rftime = datetime.strptime("2018-09-17T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        current_time = datetime.now(timezone.utc)
        
        if (rftime is None):
            self.reference_time = get_latest_reference_time_for_forecast(forecast_type.value)
        elif rftime > current_time:
            raise ValueError("reference_time cannot be a future time.")
        elif (rftime > current_time - relativedelta(hours=48)):
            actual_latest_rftime_str = get_latest_reference_time_for_forecast(forecast_type.value)
            actual_latest_rftime = datetime.strptime(actual_latest_rftime_str, "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
            if rftime > actual_latest_rftime:
                raise ValueError(f"reference_time is too recent and the forecast data for that reference time is not available yet. The latest available reference_time is {actual_latest_rftime_str} UTC.")
        elif earliest_rftime > rftime:
            raise ValueError(f"reference_time precedes the earliest forecast reference time: {earliest_rftime.strftime('%Y-%m-%dT%H:%M:%S')} UTC")
        else:
            self.reference_time = rftime.strftime("%Y-%m-%d %H:%M:%S UTC")
        return self
    
     
class RetrospectivesParams(GeometriesWithTimeRangeParams):
    model_config = ConfigDict(extra="forbid")
    start_time: Optional[str] =Field(Query(default=None, description="The start time of the retrospective timeseries. If not provided, it will default to one month before the last timestep of the retrospective 3.0 run.",
                                         example="2020-01-01T00:00:00",
                                         examples=["2020-01-01T00:00:00", "2020-01-01 00:00:00", "2020/01/01 00:00:00", "Jan 1 2020 0:00 AM"]),)
    end_time: Optional[str] =Field(Query(default=None, description="The end time of the retrospective timeseries. If not provided, it will default to the last timestep of the retrospective 3.0 run.",
                                        example="2020-02-01T00:00:00",
                                        examples=["2020-02-01T00:00:00", "2020-02-01 00:00:00", "2020/02/01 00:00:00", "Feb 1 2020 0:00 AM"]),)
    
    @field_validator("start_time")
    def validate_start_time_for_retrospective(cls, stime):
        if stime is None:
            month_ahead_last_timestep = datetime.strptime("2023-01-01T00:00:00+0000", "%Y-%m-%dT%H:%M:%S%z")
            return (month_ahead_last_timestep, 'utc')
        else:
            return (stime, 'local')
    
    @field_validator("end_time")
    def validate_end_time_for_retrospective(cls, etime):
        last_timestep = datetime.strptime("2023-02-01T00:00:00+0000", "%Y-%m-%dT%H:%M:%S%z")
        if etime is None:
            return (last_timestep, 'utc')
        else:
            return (etime, last_timestep)

    @model_validator(mode="after")
    def validate_earliest_start_time_for_analysis(self):
        earliest_stime = datetime.strptime("1979-02-01T01:00:00+0000", "%Y-%m-%dT%H:%M:%S%z")
        if self.start_time < earliest_stime:
            raise ValueError(f"start_time precedes the earliest available time for the retrospective dataset: {earliest_stime.strftime('%Y-%m-%dT%H:%M:%S')} UTC.")
        else:
            self.start_time = self.start_time.strftime("%Y-%m-%d %H:%M:%S UTC")
        if self.end_time[1]=='latest':
            self.end_time = self.end_time[0].strftime("%Y-%m-%d %H:%M:%S UTC")
        elif self.end_time[0] > self.end_time[1]:
            raise ValueError(f"end_time can be no later than the last timestep of retrospective 3.0 dataset {self.end_time[1]}.")
        else:
            self.end_time = self.end_time[0].strftime("%Y-%m-%d %H:%M:%S UTC")
        return self


class FlowMetricsParams(GeometriesWithoutTimeParams):
    metrics: Optional[str] = Field(Query(default=None, description="A comma-separated list of streamflow metrics to include. Supported metric names are: 'monthwise_mean','monthwise_cov','variability_index','slope_fdc','flashiness_index','sevenQ10','mean_annual_7_day_min','baseflow_index','zero_flow_days_n','low_flow_days_n','duration_low_flow_event','high_flow_days_n','duration_high_flow_event','half_flow_date', and 'start_date_flood_season'.",
                                                     example="variability_index,monthwise_cov,slope_fdc",
                                                     examples=["variability_index,monthwise_cov,slope_fdc", "monthwise_mean,monthwise_cov,variability_index"]))
    @field_validator("metrics")
    def validate_metrics(cls, metrics):
        if metrics is None:
            return "monthwise_mean, monthwise_cov, nth_percentile_flows, variability_index, slope_fdc, flashiness_index, sevenQ10, mean_annual_7_day_min, baseflow_index, zero_flow_days_n, low_flow_days_n, duration_low_flow_event, high_flow_days_n, duration_high_flow_event, half_flow_date, start_date_flood_season"
        metric_list = [metric.strip() for metric in metrics.split(",")]
        allowed_metrics = {'monthwise_mean','monthwise_cov','nth_percentile_flows','variability_index','slope_fdc','flashiness_index','sevenQ10','mean_annual_7_day_min','baseflow_index','zero_flow_days_n','low_flow_days_n','duration_low_flow_event','high_flow_days_n','duration_high_flow_event','half_flow_date', 'start_date_flood_season'}
        if not all(metric in allowed_metrics for metric in metric_list):
            raise ValueError(f"Invalid metrics found: {set(metric_list) - allowed_metrics}")
        else:
            return ",".join(map(str, metric_list))


class FlowPercentilesParams(GeometriesWithoutTimeParams):
    percentiles: Optional[str] = Field(Query(default=None, description="A comma-separated list of streamflow percentiles to include. Supported percentile options are: 0, 2, 5, 10, 20, 25, 30, 50, 75, 90, 95, 99, 100.",
                                                     example="0,25,50,75,100",
                                                     examples=["0,25,50,75,100", "0,2,5,10,20,25,30,50,75,90,95,99,100", "100"]),)

    @field_validator("percentiles")
    def validate_percentiles(cls, percentiles):
        if percentiles is None:
            return percentiles
        try:
            percentile_list = [int(p.strip()) for p in percentiles.split(",")]
        except ValueError:
            raise ValueError("Percentiles must be a comma-separated list of integers.")
        allowed_percentiles = {0, 2, 5, 10, 20, 25, 30, 50, 75, 90, 95, 99, 100}
        if not all(p in allowed_percentiles for p in percentile_list):
            raise ValueError(f"Invalid percentiles found: {set(percentile_list) - allowed_percentiles}")
        else:
            return percentile_list
        
    
class ReachesParams(BaseModel):
    reach_id: int = Field(Path(..., ge=0, description="The reach identifier from the National Water Model or NHDFlowlines COMID from the NHDPlusV2 dataset.",
                              example=1129533,
                              examples=[1129533, 1891586]))
    include: Optional[str] = Field(Query(default=None, description="A comma-separated list of datasets to include in the response. Supported dataset options are: 'analyses_assim', 'forecasts_short_range', 'forecasts_medium_range', 'forecasts_long_range', 'return_periods', 'flow_metrics', and 'percentile_flows'. If not provided, all will be included by default.",
                                        example="forecasts_short_range,flow_metrics",
                                        examples=["forecasts_short_range,forecasts_medium_range,forecasts_long_range,analyses_assim,flow_metrics",
                                                  "forecasts_short_range,flow_metrics", 
                                                  "analyses_assim,return_periods"]))
    reference_time: Optional[str] = Field(Query(default=None, description="The reference time for the forecast and analysis-assimilation data to include in the response, preferably in ISO format (e.g. '2024-01-01T00:00:00'). If not provided, the most recent forecast reference time will be used by default. This parameter is only relevant if forecast datasets are included in the request.",
                                          example="2020-01-01T00:00:00",
                                          examples=["2020-01-01T00:00:00", "2020/01/01 00:00:00", "Jan 1 2020 0:00 AM"]),)
    time_zone: Optional[str] = Field(Query(default="UTC", description="The time zone for the provided reference_time and the forecast and analysis-assimilation data included in the response. Default is 'UTC'.",
                                      example="US/Mountain",
                                      examples=["UTC", "US/Mountain", "America/Los_Angeles", "America/New_York"]))
    metadata: Optional[bool] = Field(Query(default=False, description="Whether to embed metadata with the actual data.",
                                           example=True,
                                           examples=[True, False]),)
    
    @field_validator("reach_id")
    def validate_reach_id(cls, reach_id):
        if reach_id < 0:
            raise ValueError("reach_id must be a non-negative integer.")
        return [reach_id,]
        
    @field_validator("reference_time")
    def validate_reference_time(cls, rftime):
        if rftime is None:
            return rftime
        try:
            rftime = date_parser.parse(rftime)
        except Exception:
            raise ValueError("reference_time must be a valid datetime string.")
        if rftime.tzinfo is not None:
            raise ValueError("reference_time must be a naive datetime string without timezone information.")
        return rftime
    
    @field_validator("time_zone")
    def validate_time_zone(cls, timezone):
        timezone = timezone.strip()
        if timezone == 'UTC' or timezone == 'utc':
            return timezone
        elif timezone not in pytz.all_timezones:
            raise ValueError(f"'{timezone}' is not a valid timezone. Please provide a valid timezone string (e.g. 'UTC', 'America/Los_Angeles', 'US/Mountain').")
        return timezone
    
    @field_validator("include")
    def validate_include_datasets(cls, datasets):
        allowed_datasets = {"analyses_assim", "forecasts_short_range", "forecasts_medium_range", "forecasts_long_range", "return_periods", "flow_metrics", "percentile_flows"}
        if datasets is None:
            return list(allowed_datasets)
        else:
            datasets = [dataset.strip() for dataset in datasets.split(",")]
        invalid_datasets = set(datasets) - allowed_datasets
        if invalid_datasets:
            raise ValueError(f"Invalid dataset options found in include parameter: {invalid_datasets}. Supported dataset options to include: 'analyses_assim', 'forecasts_short_range', 'forecasts_medium_range', 'forecasts_long_range', 'return_periods', 'flow_metrics', and 'percentile_flows'.")
        return datasets
    
    @model_validator(mode="after")
    def convert_rftime_per_timezone(self):
        timezone = pytz.timezone(self.time_zone)
        if self.reference_time is not None:
            rftime_naive = self.reference_time
            rftime_aware = timezone.localize(rftime_naive)
            self.reference_time = rftime_aware.astimezone(pytz.utc)
        return self

    @model_validator(mode="after")
    def validate_latest_rftime(self):
        rftime = self.reference_time
        datasets = self.include
        current_time = datetime.now(timezone.utc)
        if self.reach_id != 0:
            if rftime is None:
                self.reference_time = extract_latest_time_for_forecasts(datasets)
            elif rftime > current_time :
                raise ValueError("reference_time cannot be a future time.")
            elif (rftime > current_time - relativedelta(hours=48)):
                if "forecasts_short_range" in datasets:
                    actual_latest_rftime_str = get_latest_reference_time_for_forecast('short_range')
                elif "forecasts_long_range" in datasets:
                    actual_latest_rftime_str = get_latest_reference_time_for_forecast('long_range')
                elif "forecasts_medium_range" in datasets:
                    actual_latest_rftime_str = get_latest_reference_time_for_forecast('medium_range')
                actual_latest_rftime = datetime.strptime(actual_latest_rftime_str, "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
                if rftime > actual_latest_rftime:
                    raise ValueError(f"reference_time is too recent and the forecast data for that reference time is not available yet. The latest available reference_time is {actual_latest_rftime_str}.")
            else:
                self.reference_time = (rftime.strftime("%Y-%m-%d %H:%M:%S UTC"), )*3
        elif self.reach_id == 0 and rftime is None:
            self.reference_time = None
        return self



#####################################################
## Define Output Models
#####################################################
  
  
class GeometriesOutputModel(BaseModel):
    reach_id: int = Field(..., description="The reach identifier from the National Water Model or NHDFlowlines COMID from the NHDPlusV2 dataset.",
                              examples=[1891586,])
    shape_length: float = Field(..., 
                                description="The length of the reach flow line in degrees generated through ESRI.",
                                examples=[0.02426709621530345,], json_schema_extra={"unit": "degrees"})
    stream_order: int = Field(..., description="The stream order of the reach based on the Strahler method in the NHDPlusV2 dataset.",
                            examples=[1,], json_schema_extra={"unit": "Strahler"})
    geometry: str = Field(..., description="The geometry of the reach in WKT format for non-geospatial file formats.", 
                          examples=["LINESTRING(-121.336987789 37.979876274, -121.338925122 37.9799230080001, -121.340602322 37.9796488740001, -121.341845989 37.979077208, -121.343060789 37.9783224080001, -121.345519522 37.976309074, -121.346705389 37.975485608, -121.347804589 37.9744332080001, -121.348672322 37.973746874, -121.351332922 37.972099808, -121.352518922 37.9708412740001, -121.353097122 37.9704064740001, -121.356538322 37.9684620080001, -121.357318922 37.968096008, -121.357463589 37.9679816080001)",
                                    ],
                          json_schema_extra={"unit": "WKT"})
    
    
class ReturnPeriodsOutputModel(BaseModel):
    reach_id: int = Field(..., description="The reach identifier from the National Water Model or NHDFlowlines COMID from the NHDPlusV2 dataset.",
                              examples=[1891586])
    return_period_2: float = Field(..., description="The streamflow corresponds to 2-year return period.", 
                                   examples=[100], 
                                   json_schema_extra={"unit": "cubic meters per second"})
    return_period_5: float = Field(..., description="The streamflow corresponds to 5-year return period.", 
                                   examples=[200],
                                   json_schema_extra={"unit": "cubic meters per second"})
    return_period_10: float = Field(..., description="The streamflow corresponds to 10-year return period.", 
                                    examples=[300],
                                    json_schema_extra={"unit": "cubic meters per second"})
    return_period_25: float = Field(..., description="The streamflow corresponds to 25-year return period.", 
                                    examples=[400],
                                    json_schema_extra={"unit": "cubic meters per second"})
    return_period_50: float = Field(..., description="The streamflow corresponds to 50-year return period.",
                                    examples=[500],
                                    json_schema_extra={"unit": "cubic meters per second"})
    return_period_100: float = Field(..., description="The streamflow corresponds to 100-year return period.", 
                                     examples=[600],
                                     json_schema_extra={"unit": "cubic meters per second"})
    
    
class FlowMetricsOutputModel(BaseModel):
    reach_id: int = Field(..., description="The reach identifier from the National Water Model or NHDFlowlines COMID from the NHDPlusV2 dataset.",
                              examples=[1891586])
    monthwise_mean: list[float] = Field(..., description="Mean streamflow computed for each of the twelve months (January to December sequentially) across all available years.", 
                             examples=[[11.2, 19.64, 20.21, 13.7, 6.61, 4.18, 3.15, 2.73, 2.6, 2.58, 2.57, 3.61],],
                             json_schema_extra={"unit": ["cms"]*12})
    monthwise_cov: list[float] = Field(..., description="Coefficient of variation of streamflow (as a percentage) computed for each of the twelve months (January to December sequentially) across all available years.", 
                             examples=[[203.07, 190.32, 141.07, 159.46, 80.63, 49.96, 26.31, 11.3, 6.65, 12.48, 8.54, 161.46],],
                             json_schema_extra={"unit": ["%"]*12})
    nth_percentile_flows: list[float] = Field(..., description="Streamflow values corresponding to percentiles [0, 2, 5, 10, 20, 25, 30, 50, 75, 90, 95, 99, 100] across all available years.",
                                              examples=[[2.119999885559082, 2.25, 2.3199999332427979, 2.4199998378753662, 2.5366666316986084, 2.5699999332427979, 2.6000415712594989, 2.7325000166893005, 3.7262499829133353, 15.601874649524666, 34.447332676251662, 90.844798609415761, 420.02123896280926],],
                                              json_schema_extra={"unit": ["cms"]*13})
    variability_index: float = Field(..., description="Standard deviation of the logarithms of streamflow values exceeding 5% to 95% (at 5% intervals) of the time.",
                                     examples=[0.31275271159234425],)
    slope_fdc: float = Field(..., description="The slope of the flow duration curve (FDC) between 33% and 66% exceedance probabilities in log-log space.",
                             examples=[0.0041278262094402223],)
    flashiness_index: list[float] = Field(..., description="The mean flashiness index of the streamflow for several decades and the coefficient of variation of flashiness index for the latest decade and listed in the order: [FI(1980-1989), FI(1990-1999), FI(2000-2009), FI(2010-2022), COV(FI(2010-2022))]",
                                   examples=[0.0539888713430496, 0.046643847821606445, 0.029128987195879884, 0.029796465509195198, 122.82580650215074],)
    sevenQ10: float = Field(..., description="The 7Q10 low flow statistic, which is the lowest 7-day average flow that occurs on average once every 10 years.",
                            examples=[2.28328391534319],
                            json_schema_extra={"unit": "cubic meters per second"})
    mean_annual_7_day_min: float = Field(..., description="The mean of the annual minimum 7-day average streamflow across all available years.",
                                         examples=[2.4801148416789975],
                                         json_schema_extra={"unit": "cubic meters per second"})
    baseflow_index: float = Field(..., description="The baseflow index of the streamflow, which is the ratio of the total baseflow volume to the total streamflow volume across all available years.",
                                  examples=[0.82435710071462154],)
    zero_flow_days_n: int = Field(..., description="Mean of the number of zero flow days in water years, considering a zero-flow threshold of 0.001 𝑚3/s.",
                                  examples=[69.976744186046517],
                                  json_schema_extra={"unit": "days per water year"})
    low_flow_days_n: int = Field(..., description="Mean of the number of low flow days in water years, considering a low-flow threshold of 20% of the mean daily flow.",
                                    examples=[175.6046511627907], json_schema_extra={"unit": "days per water year"})
    duration_low_flow_event: float = Field(..., description="Mean duration of low flow events in water years, considering a low-flow threshold of 20% of the mean daily flow. A low flow event is defined as consecutive days with streamflow below the low-flow threshold.",   
                                    examples=[160.65957446808511], json_schema_extra={"unit": "days"})
    high_flow_days_n: int = Field(..., description="Mean of the number of high flow days in water years, considering a high-flow threshold of 9 times the median daily flow.",
                                    examples=[37.558139534883722], json_schema_extra={"unit": "days per water year"})
    duration_high_flow_event: float = Field(..., description="Mean duration of high flow events in water years, considering a high-flow threshold of 9 times the median daily flow. A high flow event is defined as consecutive days with streamflow above the high-flow threshold.",
                                    examples=[18.779069767441861], json_schema_extra={"unit": "days"})
    half_flow_date: list[float] = Field(..., description="The average day of the calendar year at which the cumulative sum of daily streamflows just exceeds half of the total flow for the water year and the coefficient of variation.",
                                examples=[83.325581395348834, 24.125723253008605], json_schema_extra={"unit": "day of the calendar year"})
    start_date_flood_season: int = Field(..., description="The average starting day of the flood season in the calendar year, which is defined as the first day of the 180 days window with the highest total streamflow in the calendar year",
                                examples=[366], json_schema_extra={"unit": "day of the calendar year"})
    
    
class FlowPercentilesOutputModel(BaseModel):
    reach_id: int = Field(..., description="The reach identifier from the National Water Model or NHDFlowlines COMID from the NHDPlusV2 dataset.",
                              examples=[1891586])
    min: float = Field(..., description="The minimum streamflow value across all available years.",
                                              examples=[2.119999885559082],
                                              json_schema_extra={"unit": "cubic meters per second"})
    perc_2: float = Field(..., description="The 2nd percentile streamflow value across all available years.",
                                              examples=[2.25],
                                              json_schema_extra={"unit": "cubic meters per second"})
    perc_5: float = Field(..., description="The 5th percentile streamflow value across all available years.",
                                              examples=[2.3199999332427979],
                                              json_schema_extra={"unit": "cubic meters per second"})
    perc_10: float = Field(..., description="The 10th percentile streamflow value across all available years.",
                                              examples=[2.4199998378753662],
                                              json_schema_extra={"unit": "cubic meters per second"})        
    perc_20: float = Field(..., description="The 20th percentile streamflow value across all available years.",
                                                examples=[2.5366666316986084],
                                                json_schema_extra={"unit": "cubic meters per second"})  
    perc_25: float = Field(..., description="The 25th percentile streamflow value across all available years.",
                                                examples=[2.5699999332427979],
                                                json_schema_extra={"unit": "cubic meters per second"})
    perc_30: float = Field(..., description="The 30th percentile streamflow value across all available years.",
                                                examples=[2.6000415712594989],
                                                json_schema_extra={"unit": "cubic meters per second"})  
    perc_50: float = Field(..., description="The 50th percentile (median) streamflow value across all available years.",
                                                examples=[2.7325000166893005],
                                                json_schema_extra={"unit": "cubic meters per second"})  
    perc_75: float = Field(..., description="The 75th percentile streamflow value across all available years.",
                                                examples=[3.7262499829133353],
                                                json_schema_extra={"unit": "cubic meters per second"})
    perc_90: float = Field(..., description="The 90th percentile streamflow value across all available years.",
                                                examples=[15.601874649524666],
                                                json_schema_extra={"unit": "cubic meters per second"})  
    perc_95: float = Field(..., description="The 95th percentile streamflow value across all available years.",
                                                examples=[34.447332676251662],
                                                json_schema_extra={"unit": "cubic meters per second"})  
    perc_99: float = Field(..., description="The 99th percentile streamflow value across all available years.",
                                                examples=[90.844798609415761],
                                                json_schema_extra={"unit": "cubic meters per second"})  
    max: float = Field(..., description="The maximum streamflow value across all available years.",
                                                examples=[420.02123896280926],
                                                json_schema_extra={"unit": "cubic meters per second"})  
    

class AnalysesAssimOutputModel(BaseModel):
    reach_id: int = Field(..., description="The reach identifier from the National Water Model or NHDFlowlines COMID from the NHDPlusV2 dataset.",
                              examples=[1891586])
    time: str = Field(..., description="Timestamp for the timestep in the analysis-assimilation run.",
                              examples=["2023-03-01T14:30:00-05:00"], json_schema_extra={"unit": "datetime in iso format"})
    streamflow: float = Field(..., description="Streamflow value at the particular timestep of analysis-assimilation run.",
                                      examples=[0.17999999225139618], json_schema_extra={"unit": "cubic meters per second"})
    velocity: float = Field(..., description="Velocity value at the particular timestep of analysis-assimilation run.",
                                    examples=[0.07999999821186066], json_schema_extra={"unit": "meters per second"})
    
    
class ForecastsOutputModel(BaseModel):
    reach_id: int = Field(..., description="The reach identifier from the National Water Model or NHDFlowlines COMID from the NHDPlusV2 dataset.",
                              examples=[1891586])
    reference_time: str = Field(..., description="The reference time for the forecast run.",
                              examples=["2023-03-01T00:00:00 UTC"], json_schema_extra={"unit": "datetime in iso format"})
    time: str = Field(..., description="Timestamp for the timestep in the forecast run.",
                              examples=["2023-03-01T14:30:00-05:00"], json_schema_extra={"unit": "datetime in iso format"})
    streamflow: float = Field(..., description="Streamflow value at the particular timestep of forecast run.",
                                      examples=[0.17999999225139618], json_schema_extra={"unit": "cubic meters per second"})
    ensemble: int | str = Field(..., description="The ensemble member number (or average) for the forecast run.",
                                    examples=[0])
    velocity: float = Field(..., description="Velocity value at the particular timestep of forecast run.",
                                    examples=[0.07999999821186066], json_schema_extra={"unit": "meters per second"})
    
    
class RetrospectivesOutputModel(BaseModel):
    reach_id: int = Field(..., description="The reach identifier from the National Water Model or NHDFlowlines COMID from the NHDPlusV2 dataset.",
                              examples=[1891586])
    time: str = Field(..., description="Timestamp for the timestep in the retrospective run.",
                              examples=["2023-03-01T14:30:00-05:00"], json_schema_extra={"unit": "datetime in iso format"})
    streamflow: float = Field(..., description="Streamflow value at the particular timestep of retrospective run.",
                                      examples=[0.17999999225139618], json_schema_extra={"unit": "cubic meters per second"})
    velocity: float = Field(..., description="Velocity value at the particular timestep of retrospective run.",
                                    examples=[0.07999999821186066], json_schema_extra={"unit": "meters per second"})


class ReachesOutputModel(BaseModel):
    reach_id: int = Field(..., description="The reach identifier from the National Water Model or NHDFlowlines COMID from the NHDPlusV2 dataset.",
                              examples=[1891586])
    geometry: str = Field(..., description="The geometry of the reach in WKT format for non-geospatial file formats.",
                          examples=["LINESTRING(-121.336987789 37.979876274, -121.338925122 37.9799230080001, -121.340602322 37.9796488740001, -121.341845989 37.979077208, -121.343060789 37.9783224080001, -121.345519522 37.976309074, -121.346705389 37.975485608, -121.347804589 37.9744332080001, -121.348672322 37.973746874, -121.351332922 37.972099808, -121.352518922 37.9708412740001, -121.353097122 37.9704064740001, -121.356538322 37.9684620080001, -121.357318922 37.968096008, -121.357463589 37.9679816080001)"])
    flow_metrics: Optional[FlowMetricsOutputModel] = Field(default=None, description="The flow metrics data for the reach",)
    percentile_flows: Optional[FlowPercentilesOutputModel] = Field(default=None, description="The flow percentiles data for the reach",
                                                                   json_schema_extra={"unit": "cubic meters per second"})
    return_periods: Optional[ReturnPeriodsOutputModel] = Field(default=None, description="The return periods data for the reach",
                                                               json_schema_extra={"unit": "cubic meters per second"})
    analyses_assim: Optional[AnalysesAssimOutputModel] = Field(default=None, description="The analyses-assimilation data for the reach",
                                                               json_schema_extra={"unit": {"streamflow": "cubic meters per second", "velocity": "meters per second"}})
    forecasts_short_range: Optional[ForecastsOutputModel] = Field(default=None, description="The short-range forecasts data for the reach",
                                                                  json_schema_extra={"unit": {"streamflow": "cubic meters per second", "velocity": "meters per second"}})
    forecasts_medium_range: Optional[ForecastsOutputModel] = Field(default=None, description="The medium-range forecasts data for the reach",
                                                                  json_schema_extra={"unit": {"streamflow": "cubic meters per second", "velocity": "meters per second"}})
    forecasts_long_range: Optional[ForecastsOutputModel] = Field(default=None, description="The long-range forecasts data for the reach",
                                                                  json_schema_extra={"unit": {"streamflow": "cubic meters per second", "velocity": "meters per second"}})

