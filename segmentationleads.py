import os
from io import StringIO
from zipfile import ZipFile
import boto3
import pandas as pd
from zcrmsdk.src.com.zoho.api.authenticator import OAuthToken, TokenType
from zcrmsdk.src.com.zoho.api.authenticator.store import DBStore
from zcrmsdk.src.com.zoho.crm.api import UserSignature, SDKConfig, Initializer, HeaderMap
from zcrmsdk.src.com.zoho.crm.api.bulk_write import BulkWriteOperations, FileBodyWrapper, UploadFileHeader, \
    SuccessResponse, APIException, RequestWrapper, Resource, FieldMapping
from zcrmsdk.src.com.zoho.crm.api.dc import USDataCenter
from zcrmsdk.src.com.zoho.crm.api.util import StreamWrapper, Choice


s3_resource = boto3.client('s3')
user = UserSignature(email='crm+jmng@roam.africa')
environment = USDataCenter.PRODUCTION()
token = OAuthToken(client_id=os.getenv("jmng_client_id"),
                   client_secret=os.getenv("jmng_client_secret"),
                   token=os.getenv("jmng_token"),
                   token_type=TokenType.REFRESH, redirect_url='https://www.jobberman.com')
store = DBStore(host=os.getenv("host"),
                database_name=os.getenv("database_name"), user_name=os.getenv("user_name"), password=os.getenv("password"), port_number='3306')
config = SDKConfig(auto_refresh_fields=True, pick_list_validation=False)
resource_path = 's3://awethu-test0/TokenStore/python-app'
Initializer.initialize(user=user, environment=environment, token=token, store=store, resource_path=resource_path,
                       sdk_config=config)
def upload_file():
    """
        This method is used to upload a CSV file in ZIP format for bulk write API. The response contains the file_id.
        :param org_id: The unique ID (zgid) of your organization obtained through the Organization API.
        :param absolute_file_path: The absoluteFilePath of the zip file you want to upload.
        """

    """
        example
        org_id = "673573045"
        absolute_file_path = "/Users/user_name/Documents/Leads.zip"
        """

    # Get instance of BulkWriteOperations Class
    bulk_write_operations = BulkWriteOperations()

    # Get instance of FileBodyWrapper class that will contain the request file
    file_body_wrapper = FileBodyWrapper()

    """
        StreamWrapper can be initialized in any of the following ways

        * param 1 -> fileName
        * param 2 -> Read Stream.
        """
    # stream_wrapper = StreamWrapper(stream=open(absolute_file_path, 'rb'))

    """
        * param 1 -> fileName
        * param 2 -> Read Stream
        * param 3 -> Absolute File Path of the file to be attached
        """

    stri = s3_resource.get_object(Bucket='ds-autosegment', Key='adv_seg_jobs_jmng/deployed/lead_output_jmng.csv')
    # stri = s3_resource.get_object(Bucket='awethu-test0', Key='lead_output_jmng (1) - Sheet1.csv')

    train_label_string = stri['Body'].read().decode('utf-8')

    read_train_labels = pd.read_csv(StringIO(train_label_string))

    read_train_labels.to_csv("segmentationleads.csv")
    with ZipFile("SEGMENTATIONLEADS.zip", "w") as newzip:
        newzip.write("segmentationleads.csv")

    stream_wrapper = StreamWrapper(file_path="SEGMENTATIONLEADS.zip")

    # Set file to the FileBodyWrapper instance
    file_body_wrapper.set_file(stream_wrapper)

    # Get instance of HeaderMap Class
    header_instance = HeaderMap()

    # Possible parameters for upload_file operation
    header_instance.add(UploadFileHeader.feature, "bulk-write")

    header_instance.add(UploadFileHeader.x_crm_org, '646499825')

    # Call upload_file method that takes FileBodyWrapper instance and header_instance as parameter
    response = bulk_write_operations.upload_file(file_body_wrapper, header_instance)

    if response is not None:
        # Get the status code from response
        print('Status Code: ' + str(response.get_status_code()))

        # Get object from response
        response_object = response.get_object()

        if response_object is not None:

            # Check if expected ActionWrapper instance is received.
            if isinstance(response_object, SuccessResponse):

                # Get the Status
                print("Status: " + response_object.get_status().get_value())

                # Get the Code
                print("Code: " + response_object.get_code().get_value())

                print("Details")

                # Get the details dict
                details = response_object.get_details()

                for key, value in details.items():
                    print(key + ' : ' + str(value))

                # Get the Message
                print("Message: " + response_object.get_message().get_value())
                return details["file_id"]

            # Check if the request returned an exception
            elif isinstance(response_object, APIException):

                if response_object.get_status() is not None:
                    # Get the Status
                    print("Status: " + response_object.get_status().get_value())

                if response_object.get_code() is not None:
                    # Get the Code
                    print("Code: " + response_object.get_code().get_value())

                print("Details")

                # Get the details dict
                details = response_object.get_details()

                if details is not None:
                    for key, value in details.items():
                        print(key + ' : ' + str(value))

                if response_object.get_error_message() is not None:
                    # Get the ErrorMessage
                    print("Error Message: " + response_object.get_error_message().get_value())

                # Get the ErrorCode
                print('Error Code: ' + str(response_object.get_error_code()))

                if response_object.get_x_error() is not None:
                    # Get the XError
                    print('XError: ' + response_object.get_x_error().get_value())

                if response_object.get_info() is not None:
                    # Get the Info
                    print("Info: " + response_object.get_info().get_value())

                if response_object.get_x_info() is not None:
                    # Get the XInfo
                    print("XInfo: " + response_object.get_x_info().get_value())

                if response_object.get_message() is not None:
                    # Get the Message
                    print("Message: " + response_object.get_message().get_value())

                print('HttpStatus: ' + response_object.get_http_status())
def create_bulk_write_job(module_api_name):
    file_id = upload_file()
    print(file_id)
    # Get instance of BulkWriteOperations Class
    bulk_write_operations = BulkWriteOperations()
    # Get instance of RequestWrapper Class that will contain the request body
    request = RequestWrapper()

    request.set_operation(Choice('insert'))

    resources = []
    # Get instance of Resource Class
    resource = Resource()
    # To set the type of module that you want to import. The value is data.
    resource.set_type(Choice('data'))
    # To set API name of the module that you select for bulk write job.
    resource.set_module(module_api_name)
    # print(resource.get_module())
    # To set the fileId obtained from file upload API.
    resource.set_file_id(str(file_id))
    # True - Ignores the empty values.The default value is false.
    resource.set_ignore_empty(True)
    # To set a field as a unique field or ID of a record.
    # resource.set_find_by('Email')

    field_mappings = []
    # Get instance of FieldMapping Class
    field_mapping = FieldMapping()
    field_mapping.set_api_name('Platform_Advertiser_ID')
    field_mapping.set_index(2)
    field_mappings.append(field_mapping)

    field_mapping = FieldMapping()
    field_mapping.set_api_name('Segment_Strategy')
    field_mapping.set_find_by('id')
    field_mapping.set_index(3)
    field_mappings.append(field_mapping)

    field_mapping = FieldMapping()
    field_mapping.set_api_name('Lead_Source')
    field_mapping.set_index(4)
    field_mappings.append(field_mapping)

    field_mapping = FieldMapping()
    field_mapping.set_api_name('Lead_Status')
    field_mapping.set_index(5)
    field_mappings.append(field_mapping)

    field_mapping = FieldMapping()
    field_mapping.set_api_name('Company')
    field_mapping.set_index(6)
    field_mappings.append(field_mapping)

    field_mapping = FieldMapping()
    field_mapping.set_api_name('Industry')
    field_mapping.set_index(7)
    field_mappings.append(field_mapping)

    field_mapping = FieldMapping()
    field_mapping.set_api_name('Number_of_Employees')
    field_mapping.set_index(8)
    field_mappings.append(field_mapping)

    field_mapping = FieldMapping()
    field_mapping.set_api_name('Last_Name')
    field_mapping.set_index(9)
    field_mappings.append(field_mapping)

    field_mapping = FieldMapping()
    field_mapping.set_api_name('Phone')
    field_mapping.set_index(10)
    field_mappings.append(field_mapping)

    field_mapping = FieldMapping()
    field_mapping.set_api_name('Email')
    field_mapping.set_index(11)
    field_mappings.append(field_mapping)

    field_mapping = FieldMapping()
    field_mapping.set_api_name('Owner')
    field_mapping.set_index(12)
    field_mappings.append(field_mapping)

    # field_mapping = FieldMapping()
    # default_value = dict()
    # default_value["Segment_Strategy"] = "2542054000094556001"
    # # To set the default value for an empty column in the uploaded file.
    # field_mapping.set_default_value(default_value)
    # field_mappings.append(field_mapping)

    # field_mapping = FieldMapping()
    # default_value = dict()
    # default_value["Segment_Strategy"] = "2542054000094556001"
    # # To set the default value for an empty column in the uploaded file.
    # field_mapping.set_default_value(default_value)
    # field_mappings.append(field_mapping)

    resource.set_field_mappings(field_mappings)
    resources.append(resource)
    # Set the list of resources to RequestWrapper instance
    request.set_resource(resources)
    # print(field_mappings)
    # Call create_bulk_write_job method that takes RequestWrapper instance as parameter
    response = bulk_write_operations.create_bulk_write_job(request)

    if response is not None:
        # Get the status code from response
        print('Status Code: ' + str(response.get_status_code()))

        # Get object from response
        response_object = response.get_object()

        if response_object is not None:

            # Check if expected ActionWrapper instance is received.
            if isinstance(response_object, SuccessResponse):

                # Get the Status
                print("Status: " + response_object.get_status().get_value())

                # Get the Code
                print("Code: " + response_object.get_code().get_value())

                print("Details")

                # Get the details dict
                details = response_object.get_details()

                for key, value in details.items():
                    print(key + ' : ' + str(value))

                # Get the Message
                print("Message: " + response_object.get_message().get_value())
                os.remove("segmentationleads.csv")
                os.remove("SEGMENTATIONLEADS.zip")

            # Check if the request returned an exception
            elif isinstance(response_object, APIException):

                if response_object.get_status() is not None:
                    # Get the Status
                    print("Status: " + response_object.get_status().get_value())

                if response_object.get_code() is not None:
                    # Get the Code
                    print("Code: " + response_object.get_code().get_value())

                print("Details")

                # Get the details dict
                details = response_object.get_details()

                if details is not None:
                    for key, value in details.items():
                        print(key + ' : ' + str(value))

                if response_object.get_error_message() is not None:
                    # Get the ErrorMessage
                    print("Error Message: " + response_object.get_error_message().get_value())

                # Get the ErrorCode
                print('Error Code: ' + str(response_object.get_error_code()))

                if response_object.get_x_error() is not None:
                    # Get the XError
                    print('XError: ' + response_object.get_x_error().get_value())

                if response_object.get_info() is not None:
                    # Get the Info
                    print("Info: " + response_object.get_info().get_value())

                if response_object.get_x_info() is not None:
                    # Get the XInfo
                    print("XInfo: " + response_object.get_x_info().get_value())

                if response_object.get_message() is not None:
                    # Get the Message
                    print("Message: " + response_object.get_message().get_value())

                print('HttpStatus: ' + response_object.get_http_status())
create_bulk_write_job("Leads")
# upload_file()