import json
from helper.jsonlogging import get_configured_logger
from helper.model import CallbackDataInput, CallbackDataOutput

logger = get_configured_logger(__name__)

def call_back_data(request):
    try:
        param = request.get_json()
        param_obj = CallbackDataInput(**param)
        
        result = CallbackDataOutput(Success=param_obj.success, ErrorCode=param_obj.error_code, ErrorMessage=param_obj.error_message, Data=param_obj.data)
        
        response = json.dumps(result.__dict__), 200, {'Content-Type': 'application/json'}
        logger.debug(response)

        return response
    
    except Exception as ex:
        result = CallbackDataOutput(Success=False, ErrorCode="Exception", ErrorMessage=str(ex))

        response = json.dumps(result.__dict__), 500, {'Content-Type': 'application/json'}

        logger.error(f"Exception error: {response}")

        return response

