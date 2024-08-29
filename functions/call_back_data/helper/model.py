class CallbackDataInput:
    def __init__(self, success=None, error_code=None, error_message=None, signature=None, data_type=None, data=None, org_company_code=None, app_id=None):
        self.success = success
        self.error_code = error_code
        self.error_message = error_message
        self.signature = signature
        self.data_type = data_type
        self.data = data
        self.org_company_code = org_company_code
        self.app_id = app_id

class CallbackDataOutput:
    def __init__(self, Success=True, ErrorCode=None, ErrorMessage="", Data=None):
        self.Success = Success
        self.ErrorCode = ErrorCode
        self.ErrorMessage = ErrorMessage
        self.Data = Data