import sys

class CustomException(Exception):
    def __init__(self, error_msg, error_detail:sys):
        super().__init__(error_msg)
        self.error_message = self.create_error_msg_detail(error_msg, error_detail)

    def create_error_msg_detail(self, error_msg, error_detail:sys):
        _,_,exc_tb=error_detail.exc_info()
        file_name=exc_tb.tb_frame.f_code.co_filename
        error_message="Error occured ! file: <{0}> | line number: {1} | error message: <{2}>".\
                        format(file_name, exc_tb.tb_lineno, str(error_msg))
        
        return error_message


    def __str__(self):
        return self.error_message
    



if __name__ == "__main__":
    # testing 
    try:
        try:
            raise TypeError("type error")
        except Exception as e:
            raise CustomException(e, sys)
    except Exception as e:
        print(e)