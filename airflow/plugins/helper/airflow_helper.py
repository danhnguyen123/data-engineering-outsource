"""
This module helps to manage Airflow helper
"""
from config import config
import json
from helper.logging_helper import LoggingHelper
from helper.redis_helper import RedisHelper
from helper.lark_helper import LarkMessage
from airflow.utils.context import Context

logger = LoggingHelper.get_logger(__name__)
redis = RedisHelper(logger=logger, redis_host=config.REDIS_HOST, redis_port=config.REDIS_PORT)
lark_message = LarkMessage(logger=logger, redis=redis)

def on_failure_callback(context: Context):
    print("Fail works  !  ")
    message = '[ERROR] {} {} \\n {}'.format(context['dag'],context['task'],str(context['exception']))
    message = str(message)
    lark_message.send_message(receiver = {'type':'chat_id','id':config.LARK_ALERT_GROUP_ID}, content = {'type':'text','content': message})

def on_retry_callback(context: Context):
    print("Retry works  !  ")
    message = '[RETRY] {} {} \\n {}'.format(context['dag'],context['task'],str(context['exception']))
    message = str(message)
    lark_message.send_message(receiver = {'type':'chat_id','id':config.LARK_ALERT_GROUP_ID}, content = {'type':'text','content': message})

def sla_miss_callback(context: Context):
    print("Timeout  !  ")

    message = '[TIMEOUT] {} {} \\n Start time: {} | End time: {}'.format(context['dag'],context['task'],context['ti'].start_date,context['ti'].end_date)
    message = str(message)
    lark_message.send_message(receiver = {'type':'chat_id','id':config.LARK_ALERT_GROUP_ID}, content = {'type':'text','content': message})

# from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
# class BeamOperator(BeamRunPythonPipelineOperator):
#     def __init__(self, custom_options=None, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.custom_options = custom_options or {}
#         self.project_id = self.custom_options.get("project_id")
#         self.dataset_id = self.custom_options.get("dataset_id")
#         self.namespace = self.custom_options.get("namespace")
#         self.execution_date = self.custom_options.get("execution_date")
#         self.table_name = self.custom_options.get("table_name")
#         self.task_name = self.custom_options.get("task_name")

#     def execute(self, context):
#         full_load = 1 if context['dag_run'].conf.get('full_load') else 0
#         execution_date = context['dag_run'].conf.get('execution_date') if context['dag_run'].conf.get('execution_date') else self.execution_date
#         table_config = context['dag_run'].conf.get(self.table_name) if context['dag_run'].conf.get(self.table_name) else context['params'].get(self.table_name)

#         run = 1 if table_config.get(self.task_name) else 0

#         self.pipeline_options = {
#             # 'runner': 'PortableRunner',
#             # 'job_endpoint': 'beamjobservice:8099',
#             # 'environment_type': 'LOOPBACK',
#             # 'flink_master': 'jobmanager',  # Set this to your Flink master's address
#             # 'parallelism': 10,
#             # 'max_parallelism': 10,
#             # 'sparkMaster': 'spark://spark-master:7077',
#             # 'maxNumWorkers': '10',
#             "project_id": self.project_id,
#             "dataset_id": self.dataset_id,
#             "table_name": self.table_name,
#             "namespace": self.namespace,
#             "full_load": full_load,
#             "execution_date": execution_date,
#             "run_pipeline": run,
#             'execution.retries': 1
#         }
#         # Merge custom_options into pipeline_options
#         # self.pipeline_options.update(self.custom_options)
#         super().execute(context)

