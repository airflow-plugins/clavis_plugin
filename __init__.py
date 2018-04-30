from airflow.plugins_manager import AirflowPlugin
from clavis_plugin.hooks.clavis_hook import ClavisHook
from clavis_plugin.operators.clavis_to_s3_operator import ClavisToS3Operator


class ClavisPlugin(AirflowPlugin):
    name = "clavis_plugin"
    operators = [ClavisToS3Operator]
    hooks = [ClavisHook]
