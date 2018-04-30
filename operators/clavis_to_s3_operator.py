import json

from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook

from clavis_plugin.hooks.clavis_hook import ClavisHook


class ClavisToS3Operator(BaseOperator):
    """
    Clavis to S3 Operator
    :param clavis_conn_id:          The Airflow connection id used to store
                                    the Clavis credentials.
    :type clavis_conn_id:           string
    :param clavis_endpoint:         The endpoint to retreive data for. Possible
                                    values are: [kpi, products, search_terms,
                                    content]
    :type clavis_endpoint:          string
    :param payload:                 Payload variables -- all are optional
        :param start_date:          The requested date to return data for
                                    (Format: yyyy-mm-dd eg. 2017-04-10).
                                    Used only in KPI endpoint.
        :type start_date:           string
        :param end_date:            The requested date to return data for
                                    (Format: yyyy-mm-dd eg. 2017-04-10).
                                    Used only in KPI endpoint.
        :type end_date:             string
        :param report_date:         The requested date to return a
                                    single days data for
                                    (Format: yyyy-mm-dd eg. 2017-04-10).
                                    Used for Products, Search Terms,
                                    and Content endpoints.
        :type report_date:          string
        :param online_store:        Filter the response data by a comma
                                    separated list of online stores.
        :type online_store:         string
        :param brand:               Filter the response data by a comma
                                    separated list of manufacturer brands.
        :type brand:                string
        :param category:            Filter the response data by a comma
                                    separated list of manufacturer categories.
        :type category:             string
        :param include_competitor_data: Indicates whether the response
                                        should include competitor data.
                                        This is turned off by default.
                                        Available values are: [0,1]
        :type include_competitor_data:  integer
        :param manufacturers:          Filter the response data by a comma
                                       separated list of manufacturers.
        :type manufacturers:           string
        :param s3_conn_id:             The Airflow connection id used to store
                                       the S3 credentials.
    :type s3_conn_id:                  string
    :param s3_bucket:                  The S3 bucket to be used to store
                                       the Clavis data.
    :type s3_bucket:                   string
    :param s3_key:                     The S3 key to be used to store
                                       the Clavis data.
    :type s3_bucket:                   string
    """

    template_fields = ['payload',
                       's3_key']

    @apply_defaults
    def __init__(self,
                 http_conn_id,
                 clavis_endpoint,
                 s3_conn_id,
                 s3_bucket,
                 payload,
                 s3_key,
                 *args,
                 **kwargs):
        super(ClavisToS3Operator, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.endpoint = clavis_endpoint
        self.s3_conn_id = s3_conn_id
        self.payload = payload
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):

        def make_request(http_conn_id, endpoint, payload=None, token=None):
            return (ClavisHook(http_conn_id=http_conn_id)
                    .run(endpoint, payload, token=token)
                    .json())

        if self.endpoint == 'kpi':
            self.payload['start_date'] = self.payload.pop('report_date')
            self.payload['end_date'] = self.payload['start_date']

        token = make_request(self.http_conn_id, 'token')['data']['token']

        final_payload = {'page_size': 20000, 'offset': 0}

        for param in self.payload:
            final_payload[param] = self.payload[param]

        response = make_request(self.http_conn_id,
                                self.endpoint,
                                final_payload,
                                token)

        output = response['data']

        final_payload['offset'] += final_payload['page_size']

        # If the offset does not exceed the total number of records,
        # make another request. The length of the page size is added
        # to the offset.

        while response['meta']['total_record_count'] > final_payload['offset']:
            response = make_request(self.http_conn_id,
                                    self.endpoint,
                                    final_payload,
                                    token)
            output += response['data']
            final_payload['offset'] += final_payload['page_size']

        s3 = S3Hook(s3_conn_id='astronomer-s3')

        s3.load_string(
            string_data=json.dumps(output),
            bucket_name=self.s3_bucket,
            key=self.s3_key,
            replace=True
        )

        s3.connection.close()
